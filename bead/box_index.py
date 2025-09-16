'''
SQLite-based index for bead storage and retrieval.
'''

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from .bead import Bead
from .box_query import QueryCondition
from .exceptions import BoxIndexError
from .meta import InputSpec
from .ziparchive import ZipArchive


def create_update_connection(index_path: Path):
    '''Create database connection for updates and ensure schema exists.'''
    conn = sqlite3.connect(str(index_path))
    try:
        create_schema(conn)
    except Exception:
        conn.close()
        raise
    return closing(conn)


def create_query_connection(index_path: Path):
    '''Create read-only database connection for queries.'''
    conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    return closing(conn)


def create_schema(conn):
    '''Create database schema if it doesn't exist.'''
    conn.execute('''
        CREATE TABLE IF NOT EXISTS beads (
            name TEXT NOT NULL,
            content_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            freeze_time_str TEXT NOT NULL,
            freeze_time_unix INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            inputs TEXT, -- JSON encoded list of inputs
            PRIMARY KEY (name, content_id)
        )
    ''')

    # Create index on unix timestamp for fast time-based queries
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_beads_freeze_time_unix
        ON beads(freeze_time_unix)
    ''')

    conn.commit()


def get_indexed_files(conn):
    '''Get set of file paths already in index.'''
    cursor = conn.execute('SELECT file_path FROM beads')
    return {row[0] for row in cursor.fetchall()}


def insert_bead_record(conn, archive, relative_path):
    '''Insert bead record into database.'''
    freeze_time_unix = timestamp_to_unix_utc_microseconds(archive.freeze_time_str)
    inputs_json = json.dumps([i.as_dict() for i in archive.inputs])
    conn.execute('''
        INSERT OR REPLACE INTO beads
        (name, content_id, kind, freeze_time_str, freeze_time_unix, file_path, inputs)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (archive.name, archive.content_id, archive.kind,
          archive.freeze_time_str, freeze_time_unix, str(relative_path),
          inputs_json))


def delete_bead_record(conn, file_path):
    '''Delete bead by file path.'''
    conn.execute('DELETE FROM beads WHERE file_path = ?', (file_path,))


def find_file_path(conn, name, content_id):
    '''Find file path for bead by name and content_id.'''
    cursor = conn.execute(
        'SELECT file_path FROM beads WHERE name = ? AND content_id = ?',
        (name, content_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def timestamp_to_unix_utc_microseconds(timestamp):
    """Convert timestamp to UTC unix microseconds for database storage."""
    if hasattr(timestamp, 'timestamp'):
        return int(timestamp.timestamp() * 1_000_000)
    elif isinstance(timestamp, str):
        from .tech.timestamp import time_from_timestamp
        dt = time_from_timestamp(timestamp)
        return int(dt.timestamp() * 1_000_000)
    return timestamp


def unix_microseconds_to_timestamp_str(unix_microseconds):
    """Convert unix microseconds back to ISO string for Bead objects."""
    import datetime
    dt = datetime.datetime.fromtimestamp(unix_microseconds / 1_000_000, tz=datetime.timezone.utc)
    return dt.isoformat().replace('+00:00', '+0000')


def normalize_timestamp_value(value):
    '''Convert timestamp value to unix microseconds for database queries.'''
    return timestamp_to_unix_utc_microseconds(value)


def build_where_clause(conditions):
    '''Build SQL WHERE clause from query conditions.'''
    condition_mapping = {
        QueryCondition.BEAD_NAME: ('name = ?', lambda v: v),
        QueryCondition.KIND: ('kind = ?', lambda v: v),
        QueryCondition.CONTENT_ID: ('content_id = ?', lambda v: v),
        QueryCondition.AT_TIME: ('freeze_time_unix = ?', normalize_timestamp_value),
        QueryCondition.NEWER_THAN: ('freeze_time_unix > ?', normalize_timestamp_value),
        QueryCondition.OLDER_THAN: ('freeze_time_unix < ?', normalize_timestamp_value),
        QueryCondition.AT_OR_NEWER: ('freeze_time_unix >= ?', normalize_timestamp_value),
        QueryCondition.AT_OR_OLDER: ('freeze_time_unix <= ?', normalize_timestamp_value),
    }
    
    where_parts = []
    parameters = []
    
    for condition_type, value in conditions:
        if condition_type in condition_mapping:
            sql_clause, value_transformer = condition_mapping[condition_type]
            where_parts.append(sql_clause)
            parameters.append(value_transformer(value))
    
    return where_parts, parameters


def query_beads(conn, conditions, box_name):
    '''Execute query and return list of Bead instances.'''
    where_parts, parameters = build_where_clause(conditions)

    sql = 'SELECT name, content_id, kind, freeze_time_str, file_path, inputs FROM beads'
    if where_parts:
        sql += ' WHERE ' + ' AND '.join(where_parts)
    sql += ' ORDER BY freeze_time_unix'

    cursor = conn.execute(sql, parameters)

    beads = []
    for row in cursor.fetchall():
        name, content_id, kind, freeze_time_str, file_path, inputs_json = row

        bead = Bead()
        bead.name = name
        bead.content_id = content_id
        bead.kind = kind
        bead.freeze_time_str = freeze_time_str
        bead.box_name = box_name

        bead.inputs = [InputSpec.from_dict(d) for d in json.loads(inputs_json)]
        beads.append(bead)

    return beads


def index_path_exists(box_directory: Path) -> bool:
    """Check if SQLite index file exists in box directory."""
    index_path = box_directory / '.index.sqlite'
    return index_path.exists()


def can_read_index(box_directory: Path) -> bool:
    """Test if SQLite index can be read."""
    index_path = box_directory / '.index.sqlite'
    try:
        with closing(sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)):
            pass
        return True
    except Exception:
        return False


def ensure_index(box_directory: Path) -> bool:
    """Ensure SQLite index exists, creating it if necessary."""
    try:
        index_path = box_directory / '.index.sqlite'
        with create_update_connection(index_path):
            pass
        return True
    except Exception:
        return False


class BoxIndex:
    '''
    SQLite-based index for a bead box implementing BoxResolver protocol.
    '''
    
    def __init__(self, box_directory: Path):
        self.box_directory = Path(box_directory)
        self.index_path = self.box_directory / '.index.sqlite'
        ensure_index(self.box_directory)
    
    def rebuild(self):
        '''Rebuild index from scratch by scanning all files.'''
        if self.index_path.exists():
            self.index_path.unlink()
        
        for zip_path in self.box_directory.glob('*.zip'):
            self.index_archive_file(zip_path)
    
    def sync(self):
        '''Add new files to index and remove deleted files.'''
        try:
            with create_query_connection(self.index_path) as conn:
                indexed_files = get_indexed_files(conn)

            # Get current files in directory
            current_files = set()
            for archive_path in self.box_directory.glob('*.zip'):
                relative_path = archive_path.relative_to(self.box_directory)
                current_files.add(str(relative_path))
                
                # Add new files to index
                if str(relative_path) not in indexed_files:
                    self.index_archive_file(archive_path)
            
            # Remove files from index that no longer exist
            orphaned_files = indexed_files - current_files
            for file_path in orphaned_files:
                orphaned_archive_path = self.box_directory / file_path
                self.unindex_archive_file(orphaned_archive_path)
        except Exception:
            pass
    
    def index_archive_file(self, archive_path: Path):
        '''Add single bead to index.'''
        try:
            archive = ZipArchive(archive_path, box_name='')
            archive.validate()

            relative_path = archive_path.relative_to(self.box_directory)

            with create_update_connection(self.index_path) as conn:
                insert_bead_record(conn, archive, relative_path)
                conn.commit()
        except Exception:
            pass
    
    def unindex_archive_file(self, archive_path: Path):
        '''Remove bead from index by file path.'''
        try:
            relative_path = archive_path.relative_to(self.box_directory)
            
            with create_update_connection(self.index_path) as conn:
                delete_bead_record(conn, str(relative_path))
                conn.commit()
        except Exception:
            pass
    
    def get_beads(self, conditions, box_name: str) -> list[Bead]:
        '''Query beads from index.'''
        try:
            with create_query_connection(self.index_path) as conn:
                return query_beads(conn, conditions, box_name)
        except Exception as e:
            raise BoxIndexError(f"Failed to query index: {e}")
    
    def get_file_path(self, name: str, content_id: str) -> Path:
        '''Get file path for bead.'''
        try:
            with create_query_connection(self.index_path) as conn:
                file_path = find_file_path(conn, name, content_id)
                if file_path is None:
                    raise LookupError(f"Bead not found in index: name='{name}', content_id='{content_id}'")
                return self.box_directory / file_path
        except LookupError:
            raise
        except Exception as e:
            raise BoxIndexError(f"Failed to get file path from index: {e}")
