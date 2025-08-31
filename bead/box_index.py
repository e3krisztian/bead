'''
SQLite-based index for bead storage and retrieval.
'''

import sqlite3
from pathlib import Path

from .bead import Bead
from .exceptions import BoxIndexError
from .ziparchive import ZipArchive


def create_update_connection(index_path: Path):
    '''Create database connection for updates and ensure schema exists.'''
    conn = sqlite3.connect(str(index_path))
    create_schema(conn)
    return conn


def create_query_connection(index_path: Path):
    '''Create read-only database connection for queries.'''
    conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    return conn


def create_schema(conn):
    '''Create database schema if it doesn't exist.'''
    conn.execute('''
        CREATE TABLE IF NOT EXISTS beads (
            name TEXT NOT NULL,
            content_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            freeze_time_str TEXT NOT NULL,
            file_path TEXT NOT NULL,
            PRIMARY KEY (name, content_id)
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS inputs (
            bead_name TEXT NOT NULL,
            bead_content_id TEXT NOT NULL,
            input_name TEXT NOT NULL,
            input_kind TEXT NOT NULL,
            input_content_id TEXT NOT NULL,
            input_freeze_time_str TEXT NOT NULL,
            FOREIGN KEY (bead_name, bead_content_id) REFERENCES beads(name, content_id)
        )
    ''')
    
    conn.commit()


def get_indexed_files(conn):
    '''Get set of file paths already in index.'''
    cursor = conn.execute('SELECT file_path FROM beads')
    return {row[0] for row in cursor.fetchall()}


def insert_bead_record(conn, archive, relative_path):
    '''Insert bead record into database.'''
    conn.execute('''
        INSERT OR REPLACE INTO beads 
        (name, content_id, kind, freeze_time_str, file_path)
        VALUES (?, ?, ?, ?, ?)
    ''', (archive.name, archive.content_id, archive.kind, 
          archive.freeze_time_str, str(relative_path)))


def delete_bead_inputs(conn, name, content_id):
    '''Delete existing inputs for a bead.'''
    conn.execute('DELETE FROM inputs WHERE bead_name = ? AND bead_content_id = ?',
                (name, content_id))


def insert_input_record(conn, bead_name, bead_content_id, input_spec):
    '''Insert single input record into database.'''
    conn.execute('''
        INSERT INTO inputs 
        (bead_name, bead_content_id, input_name, input_kind, 
         input_content_id, input_freeze_time_str)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (bead_name, bead_content_id, input_spec.name, 
          input_spec.kind, input_spec.content_id, input_spec.freeze_time_str))


def find_file_path(conn, name, content_id):
    '''Find file path for bead by name and content_id.'''
    cursor = conn.execute(
        'SELECT file_path FROM beads WHERE name = ? AND content_id = ?',
        (name, content_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def normalize_timestamp_value(value):
    '''Convert timestamp value to string format for database queries.'''
    if hasattr(value, 'isoformat'):
        return value.isoformat().replace('+00:00', '+0000')
    return value


def build_where_clause(conditions):
    '''Build SQL WHERE clause from query conditions.'''
    from .box import QueryCondition
    
    where_parts = []
    parameters = []
    
    for condition_type, value in conditions:
        if condition_type == QueryCondition.BEAD_NAME:
            where_parts.append('name = ?')
            parameters.append(value)
        elif condition_type == QueryCondition.KIND:
            where_parts.append('kind = ?')
            parameters.append(value)
        elif condition_type == QueryCondition.CONTENT_ID:
            where_parts.append('content_id = ?')
            parameters.append(value)
        elif condition_type == QueryCondition.AT_TIME:
            where_parts.append('freeze_time_str = ?')
            parameters.append(normalize_timestamp_value(value))
        elif condition_type == QueryCondition.NEWER_THAN:
            where_parts.append('freeze_time_str > ?')
            parameters.append(normalize_timestamp_value(value))
        elif condition_type == QueryCondition.OLDER_THAN:
            where_parts.append('freeze_time_str < ?')
            parameters.append(normalize_timestamp_value(value))
        elif condition_type == QueryCondition.AT_OR_NEWER:
            where_parts.append('freeze_time_str >= ?')
            parameters.append(normalize_timestamp_value(value))
        elif condition_type == QueryCondition.AT_OR_OLDER:
            where_parts.append('freeze_time_str <= ?')
            parameters.append(normalize_timestamp_value(value))
    
    return where_parts, parameters


def query_beads(conn, conditions, box_name):
    '''Execute query and return list of Bead instances.'''
    where_parts, parameters = build_where_clause(conditions)
    
    sql = 'SELECT name, content_id, kind, freeze_time_str, file_path FROM beads'
    if where_parts:
        sql += ' WHERE ' + ' AND '.join(where_parts)
    sql += ' ORDER BY freeze_time_str'
    
    cursor = conn.execute(sql, parameters)
    
    beads = []
    for row in cursor.fetchall():
        name, content_id, kind, freeze_time_str, file_path = row
        
        bead = Bead()
        bead.name = name
        bead.content_id = content_id
        bead.kind = kind
        bead.freeze_time_str = freeze_time_str
        bead.box_name = box_name
        
        bead.inputs = load_bead_inputs(conn, name, content_id)
        beads.append(bead)
    
    return beads


def load_bead_inputs(conn, name, content_id):
    '''Load input specifications for a bead.'''
    cursor = conn.execute('''
        SELECT input_name, input_kind, input_content_id, input_freeze_time_str
        FROM inputs WHERE bead_name = ? AND bead_content_id = ?
    ''', (name, content_id))
    
    inputs = []
    for row in cursor.fetchall():
        input_name, input_kind, input_content_id, input_freeze_time_str = row
        from .meta import InputSpec
        inputs.append(InputSpec(
            name=input_name,
            kind=input_kind,
            content_id=input_content_id,
            freeze_time_str=input_freeze_time_str
        ))
    
    return inputs


class BoxIndex:
    '''
    SQLite-based index for a bead box implementing BoxResolver protocol.
    '''
    
    def __init__(self, box_directory: Path):
        self.box_directory = Path(box_directory)
        self.index_path = self.box_directory / '.index.sqlite'
    
    def rebuild(self):
        '''Rebuild index from scratch by scanning all files.'''
        if self.index_path.exists():
            self.index_path.unlink()
        
        for zip_path in self.box_directory.glob('*.zip'):
            self.add_archive_file(zip_path)
    
    def sync(self):
        '''Add new files to index.'''
        try:
            with create_query_connection(self.index_path) as conn:
                indexed_files = get_indexed_files(conn)
            
            for archive_path in self.box_directory.glob('*.zip'):
                relative_path = archive_path.relative_to(self.box_directory)
                if str(relative_path) not in indexed_files:
                    self.add_archive_file(archive_path)
        except Exception:
            pass
    
    def add_archive_file(self, archive_path: Path):
        '''Add single bead to index.'''
        try:
            archive = ZipArchive(archive_path, box_name='')
            archive.validate()
            
            relative_path = archive_path.relative_to(self.box_directory)
            
            with create_update_connection(self.index_path) as conn:
                insert_bead_record(conn, archive, relative_path)
                delete_bead_inputs(conn, archive.name, archive.content_id)
                
                for input_spec in archive.inputs:
                    insert_input_record(conn, archive.name, archive.content_id, input_spec)
                
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
