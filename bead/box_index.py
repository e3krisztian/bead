'''
SQLite-based index for bead storage and retrieval.
'''

import json
from contextlib import contextmanager
from dataclasses import dataclass
from bead.infra.fs import Path
from typing import Callable, Generator

from .bead import Bead
from .box_query import QueryCondition
from .exceptions import BoxIndexError, InvalidArchive
from .meta import InputSpec
from .infra import sqlite
from .ziparchive import ZipArchive


SCHEMA_VERSION = 4


@dataclass(frozen=True)
class IndexingError:
    """Represents a non-fatal error for a single archive."""
    path: Path          # The path of the problematic archive
    reason: str         # A string explaining the error


@dataclass(frozen=True)
class IndexingProgress:
    """Represents a single step in the indexing process."""
    total: int                              # Total number of archives to process
    processed: int                          # Number of archives processed so far
    path: Path                              # The path of the archive just processed
    error_count: int                        # Total number of errors encountered so far
    latest_error: IndexingError | None      # The error for the current `path`, if any


def is_new_db(conn):
    """Check if the database is uninitialized."""
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='beads'")
    return cursor.fetchone() is None


def create_schema(conn):
    '''Create database schema if it doesn't exist.'''
    conn.execute('''
        CREATE TABLE IF NOT EXISTS beads (
            name TEXT NOT NULL,
            content_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            freeze_time_iso TEXT NOT NULL,
            freeze_time_unix INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            inputs TEXT, -- JSON encoded list of inputs
            input_map TEXT, -- JSON encoded dict mapping input names to bead names
            PRIMARY KEY (name, content_id)
        )
    ''')

    # Create index on unix timestamp for fast time-based queries
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_beads_freeze_time_unix
        ON beads(freeze_time_unix)
    ''')

    # Index for queries by kind, and (kind, name)
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_beads_kind_name
        ON beads(kind, name)
    ''')

    # Index for queries by content_id
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_beads_content_id
        ON beads(content_id)
    ''')

    conn.execute(f'PRAGMA user_version = {SCHEMA_VERSION}')

    conn.commit()


def get_indexed_files(conn):
    '''Get set of file paths already in index.'''
    cursor = conn.execute('SELECT file_path FROM beads')
    return {row[0] for row in cursor.fetchall()}


def insert_bead_record(conn, archive, relative_path):
    '''Insert bead record into database.'''
    freeze_time_unix = iso_timestamp_to_unix_utc_microseconds(archive.freeze_time_iso)
    inputs_json = json.dumps([i.as_dict() for i in archive.inputs])
    input_map_json = json.dumps(archive.input_map)
    conn.execute('''
        INSERT INTO beads
        (name, content_id, kind, freeze_time_iso, freeze_time_unix, file_path, inputs, input_map)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (archive.name, archive.content_id, archive.kind,
          archive.freeze_time_iso, freeze_time_unix, str(relative_path),
          inputs_json, input_map_json))


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


def iso_timestamp_to_unix_utc_microseconds(timestamp):
    """Convert timestamp to UTC unix microseconds for database storage."""
    if hasattr(timestamp, 'timestamp'):
        return int(timestamp.timestamp() * 1_000_000)
    elif isinstance(timestamp, str):
        from .infra.timestamp import time_from_timestamp
        dt = time_from_timestamp(timestamp)
        return int(dt.timestamp() * 1_000_000)
    return timestamp


def unix_microseconds_to_iso_timestamp(unix_microseconds):
    """Convert unix microseconds back to ISO timestamp for Bead objects."""
    import datetime
    dt = datetime.datetime.fromtimestamp(unix_microseconds / 1_000_000, tz=datetime.timezone.utc)
    return dt.isoformat().replace('+00:00', '+0000')


def normalize_timestamp_value(value):
    '''Convert timestamp value to unix microseconds for database queries.'''
    return iso_timestamp_to_unix_utc_microseconds(value)


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

    sql = 'SELECT name, content_id, kind, freeze_time_iso, file_path, inputs, input_map FROM beads'
    if where_parts:
        sql += ' WHERE ' + ' AND '.join(where_parts)
    sql += ' ORDER BY freeze_time_unix'

    cursor = conn.execute(sql, parameters)

    beads = []
    for row in cursor.fetchall():
        name, content_id, kind, freeze_time_iso, file_path, inputs_json, input_map_json = row

        bead = Bead(
            name=name,
            content_id=content_id,
            kind=kind,
            freeze_time_iso=freeze_time_iso,
            box_name=box_name,
            inputs=[InputSpec.from_dict(d) for d in json.loads(inputs_json)],
            input_map=json.loads(input_map_json) if input_map_json else {}
        )
        beads.append(bead)

    return beads


class BoxIndex:
    '''
    SQLite-based index for a bead box.
    '''

    def __init__(self, box_name: str, box_directory: Path, index_file_path: Path):
        self.box_name = box_name
        self.box_directory = Path(box_directory)
        self.index_path = Path(index_file_path)

        try:
            with sqlite.transaction(self.index_path) as conn:
                if is_new_db(conn):
                    create_schema(conn)
                else:
                    self._check_schema_version(conn)

        except BoxIndexError:
            raise
        except sqlite.Error as e:
            advice = "This might be resolved by running '{REINDEX_COMMAND}'."
            raise self._error(
                f"Failed to initialize or verify index: {e}",
                advice=advice
            ) from e

    def _error(self, message, *, advice=None):
        """Create a BoxIndexError with this box's context information."""
        return BoxIndexError(message, advice=advice, box_name=self.box_name, index_path=self.index_path)

    def _check_schema_version(self, conn):
        """Check database schema version and raise error if incompatible."""
        [[db_version]] = conn.execute('PRAGMA user_version')
        if db_version == SCHEMA_VERSION:
            return

        if db_version == 0:
            raise self._error(
                "Index database is unversioned.",
                advice=f"Please run '{{REINDEX_COMMAND}}' to upgrade to version {SCHEMA_VERSION}."
            )
        elif db_version < SCHEMA_VERSION:
            raise self._error(
                f"Index schema is out of date (version {db_version}, expected {SCHEMA_VERSION}).",
                advice="Please run '{REINDEX_COMMAND}' to upgrade."
            )
        else:  # db_version > SCHEMA_VERSION
            raise self._error(
                f"Index schema is from a newer version of bead (version {db_version}, expected {SCHEMA_VERSION}).",
                advice="Please upgrade your 'bead' tool."
            )

    @contextmanager
    def _safe_db_access(self, read_only: bool = False):
        '''A context manager to safely access the database, handling corruption errors.'''
        try:
            with sqlite.transaction(self.index_path, read_only=read_only) as conn:
                yield conn
        except sqlite.DatabaseError as e:
            raise self._error(
                "Index database is corrupt.",
                advice="Please run '{REINDEX_COMMAND}' to fix it."
            ) from e
        except sqlite.Error as e:
            # Catch other potential sqlite errors
            raise self._error(
                f"A database error occurred: {e}",
                advice="This might be resolved by running '{REINDEX_COMMAND}'."
            ) from e

    def _process_files(
        self,
        paths: list[Path],
        action: Callable[[Path], None],
        total: int,
        processed: int,
        error_count: int,
    ) -> Generator[IndexingProgress, None, tuple[int, int]]:
        '''
        A helper generator to process a list of files with a given action.
        It yields progress and returns the final processed and error counts.
        '''
        for path in paths:
            processed += 1
            latest_error = None
            try:
                action(self.box_directory / path)
            except InvalidArchive as e:
                latest_error = IndexingError(path=path, reason=str(e))
                error_count += 1
            # Note: sqlite.Error (raised as BoxIndexError) is not caught here
            # and will terminate the generator.

            yield IndexingProgress(
                total=total,
                processed=processed,
                path=self.box_directory / path,
                error_count=error_count,
                latest_error=latest_error,
            )
        return processed, error_count

    def sync(self) -> Generator[IndexingProgress, None, None]:
        '''
        Add new files to index and remove deleted files.
        The caller is responsible for collecting and interpreting errors.
        '''
        with self._safe_db_access(read_only=True) as conn:
            indexed_files = get_indexed_files(conn)

        current_files = {
            p.relative_to(self.box_directory) for p in self.box_directory.glob('*.zip')
        }
        indexed_files = {Path(p) for p in indexed_files}

        new_files = list(current_files - indexed_files)
        orphaned_files = list(indexed_files - current_files)
        total = len(new_files) + len(orphaned_files)

        processed, error_count = yield from self._process_files(
            paths=new_files,
            action=self.add_file,
            total=total,
            processed=0,
            error_count=0,
        )

        yield from self._process_files(
            paths=orphaned_files,
            action=self.remove_file,
            total=total,
            processed=processed,
            error_count=error_count,
        )

    def add_file(self, archive_path: Path):
        '''
        Add a bead archive to the index.

        Extracts metadata from the archive file, validates it, and adds a record
        to the index. This is called automatically by Box.store() when creating
        a new bead, but can also be used to index manually added archive files.

        Args:
            archive_path: Path to the bead archive file (.zip)

        Raises:
            InvalidArchive: Archive is corrupted or has invalid metadata (non-fatal)
            BoxIndexError: Database operation failed (fatal, see error advice)
        '''
        archive = ZipArchive(archive_path, box_name='')
        archive.validate()
        relative_path = archive_path.relative_to(self.box_directory)
        with self._safe_db_access() as conn:
            insert_bead_record(conn, archive, relative_path)
            conn.commit()

    def remove_file(self, archive_path: Path):
        '''
        Remove a bead archive from the index.

        Deletes the index record for the given archive file. The operation is
        idempotent - no error if the file is not in the index. This is called
        automatically by sync() for deleted files, but can also be used to
        manually remove index entries.

        Args:
            archive_path: Path to the bead archive file (.zip)

        Raises:
            BoxIndexError: Database operation failed (fatal, see error advice)
        '''
        relative_path = archive_path.relative_to(self.box_directory)
        with self._safe_db_access() as conn:
            delete_bead_record(conn, str(relative_path))
            conn.commit()

    def get_beads(self, conditions) -> list[Bead]:
        '''Query beads from index.'''
        with self._safe_db_access(read_only=True) as conn:
            return query_beads(conn, conditions, self.box_name)

    def get_file_path(self, name: str, content_id: str) -> Path:
        '''Get file path for bead.'''
        with self._safe_db_access(read_only=True) as conn:
            file_path = find_file_path(conn, name, content_id)
            if file_path is None:
                raise LookupError(f"Bead not found in index: name='{name}', content_id='{content_id}'")
            return self.box_directory / file_path
