'''
SQLite-based index for bead storage and retrieval.

Provides fast queries, concurrent access protection, and dependency tracking
while maintaining full backward compatibility with filesystem-based operations.
'''

import sqlite3
import logging
from pathlib import Path
from typing import List, Tuple, Optional, Any

from . import tech
from .bead import Bead
from .box import QueryCondition
from .exceptions import InvalidArchive
from .meta import parse_inputs
from .tech.timestamp import time_from_timestamp
from .ziparchive import ZipArchive

logger = logging.getLogger(__name__)

# SQLite schema version for future migrations
SCHEMA_VERSION = 1

CREATE_BEADS_TABLE = '''
CREATE TABLE IF NOT EXISTS beads (
    bead_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    content_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    freeze_name TEXT NOT NULL,
    freeze_time_str TEXT NOT NULL,
    file_path TEXT NOT NULL,
    UNIQUE(file_path),
    UNIQUE(name, content_id)
)
'''

CREATE_INPUTS_TABLE = '''
CREATE TABLE IF NOT EXISTS inputs (
    input_id INTEGER PRIMARY KEY,
    bead_id INTEGER NOT NULL,
    input_name TEXT NOT NULL,
    input_kind TEXT NOT NULL,
    input_content_id TEXT NOT NULL,
    input_freeze_time_str TEXT NOT NULL,
    FOREIGN KEY (bead_id) REFERENCES beads(bead_id) ON DELETE CASCADE,
    UNIQUE(bead_id, input_name)
)
'''

CREATE_SCHEMA_TABLE = '''
CREATE TABLE IF NOT EXISTS schema_info (
    version INTEGER PRIMARY KEY
)
'''

CREATE_INDEXES = [
    'CREATE INDEX IF NOT EXISTS idx_beads_name ON beads(name)',
    'CREATE INDEX IF NOT EXISTS idx_beads_content_id ON beads(content_id)',
    'CREATE INDEX IF NOT EXISTS idx_beads_kind ON beads(kind)',
    'CREATE INDEX IF NOT EXISTS idx_beads_freeze_time ON beads(freeze_time_str)',
    'CREATE INDEX IF NOT EXISTS idx_inputs_bead_id ON inputs(bead_id)',
    'CREATE INDEX IF NOT EXISTS idx_inputs_content_id ON inputs(input_content_id)',
    'CREATE INDEX IF NOT EXISTS idx_inputs_kind ON inputs(input_kind)',
    'CREATE INDEX IF NOT EXISTS idx_inputs_name ON inputs(input_name)',
]


class BoxIndex:
    '''
    SQLite-based index for a bead box.
    
    Manages the SQLite database lifecycle and provides operations for
    indexing, querying, and maintaining bead metadata.
    '''
    
    def __init__(self, box_directory: Path):
        self.box_directory = Path(box_directory)
        self.index_path = self.box_directory / '.index.sqlite'
        self._db_connection = None
        self._is_readonly = False
        self._is_available = None
        
    def is_available(self) -> bool:
        '''
        Check if the index is available for use.
        
        Returns True if index exists and is accessible, False otherwise.
        Caches result to avoid repeated filesystem checks.
        '''
        if self._is_available is not None:
            return self._is_available
            
        try:
            # Check if we can access the box directory
            if not self.box_directory.exists():
                self._is_available = False
                return False
                
            # Check if index exists
            if self.index_path.exists():
                # Try to open and validate the index
                try:
                    with self._get_connection() as conn:
                        # Simple validation query
                        conn.execute('SELECT COUNT(*) FROM beads').fetchone()
                    self._is_available = True
                    return True
                except (sqlite3.Error, sqlite3.DatabaseError):
                    logger.warning(f"Index database corrupted at {self.index_path}, will rebuild")
                    self._is_available = False
                    return False
            else:
                # Index doesn't exist, check if we can create it
                try:
                    self._check_write_access()
                    self._is_available = True
                    return True
                except (OSError, PermissionError):
                    logger.info(f"Cannot create index at {self.index_path}, using filesystem fallback")
                    self._is_available = False
                    return False
                    
        except Exception as e:
            logger.warning(f"Error checking index availability: {e}")
            self._is_available = False
            return False
    
    def _check_write_access(self):
        '''Check if we can write to the box directory.'''
        if not self.box_directory.exists():
            raise OSError(f"Box directory does not exist: {self.box_directory}")
            
        # Try to create a temporary file to test write access
        test_file = self.box_directory / '.write_test'
        try:
            test_file.touch()
            test_file.unlink()
        except (OSError, PermissionError) as e:
            self._is_readonly = True
            raise OSError(f"Cannot write to box directory: {e}")
    
    def _get_connection(self) -> sqlite3.Connection:
        '''Get a database connection, creating the database if needed.'''
        if self._db_connection is None:
            try:
                # Check if database file exists
                db_exists = self.index_path.exists()
                
                # Open connection
                self._db_connection = sqlite3.connect(
                    str(self.index_path),
                    timeout=30.0,  # 30 second timeout for network filesystems
                    check_same_thread=False
                )
                
                # Enable foreign keys
                self._db_connection.execute('PRAGMA foreign_keys = ON')
                
                # Set WAL mode for better concurrent access
                try:
                    self._db_connection.execute('PRAGMA journal_mode = WAL')
                except sqlite3.OperationalError:
                    # WAL mode might not be available on some network filesystems
                    logger.info("WAL mode not available, using default journal mode")
                
                # Create schema if database is new
                if not db_exists:
                    self._create_schema()
                else:
                    self._validate_schema()
                    
            except sqlite3.Error as e:
                logger.error(f"Failed to open database {self.index_path}: {e}")
                self._db_connection = None
                raise
                
        return self._db_connection
    
    def _create_schema(self):
        '''Create the database schema.'''
        with self._get_connection() as conn:
            # Create tables
            conn.execute(CREATE_SCHEMA_TABLE)
            conn.execute(CREATE_BEADS_TABLE)
            conn.execute(CREATE_INPUTS_TABLE)
            
            # Create indexes
            for index_sql in CREATE_INDEXES:
                conn.execute(index_sql)
            
            # Insert schema version
            conn.execute('INSERT INTO schema_info (version) VALUES (?)', (SCHEMA_VERSION,))
            
            conn.commit()
    
    def _validate_schema(self):
        '''Validate that the existing schema is compatible.'''
        try:
            with self._get_connection() as conn:
                # Check if schema_info table exists
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_info'"
                )
                if not cursor.fetchone():
                    raise sqlite3.DatabaseError("Missing schema_info table")
                
                # Check schema version
                cursor = conn.execute('SELECT version FROM schema_info')
                row = cursor.fetchone()
                if not row or row[0] != SCHEMA_VERSION:
                    raise sqlite3.DatabaseError(f"Incompatible schema version: {row[0] if row else 'unknown'}")
                
                # Verify required tables exist
                required_tables = ['beads', 'inputs']
                for table in required_tables:
                    cursor = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
                    )
                    if not cursor.fetchone():
                        raise sqlite3.DatabaseError(f"Missing required table: {table}")
                        
        except sqlite3.Error as e:
            logger.error(f"Schema validation failed: {e}")
            raise sqlite3.DatabaseError(f"Invalid database schema: {e}")
    
    def rebuild(self):
        '''
        Rebuild the index from scratch by scanning all files in the box directory.
        '''
        if not self.is_available():
            raise RuntimeError("Index is not available for rebuild")
            
        logger.info(f"Rebuilding index for box at {self.box_directory}")
        
        try:
            # Remove existing database file if it exists
            if self.index_path.exists():
                self.close()
                self.index_path.unlink()
                self._db_connection = None
            
            # Create new database with schema
            self._create_schema()
            
            # Scan directory for archive files
            archive_files = list(self.box_directory.glob('*.zip'))
            logger.info(f"Found {len(archive_files)} archive files to index")
            
            indexed_count = 0
            error_count = 0
            
            with self._get_connection() as conn:
                for archive_path in archive_files:
                    try:
                        self._add_archive_to_index(conn, archive_path)
                        indexed_count += 1
                    except (InvalidArchive, sqlite3.Error) as e:
                        logger.warning(f"Failed to index {archive_path}: {e}")
                        error_count += 1
                
                conn.commit()
            
            logger.info(f"Index rebuild complete: {indexed_count} beads indexed, {error_count} errors")
            
        except Exception as e:
            logger.error(f"Index rebuild failed: {e}")
            # Clean up partial database
            if self.index_path.exists():
                try:
                    self.close()
                    self.index_path.unlink()
                except Exception:
                    pass
            raise
    
    def sync(self):
        '''
        Discover new files and add them to the index.
        '''
        if not self.is_available():
            return
            
        try:
            # Get list of files already in index
            with self._get_connection() as conn:
                cursor = conn.execute('SELECT file_path FROM beads')
                indexed_files = {row[0] for row in cursor.fetchall()}
            
            # Scan directory for all archive files
            archive_files = list(self.box_directory.glob('*.zip'))
            
            # Find new files
            new_files = []
            for archive_path in archive_files:
                relative_path = archive_path.relative_to(self.box_directory)
                if str(relative_path) not in indexed_files:
                    new_files.append(archive_path)
            
            if not new_files:
                return
                
            logger.info(f"Syncing {len(new_files)} new files to index")
            
            added_count = 0
            error_count = 0
            
            with self._get_connection() as conn:
                for archive_path in new_files:
                    try:
                        self._add_archive_to_index(conn, archive_path)
                        added_count += 1
                    except (InvalidArchive, sqlite3.Error) as e:
                        logger.warning(f"Failed to index {archive_path}: {e}")
                        error_count += 1
                
                conn.commit()
            
            logger.info(f"Sync complete: {added_count} beads added, {error_count} errors")
            
        except Exception as e:
            logger.error(f"Index sync failed: {e}")
            # Don't raise - sync is best effort
    
    def add_bead(self, archive_path: Path):
        '''
        Add a single bead to the index.
        '''
        if not self.is_available():
            return
            
        try:
            with self._get_connection() as conn:
                self._add_archive_to_index(conn, archive_path)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to add bead {archive_path} to index: {e}")
            # Don't raise - adding to index is best effort
    
    def remove_bead(self, archive_path: Path):
        '''
        Remove a bead from the index (manual operation only).
        '''
        if not self.is_available():
            return
            
        try:
            relative_path = archive_path.relative_to(self.box_directory)
            
            with self._get_connection() as conn:
                cursor = conn.execute('DELETE FROM beads WHERE file_path = ?', (str(relative_path),))
                if cursor.rowcount > 0:
                    conn.commit()
                    logger.info(f"Removed bead {relative_path} from index")
                else:
                    logger.warning(f"Bead {relative_path} not found in index")
                    
        except Exception as e:
            logger.error(f"Failed to remove bead {archive_path} from index: {e}")
            # Don't raise - removal from index is best effort
    
    def _add_archive_to_index(self, conn: sqlite3.Connection, archive_path: Path):
        '''
        Add a single archive to the index within an existing transaction.
        '''
        # Create archive instance to extract metadata
        archive = ZipArchive(archive_path, box_name='')  # box_name will be set by caller
        
        # Validate archive
        archive.validate()
        
        # Calculate relative path
        relative_path = archive_path.relative_to(self.box_directory)
        
        # Insert bead record
        cursor = conn.execute('''
            INSERT OR REPLACE INTO beads 
            (name, content_id, kind, freeze_name, freeze_time_str, file_path)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            archive.name,
            archive.content_id,
            archive.kind,
            archive.name,  # freeze_name same as name for now
            archive.freeze_time_str,
            str(relative_path)
        ))
        
        bead_id = cursor.lastrowid
        
        # Delete existing inputs for this bead (in case of replacement)
        conn.execute('DELETE FROM inputs WHERE bead_id = ?', (bead_id,))
        
        # Insert input records
        for input_spec in archive.inputs:
            conn.execute('''
                INSERT INTO inputs 
                (bead_id, input_name, input_kind, input_content_id, input_freeze_time_str)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                bead_id,
                input_spec.name,
                input_spec.kind,
                input_spec.content_id,
                input_spec.freeze_time_str
            ))
    
    def compile_conditions(self, conditions: List[Tuple[QueryCondition, Any]]) -> Tuple[str, List[Any]]:
        '''
        Convert QueryCondition list to SQL WHERE clause and parameters.
        
        Returns:
            Tuple of (where_clause, parameters)
        '''
        if not conditions:
            return '', []
        
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
                # Convert to string if needed
                if hasattr(value, 'isoformat'):
                    value = value.isoformat().replace('+00:00', '+0000')
                where_parts.append('freeze_time_str = ?')
                parameters.append(value)
            elif condition_type == QueryCondition.NEWER_THAN:
                if hasattr(value, 'isoformat'):
                    value = value.isoformat().replace('+00:00', '+0000')
                where_parts.append('freeze_time_str > ?')
                parameters.append(value)
            elif condition_type == QueryCondition.OLDER_THAN:
                if hasattr(value, 'isoformat'):
                    value = value.isoformat().replace('+00:00', '+0000')
                where_parts.append('freeze_time_str < ?')
                parameters.append(value)
            elif condition_type == QueryCondition.AT_OR_NEWER:
                if hasattr(value, 'isoformat'):
                    value = value.isoformat().replace('+00:00', '+0000')
                where_parts.append('freeze_time_str >= ?')
                parameters.append(value)
            elif condition_type == QueryCondition.AT_OR_OLDER:
                if hasattr(value, 'isoformat'):
                    value = value.isoformat().replace('+00:00', '+0000')
                where_parts.append('freeze_time_str <= ?')
                parameters.append(value)
            else:
                raise ValueError(f"Unsupported query condition: {condition_type}")
        
        where_clause = ' AND '.join(where_parts)
        return where_clause, parameters
    
    def query(self, conditions: List[Tuple[QueryCondition, Any]], box_name: str) -> List[Bead]:
        '''
        Execute query against the index and return list of Bead instances.
        '''
        if not self.is_available():
            raise RuntimeError("Index is not available for queries")
        
        where_clause, parameters = self.compile_conditions(conditions)
        
        sql = '''
            SELECT name, content_id, kind, freeze_name, freeze_time_str, file_path
            FROM beads
        '''
        
        if where_clause:
            sql += f' WHERE {where_clause}'
        
        sql += ' ORDER BY freeze_time_str'
        
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(sql, parameters)
                beads = []
                
                for row in cursor.fetchall():
                    name, content_id, kind, freeze_name, freeze_time_str, file_path = row
                    
                    # Create Bead instance
                    bead = Bead()
                    bead.name = name
                    bead.content_id = content_id
                    bead.kind = kind
                    bead.freeze_time_str = freeze_time_str
                    bead.box_name = box_name
                    
                    # Load inputs for this bead
                    input_cursor = conn.execute('''
                        SELECT input_name, input_kind, input_content_id, input_freeze_time_str
                        FROM inputs
                        WHERE bead_id = (SELECT bead_id FROM beads WHERE content_id = ? AND name = ?)
                    ''', (content_id, name))
                    
                    inputs = []
                    for input_row in input_cursor.fetchall():
                        input_name, input_kind, input_content_id, input_freeze_time_str = input_row
                        from .meta import InputSpec
                        input_spec = InputSpec(
                            name=input_name,
                            kind=input_kind,
                            content_id=input_content_id,
                            freeze_time_str=input_freeze_time_str
                        )
                        inputs.append(input_spec)
                    
                    bead.inputs = inputs
                    beads.append(bead)
                
                return beads
                
        except sqlite3.Error as e:
            logger.error(f"Query failed: {e}")
            raise RuntimeError(f"Index query failed: {e}")
    
    def get_file_path(self, name: str, content_id: str) -> Optional[Path]:
        '''
        Resolve (name, content_id) to file path for Box.resolve().
        
        Returns:
            Path to archive file relative to box directory, or None if not found
        '''
        if not self.is_available():
            return None
        
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    'SELECT file_path FROM beads WHERE name = ? AND content_id = ?',
                    (name, content_id)
                )
                row = cursor.fetchone()
                
                if row:
                    return self.box_directory / row[0]
                else:
                    return None
                    
        except sqlite3.Error as e:
            logger.error(f"File path lookup failed: {e}")
            return None
    
    def close(self):
        '''Close the database connection.'''
        if self._db_connection:
            try:
                self._db_connection.close()
            except sqlite3.Error:
                pass
            finally:
                self._db_connection = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
