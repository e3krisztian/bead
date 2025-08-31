'''
SQLite-based index for bead storage and retrieval.
'''

import sqlite3
from pathlib import Path
from typing import List, Tuple, Optional, Any

from .bead import Bead
from .box import QueryCondition
from .exceptions import InvalidArchive
from .ziparchive import ZipArchive


class BoxIndex:
    '''
    Simple SQLite-based index for a bead box.
    '''
    
    def __init__(self, box_directory: Path):
        self.box_directory = Path(box_directory)
        self.index_path = self.box_directory / '.index.sqlite'
        self._connection = None
    
    def _get_connection(self):
        '''Get database connection, creating schema if needed.'''
        if self._connection is None:
            self._connection = sqlite3.connect(str(self.index_path))
            self._create_schema()
        return self._connection
    
    def _create_schema(self):
        '''Create database schema if it doesn't exist.'''
        conn = self._get_connection()
        
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
    
    def query(self, conditions: List[Tuple[QueryCondition, Any]], box_name: str) -> Optional[List[Bead]]:
        '''Query beads from index. Returns None if index unavailable.'''
        try:
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
            
            sql = 'SELECT name, content_id, kind, freeze_time_str, file_path FROM beads'
            if where_parts:
                sql += ' WHERE ' + ' AND '.join(where_parts)
            sql += ' ORDER BY freeze_time_str'
            
            conn = self._get_connection()
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
                
                # Load inputs
                input_cursor = conn.execute('''
                    SELECT input_name, input_kind, input_content_id, input_freeze_time_str
                    FROM inputs WHERE bead_name = ? AND bead_content_id = ?
                ''', (name, content_id))
                
                inputs = []
                for input_row in input_cursor.fetchall():
                    input_name, input_kind, input_content_id, input_freeze_time_str = input_row
                    from .meta import InputSpec
                    inputs.append(InputSpec(
                        name=input_name,
                        kind=input_kind,
                        content_id=input_content_id,
                        freeze_time_str=input_freeze_time_str
                    ))
                
                bead.inputs = inputs
                beads.append(bead)
            
            return beads
            
        except Exception:
            return None  # Index unavailable
    
    def get_file_path(self, name: str, content_id: str) -> Optional[Path]:
        '''Get file path for bead. Returns None if not found or index unavailable.'''
        try:
            conn = self._get_connection()
            cursor = conn.execute(
                'SELECT file_path FROM beads WHERE name = ? AND content_id = ?',
                (name, content_id)
            )
            row = cursor.fetchone()
            return self.box_directory / row[0] if row else None
        except Exception:
            return None
    
    def close(self):
        '''Close database connection.'''
        if self._connection:
            self._connection.close()
            self._connection = None
