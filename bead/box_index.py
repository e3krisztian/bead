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
    
    def _get_connection(self):
        '''Get new database connection, creating schema if needed.'''
        conn = sqlite3.connect(str(self.index_path))
        self._create_schema(conn)
        return conn
    
    def _create_schema(self, conn):
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
    
    def rebuild(self):
        '''
        Rebuild the index from scratch by scanning all files in the box directory.
        '''
        # Clear existing data
        with self._get_connection() as conn:
            conn.execute('DELETE FROM inputs')
            conn.execute('DELETE FROM beads')
            conn.commit()
        
        # Add all zip files
        for zip_path in self.box_directory.glob('*.zip'):
            try:
                self._add_archive(zip_path)
            except InvalidArchive:
                pass  # Skip invalid archives
    
    def sync(self):
        '''
        Discover new files and add them to the index.
        '''
        # Get list of files already in index
        with self._get_connection() as conn:
            cursor = conn.execute('SELECT file_path FROM beads')
            indexed_files = {row[0] for row in cursor.fetchall()}
        
        # Add new files
        for archive_path in self.box_directory.glob('*.zip'):
            relative_path = archive_path.relative_to(self.box_directory)
            if str(relative_path) not in indexed_files:
                try:
                    self._add_archive(archive_path)
                except InvalidArchive:
                    pass  # Skip invalid archives
    
    def add_bead(self, archive_path: Path):
        '''
        Add a single bead to the index.
        '''
        try:
            self._add_archive(archive_path)
        except Exception:
            pass  # Best effort
    
    def remove_bead(self, archive_path: Path):
        '''Remove bead from index.'''
        try:
            relative_path = archive_path.relative_to(self.box_directory)
            with self._get_connection() as conn:
                conn.execute('DELETE FROM beads WHERE file_path = ?', (str(relative_path),))
                conn.commit()
        except Exception:
            pass  # Best effort
    
    def _add_archive(self, archive_path: Path):
        '''Add archive to index.'''
        archive = ZipArchive(archive_path, box_name='')
        archive.validate()
        
        relative_path = archive_path.relative_to(self.box_directory)
        
        with self._get_connection() as conn:
            # Insert bead
            conn.execute('''
                INSERT OR REPLACE INTO beads 
                (name, content_id, kind, freeze_time_str, file_path)
                VALUES (?, ?, ?, ?, ?)
            ''', (archive.name, archive.content_id, archive.kind, 
                  archive.freeze_time_str, str(relative_path)))
            
            # Delete old inputs and insert new ones
            conn.execute('DELETE FROM inputs WHERE bead_name = ? AND bead_content_id = ?',
                        (archive.name, archive.content_id))
            
            for input_spec in archive.inputs:
                conn.execute('''
                    INSERT INTO inputs 
                    (bead_name, bead_content_id, input_name, input_kind, 
                     input_content_id, input_freeze_time_str)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (archive.name, archive.content_id, input_spec.name, 
                      input_spec.kind, input_spec.content_id, input_spec.freeze_time_str))
            
            conn.commit()
    
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
            
            with self._get_connection() as conn:
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
            with self._get_connection() as conn:
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
        pass  # No persistent connection to close
