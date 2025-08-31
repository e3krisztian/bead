'''
SQLite-based index for bead storage and retrieval.
'''

import sqlite3
from pathlib import Path
from typing import List, Tuple, Optional

from .bead import Bead
from .box import QueryCondition
from .exceptions import InvalidArchive
from .ziparchive import ZipArchive


def create_connection(index_path: Path):
    '''Create database connection and ensure schema exists.'''
    conn = sqlite3.connect(str(index_path))
    create_schema(conn)
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


def clear_all_data(conn):
    '''Remove all data from database.'''
    conn.execute('DELETE FROM inputs')
    conn.execute('DELETE FROM beads')
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


def delete_bead_by_path(conn, relative_path):
    '''Delete bead record by file path.'''
    conn.execute('DELETE FROM beads WHERE file_path = ?', (str(relative_path),))
    conn.commit()


def find_file_path(conn, name, content_id):
    '''Find file path for bead by name and content_id.'''
    cursor = conn.execute(
        'SELECT file_path FROM beads WHERE name = ? AND content_id = ?',
        (name, content_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def build_where_clause(conditions):
    '''Build SQL WHERE clause from query conditions.'''
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
    SQLite-based index for a bead box.
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
            with create_connection(self.index_path) as conn:
                indexed_files = get_indexed_files(conn)
            
            for archive_path in self.box_directory.glob('*.zip'):
                relative_path = archive_path.relative_to(self.box_directory)
                if str(relative_path) not in indexed_files:
                    self.add_archive_file(archive_path)
        except Exception:
            pass
    
    def add_bead(self, archive_path: Path):
        '''Add single bead to index.'''
        self.add_archive_file(archive_path)
    
    def remove_bead(self, archive_path: Path):
        '''Remove bead from index.'''
        try:
            relative_path = archive_path.relative_to(self.box_directory)
            with create_connection(self.index_path) as conn:
                delete_bead_by_path(conn, relative_path)
        except Exception:
            pass
    
    def add_archive_file(self, archive_path: Path):
        '''Add archive file to index.'''
        try:
            archive = ZipArchive(archive_path, box_name='')
            archive.validate()
            
            relative_path = archive_path.relative_to(self.box_directory)
            
            with create_connection(self.index_path) as conn:
                insert_bead_record(conn, archive, relative_path)
                delete_bead_inputs(conn, archive.name, archive.content_id)
                
                for input_spec in archive.inputs:
                    insert_input_record(conn, archive.name, archive.content_id, input_spec)
                
                conn.commit()
        except Exception:
            pass
    
    def query(self, conditions, box_name) -> Optional[List[Bead]]:
        '''Query beads from index.'''
        try:
            with create_connection(self.index_path) as conn:
                return query_beads(conn, conditions, box_name)
        except Exception:
            return None
    
    def get_file_path(self, name: str, content_id: str) -> Optional[Path]:
        '''Get file path for bead.'''
        try:
            with create_connection(self.index_path) as conn:
                file_path = find_file_path(conn, name, content_id)
                return self.box_directory / file_path if file_path else None
        except Exception:
            return None
    
