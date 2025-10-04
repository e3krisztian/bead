"""SQLite connection and query utilities.

This module provides a safe abstraction over sqlite3 connections, ensuring
proper resource cleanup and preventing connection leaks.

Re-exported exceptions from sqlite3 for use by other modules:
- Error: Base class for all SQLite errors
- DatabaseError: Errors related to database corruption or structure
- OperationalError: Runtime errors (e.g., read-only database violations)
"""

import sqlite3
from contextlib import closing
from pathlib import Path

# Re-export sqlite3 exceptions to avoid direct sqlite3 imports elsewhere
Error = sqlite3.Error
DatabaseError = sqlite3.DatabaseError
OperationalError = sqlite3.OperationalError


def query_one(path: Path | str, sql: str, params=()):
    """Execute read-only query and return single result row.

    Args:
        path: Path to SQLite database file
        sql: SQL query to execute
        params: Query parameters (optional)

    Returns:
        Single row tuple or None if no results
    """
    with _connect(path, read_only=True) as conn:
        cursor = conn.execute(sql, params)
        return cursor.fetchone()


def query_all(path: Path | str, sql: str, params=()):
    """Execute read-only query and return all result rows.

    Args:
        path: Path to SQLite database file
        sql: SQL query to execute
        params: Query parameters (optional)

    Returns:
        List of row tuples
    """
    with _connect(path, read_only=True) as conn:
        cursor = conn.execute(sql, params)
        return cursor.fetchall()


def execute(path: Path | str, sql: str, params=()):
    """Execute update/insert/delete statement with automatic commit.

    Args:
        path: Path to SQLite database file
        sql: SQL statement to execute
        params: Statement parameters (optional)
    """
    with _connect(path, read_only=False) as conn:
        conn.execute(sql, params)
        conn.commit()


def transaction(path: Path | str, read_only: bool = False):
    """Return a connection context manager for multi-statement transactions.

    Use this for operations that require multiple SQL statements in a single
    transaction, or when you need to control commit timing.

    Args:
        path: Path to SQLite database file
        read_only: If True, opens in read-only mode (default: False)

    Returns:
        Context manager yielding a sqlite3.Connection

    Example:
        with sqlite.transaction(db_path) as conn:
            conn.execute("INSERT INTO ...")
            conn.execute("UPDATE ...")
            conn.commit()
    """
    return _connect(path, read_only)


def _connect(path: Path | str, read_only: bool):
    """Internal: create appropriate connection with proper cleanup.

    Args:
        path: Path to SQLite database file
        read_only: If True, opens with ?mode=ro flag

    Returns:
        closing() wrapped sqlite3.Connection
    """
    if read_only:
        conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    else:
        conn = sqlite3.connect(str(path))
    return closing(conn)
