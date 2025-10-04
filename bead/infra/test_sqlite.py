"""Tests for sqlite module."""

from pathlib import Path

import pytest

from . import sqlite


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / 'test.db'


def test_execute_creates_database(db_path: Path):
    """Test that execute() creates a database file."""
    assert not db_path.exists()

    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER, name TEXT)')

    assert db_path.exists()


def test_execute_inserts_data(db_path: Path):
    """Test that execute() can insert data."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER, name TEXT)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (?, ?)', (1, 'Alice'))

    result = sqlite.query_one(db_path, 'SELECT * FROM test WHERE id = 1')
    assert result == (1, 'Alice')


def test_query_one_returns_single_row(db_path: Path):
    """Test that query_one() returns a single row."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER, name TEXT)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (1, "Alice")')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (2, "Bob")')

    result = sqlite.query_one(db_path, 'SELECT name FROM test WHERE id = 2')
    assert result == ('Bob',)


def test_query_one_returns_none_for_no_results(db_path: Path):
    """Test that query_one() returns None when no results."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER)')

    result = sqlite.query_one(db_path, 'SELECT * FROM test')
    assert result is None


def test_query_all_returns_all_rows(db_path: Path):
    """Test that query_all() returns all matching rows."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER, name TEXT)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (1, "Alice")')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (2, "Bob")')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (3, "Charlie")')

    results = sqlite.query_all(db_path, 'SELECT name FROM test ORDER BY id')
    assert results == [('Alice',), ('Bob',), ('Charlie',)]


def test_query_all_returns_empty_list_for_no_results(db_path: Path):
    """Test that query_all() returns empty list when no results."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER)')

    results = sqlite.query_all(db_path, 'SELECT * FROM test')
    assert results == []


def test_query_with_parameters(db_path: Path):
    """Test that queries work with parameters."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER, name TEXT)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (?, ?)', (1, 'Alice'))
    sqlite.execute(db_path, 'INSERT INTO test VALUES (?, ?)', (2, 'Bob'))

    result = sqlite.query_one(db_path, 'SELECT name FROM test WHERE id = ?', (2,))
    assert result == ('Bob',)


def test_transaction_allows_multiple_operations(db_path: Path):
    """Test that transaction() allows multiple operations."""
    with sqlite.transaction(db_path) as conn:
        conn.execute('CREATE TABLE test (id INTEGER, name TEXT)')
        conn.execute('INSERT INTO test VALUES (1, "Alice")')
        conn.execute('INSERT INTO test VALUES (2, "Bob")')
        conn.commit()

    results = sqlite.query_all(db_path, 'SELECT name FROM test ORDER BY id')
    assert results == [('Alice',), ('Bob',)]


def test_transaction_read_only(db_path: Path):
    """Test that read-only transaction prevents writes."""
    # Create and populate database
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (1)')

    # Read-only transaction should allow reads
    with sqlite.transaction(db_path, read_only=True) as conn:
        result = conn.execute('SELECT * FROM test').fetchone()
        assert result == (1,)

        # But should raise error on write operations
        with pytest.raises(sqlite.OperationalError, match='readonly'):
            conn.execute('INSERT INTO test VALUES (2)')

        with pytest.raises(sqlite.OperationalError, match='readonly'):
            conn.execute('UPDATE test SET id = 2 WHERE id = 1')

        with pytest.raises(sqlite.OperationalError, match='readonly'):
            conn.execute('DELETE FROM test WHERE id = 1')


def test_execute_auto_commits(db_path: Path):
    """Test that execute() automatically commits changes."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (1)')

    # Query in separate connection should see the data
    result = sqlite.query_one(db_path, 'SELECT * FROM test')
    assert result == (1,)


def test_multiple_execute_calls_independent(db_path: Path):
    """Test that multiple execute() calls are independent transactions."""
    sqlite.execute(db_path, 'CREATE TABLE test (id INTEGER)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (1)')
    sqlite.execute(db_path, 'INSERT INTO test VALUES (2)')

    count = sqlite.query_one(db_path, 'SELECT COUNT(*) FROM test')
    assert count == (2,)
