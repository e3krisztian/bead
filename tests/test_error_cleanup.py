"""Tests for error file cleanup functionality."""

import os
import time

from bead_cli.main import cleanup_old_error_files


def test_cleanup_keeps_recent_files(tmp_path):
    """Test that cleanup keeps the most recent files."""
    error_dir = tmp_path / 'errors'
    error_dir.mkdir()

    # Create 25 error files
    for i in range(25):
        error_file = error_dir / f'error_{i:03d}.txt'
        error_file.write_text(f'error {i}')
        # Ensure different modification times
        time.sleep(0.01)

    # Keep only 20 files
    cleanup_old_error_files(error_dir, keep_count=20)

    remaining_files = sorted(error_dir.glob('error_*.txt'))
    assert len(remaining_files) == 20

    # Check that the oldest 5 files were deleted
    for i in range(5):
        assert not (error_dir / f'error_{i:03d}.txt').exists()

    # Check that the newest 20 files remain
    for i in range(5, 25):
        assert (error_dir / f'error_{i:03d}.txt').exists()


def test_cleanup_deletes_old_files(tmp_path):
    """Test that cleanup deletes files older than max_age_days."""
    error_dir = tmp_path / 'errors'
    error_dir.mkdir()

    # Create an old file (91 days ago)
    old_file = error_dir / 'error_old.txt'
    old_file.write_text('old error')
    old_mtime = time.time() - (91 * 86400)
    os.utime(old_file, times=(old_mtime, old_mtime))

    # Create a recent file
    new_file = error_dir / 'error_new.txt'
    new_file.write_text('new error')

    # Cleanup with 90 day max age
    cleanup_old_error_files(error_dir, max_age_days=90)

    # Old file should be deleted, new file should remain
    assert not old_file.exists()
    assert new_file.exists()


def test_cleanup_handles_nonexistent_directory(tmp_path):
    """Test that cleanup handles nonexistent error directory gracefully."""
    error_dir = tmp_path / 'nonexistent'

    # Should not raise an exception
    cleanup_old_error_files(error_dir)


def test_cleanup_hybrid_strategy(tmp_path):
    """Test cleanup with both count and age limits."""
    error_dir = tmp_path / 'errors'
    error_dir.mkdir()

    # Create 15 files, some old, some new
    current_time = time.time()

    # 5 old files (100 days old)
    for i in range(5):
        old_file = error_dir / f'error_old_{i:03d}.txt'
        old_file.write_text(f'old error {i}')
        old_mtime = current_time - (100 * 86400)
        os.utime(old_file, times=(old_mtime, old_mtime))

    # 10 recent files
    for i in range(10):
        new_file = error_dir / f'error_new_{i:03d}.txt'
        new_file.write_text(f'new error {i}')
        time.sleep(0.01)

    # Cleanup: keep 20 files, delete anything older than 90 days
    cleanup_old_error_files(error_dir, keep_count=20, max_age_days=90)

    # All old files should be deleted (age-based)
    for i in range(5):
        assert not (error_dir / f'error_old_{i:03d}.txt').exists()

    # All new files should remain (within count and age limits)
    for i in range(10):
        assert (error_dir / f'error_new_{i:03d}.txt').exists()

    remaining_files = list(error_dir.glob('error_*.txt'))
    assert len(remaining_files) == 10
