import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from .box_index import BoxIndex
from .exceptions import BoxIndexError
from .workspace import Workspace


def count_beads_in_index(box_index: BoxIndex) -> int:
    """Return the total number of beads in the index."""
    with sqlite3.connect(box_index.index_path) as conn:
        [[count]] = conn.execute("SELECT COUNT(*) FROM beads")
        return count


def get_bead_file_paths_in_index(box_index: BoxIndex) -> set[str]:
    """Return set of file paths currently in the index."""
    with sqlite3.connect(box_index.index_path) as conn:
        cursor = conn.execute("SELECT file_path FROM beads")
        return {row[0] for row in cursor}


@pytest.fixture
def box_directory(tmp_path: Path) -> Path:
    """Create a directory for a test box."""
    return tmp_path / "box"


@pytest.fixture
def box_index(box_directory: Path) -> BoxIndex:
    """Create a BoxIndex instance."""
    box_directory.mkdir()
    return BoxIndex(box_directory)


def create_unindexed_bead(box_directory: Path, name: str, kind: str = "test-kind"):
    """Helper to create a valid bead archive file without indexing it."""
    ws_path = box_directory / f"ws_{name}"
    ws = Workspace(ws_path)
    ws.create(kind)

    # Create the archive directly without using Box.store() to avoid auto-indexing
    freeze_time = "20230101T000000000000+0000"
    zipfilename = box_directory / f'{name}_{freeze_time}.zip'
    ws.pack(zipfilename, freeze_time=freeze_time, comment="Test bead archive")

    return zipfilename


def create_indexed_bead(box_directory: Path, name: str, kind: str = "test-kind"):
    """Helper to create a valid bead archive in the box."""
    ws_path = box_directory / f"ws_{name}"
    ws = Workspace(ws_path)
    ws.create(kind)
    # A real Box object would be needed to properly store,
    # but for testing the indexer, creating a dummy zip is sufficient
    # if we mock the validation. For now, let's create a real one.
    from .box import Box
    box = Box("test", box_directory)
    box.store(ws, "20230101T000000000000+0000")


def create_invalid_file(box_directory: Path, name: str):
    """Helper to create a file that is not a valid bead."""
    (box_directory / name).touch()


def test_rebuild_success(box_directory: Path, box_index: BoxIndex):
    create_indexed_bead(box_directory, "bead1")
    create_indexed_bead(box_directory, "bead2")

    progress_updates = list(box_index.rebuild())

    assert len(progress_updates) == 2
    # Check the final progress update
    final_progress = progress_updates[-1]
    assert final_progress.total == 2
    assert final_progress.processed == 2
    assert final_progress.error_count == 0
    assert final_progress.latest_error is None

    # Verify the index contains the beads
    assert count_beads_in_index(box_index) == 2


def test_rebuild_with_invalid_files(box_directory: Path, box_index: BoxIndex):
    create_indexed_bead(box_directory, "good_bead")
    create_invalid_file(box_directory, "bad_file.zip")

    progress_updates = list(box_index.rebuild())

    assert len(progress_updates) == 2
    final_progress = progress_updates[-1]
    assert final_progress.total == 2
    assert final_progress.error_count == 1

    # Find the error progress update
    error_update = next(p for p in progress_updates if p.latest_error)
    assert error_update is not None
    assert "bad_file.zip" in str(error_update.path)

    # Verify that only the good bead is in the index
    file_paths = get_bead_file_paths_in_index(box_index)
    assert len(file_paths) == 1
    assert any("good_bead" in path for path in file_paths)


@patch("bead.box_index.sqlite3.connect")
def test_rebuild_fatal_db_error(mock_connect, box_directory: Path, box_index: BoxIndex):
    create_indexed_bead(box_directory, "bead1")

    # Simulate a database error
    mock_connect.side_effect = sqlite3.Error("Test DB error")

    with pytest.raises(BoxIndexError, match="Test DB error"):
        # Consume the generator to trigger the error
        list(box_index.rebuild())


def test_sync_add_new_file(box_directory: Path, box_index: BoxIndex):
    # Start with one bead in the index
    create_unindexed_bead(box_directory, "bead1")
    list(box_index.rebuild())

    # Add a new bead (without indexing it)
    create_unindexed_bead(box_directory, "bead2")

    # Run sync
    progress_updates = list(box_index.sync())

    assert len(progress_updates) == 1
    final_progress = progress_updates[0]
    assert final_progress.total == 1
    assert "bead2" in str(final_progress.path)
    assert final_progress.error_count == 0

    # Verify both beads are now in the index
    assert count_beads_in_index(box_index) == 2


def test_sync_remove_deleted_file(box_directory: Path, box_index: BoxIndex):
    create_unindexed_bead(box_directory, "bead1")
    create_unindexed_bead(box_directory, "bead2")
    list(box_index.rebuild())

    # Delete one of the bead files
    bead2_path = next(box_directory.glob("*bead2*.zip"))
    bead2_path.unlink()

    progress_updates = list(box_index.sync())

    assert len(progress_updates) == 1
    final_progress = progress_updates[0]
    assert final_progress.total == 1
    assert "bead2" in str(final_progress.path)
    assert final_progress.error_count == 0

    # Verify only the remaining bead is in the index
    file_paths = get_bead_file_paths_in_index(box_index)
    assert len(file_paths) == 1
    assert any("bead1" in path for path in file_paths)


def test_sync_mixed_operations(box_directory: Path, box_index: BoxIndex):
    # Start with two beads
    create_unindexed_bead(box_directory, "bead1")
    create_unindexed_bead(box_directory, "bead2_to_delete")
    list(box_index.rebuild())

    # Delete one bead and add a new one
    bead2_path = next(box_directory.glob("*bead2_to_delete*.zip"))
    bead2_path.unlink()
    create_unindexed_bead(box_directory, "bead3_new")

    progress_updates = list(box_index.sync())

    assert len(progress_updates) == 2
    assert progress_updates[-1].total == 2
    paths_synced = {str(p.path) for p in progress_updates}
    assert any("bead2_to_delete" in path for path in paths_synced)
    assert any("bead3_new" in path for path in paths_synced)

    # Verify the correct beads are in the final index
    file_paths = get_bead_file_paths_in_index(box_index)
    assert len(file_paths) == 2
    assert any("bead1" in path for path in file_paths)
    assert any("bead3_new" in path for path in file_paths)
    assert not any("bead2_to_delete" in path for path in file_paths)
