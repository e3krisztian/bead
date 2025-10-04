from pathlib import Path

import pytest

import bead.zipopener
from .box_index import BoxIndex
from .exceptions import BoxIndexError
from .infra import sqlite
from .workspace import Workspace


def count_beads_in_index(box_index: BoxIndex) -> int:
    """Return the total number of beads in the index."""
    result = sqlite.query_one(box_index.index_path, "SELECT COUNT(*) FROM beads")
    return result[0]


def get_bead_file_paths_in_index(box_index: BoxIndex) -> set[str]:
    """Return set of file paths currently in the index."""
    rows = sqlite.query_all(box_index.index_path, "SELECT file_path FROM beads")
    return {row[0] for row in rows}


@pytest.fixture
def box_directory(tmp_path: Path) -> Path:
    """Create a directory for a test box."""
    box_dir = tmp_path / "box"
    box_dir.mkdir()
    return box_dir


@pytest.fixture
def box_index(box_directory: Path) -> BoxIndex:
    """Create a BoxIndex instance."""
    index_file_path = box_directory / 'index.db'
    return BoxIndex("test_box", box_directory, index_file_path)


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
    index_file_path = box_directory / 'index.db'
    box = Box("test", box_directory, index_file_path)
    box.store(ws, "20230101T000000000000+0000")


def create_invalid_file(box_directory: Path, name: str):
    """Helper to create a file that is not a valid bead."""
    (box_directory / name).touch()



def test_sync_add_new_file(box_directory: Path, box_index: BoxIndex):
    # Start with one bead in the index
    create_unindexed_bead(box_directory, "bead1")
    list(box_index.sync())

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
    list(box_index.sync())

    # Close zip cache before deleting files (Windows compatibility)
    bead.zipopener.close_all()

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
    list(box_index.sync())

    # Close zip cache before deleting files (Windows compatibility)
    bead.zipopener.close_all()

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


def test_box_index_init_unversioned_db(box_directory: Path):
    """Verify that an unversioned DB raises the correct error."""
    index_path = box_directory / "index.db"
    # Manually create an old-style, unversioned database (user_version == 0)
    sqlite.execute(index_path, "CREATE TABLE beads (name TEXT)")

    with pytest.raises(BoxIndexError, match="Index database is unversioned"):
        BoxIndex("test_box", box_directory, index_path)


def test_box_index_init_outdated_db(box_directory: Path):
    """Verify that an outdated DB raises the correct error."""
    index_path = box_directory / "index.db"
    # Manually create a database with an old schema version
    with sqlite.transaction(index_path) as conn:
        conn.execute("CREATE TABLE beads (name TEXT)")
        conn.execute("PRAGMA user_version = 1")
        conn.commit()

    # The code now expects SCHEMA_VERSION = 2
    with pytest.raises(BoxIndexError, match="Index schema is out of date"):
        BoxIndex("test_box", box_directory, index_path)


def test_box_index_init_newer_db(box_directory: Path):
    """Verify that a newer DB raises the correct error."""
    from bead.box_index import SCHEMA_VERSION
    index_path = box_directory / "index.db"
    # Manually create a database with a future schema version
    with sqlite.transaction(index_path) as conn:
        conn.execute("CREATE TABLE beads (name TEXT)")
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
        conn.commit()

    with pytest.raises(BoxIndexError, match="Index schema is from a newer version"):
        BoxIndex("test_box", box_directory, index_path)
