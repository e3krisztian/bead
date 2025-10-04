import os

import pytest

from bead.tech import sqlite
from bead.tech.timestamp import timestamp as now_ts
from bead.workspace import Workspace

from .test_shell import Shell


@pytest.fixture
def box(tmp_path_factory):
    return tmp_path_factory.mktemp("box")


@pytest.fixture
def timestamp():
    return now_ts()


@pytest.fixture
def bead(tmp_path_factory, timestamp):
    tmp = tmp_path_factory.mktemp("bead_tmp")
    ws = Workspace(tmp / 'ws')
    ws.create('a bead kind')
    bead_archive = tmp / 'bead.zip'
    ws.pack(bead_archive, timestamp, comment='bead for a shared box')
    return bead_archive


@pytest.fixture
def alice(box):
    with Shell() as shell:
        shell.bead('box', 'add', 'bobbox', box)
        yield shell


@pytest.fixture
def bob(box):
    with Shell() as shell:
        shell.bead('box', 'add', 'alicebox', box)
        yield shell


def test_shared_box_update(alice, bob, bead):
    bob.bead('new', 'bobbead')
    bob.cd('bobbead')
    bob.bead('input', 'add', 'alicebead1', bead)
    bob.bead('input', 'add', 'alicebead2', bead)

    alice.bead('edit', bead)
    alice.cd('bead')
    alice.write_file('output/datafile', '''Alice's new data''')
    alice.bead('save')

    # update only one input
    bob.bead('input', 'update', 'alicebead1', '--no-name')

    datafile1 = bob.cwd / 'input/alicebead1/datafile'
    assert datafile1.exists()
    assert '''Alice's new data''' in datafile1.read_text()

    # second input directory not changed
    datafile2 = bob.cwd / 'input/alicebead2/datafile'
    assert not datafile2.exists()

    # update all inputs
    bob.bead('input', 'update', '--no-name')

    assert datafile2.exists()
    assert '''Alice's new data''' in datafile2.read_text()


@pytest.fixture
def dir1(shell):
    os.makedirs(shell.cwd / 'dir1')
    return 'dir1'


@pytest.fixture
def dir2(shell):
    os.makedirs(shell.cwd / 'dir2')
    return 'dir2'


def test_list_when_there_are_no_boxes():
    with Shell() as shell:
        shell.bead('box', 'list')
        assert 'There are no defined boxes' in shell.stdout


def test_add_non_existing_directory_fails(shell):
    shell.bead('box', 'add', 'notadded', 'non-existing', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'notadded' not in shell.stdout


def test_add_multiple(shell, dir1, dir2):
    shell.bead('box', 'add', 'name1', 'dir1')
    shell.bead('box', 'add', 'name2', 'dir2')
    assert 'ERROR' not in shell.stdout

    shell.bead('box', 'list')
    assert 'name1' in shell.stdout
    assert 'name2' in shell.stdout
    assert 'dir1' in shell.stdout
    assert 'dir2' in shell.stdout


def test_add_with_same_name_fails(shell, dir1, dir2):
    shell.bead('box', 'add', 'name', 'dir1')
    assert 'ERROR' not in shell.stdout

    shell.bead('box', 'add', 'name', 'dir2', expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_add_same_directory_twice_fails(shell, dir1):
    shell.bead('box', 'add', 'name1', dir1)
    assert 'ERROR' not in shell.stdout

    shell.bead('box', 'add', 'name2', dir1, expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_forget_box(shell, dir1, dir2):
    shell.bead('box', 'add', 'box-to-delete', dir1)
    shell.bead('box', 'add', 'another-box', dir2)

    shell.bead('box', 'forget', 'box-to-delete')
    assert 'forgotten' in shell.stdout

    shell.bead('box', 'list')
    assert 'box-to-delete' not in shell.stdout
    assert 'another-box' in shell.stdout


def test_forget_nonexisting_box(shell):
    shell.bead('box', 'forget', 'non-existing')
    assert 'WARNING' in shell.stdout


def test_box_list_shows_disabled_status(shell):
    # GIVEN a box (provided by the shell fixture)
    # WHEN it is disabled
    shell.bead('box', 'disable', 'box')
    shell.bead('box', 'list')
    # THEN the status is shown
    assert '(disabled)' in shell.stdout
    assert '(enabled)' not in shell.stdout

    # WHEN it is enabled
    shell.bead('box', 'enable', 'box')
    shell.bead('box', 'list')
    # THEN the status is shown
    assert '(disabled)' not in shell.stdout
    assert '(enabled)' in shell.stdout


def test_input_add_respects_disabled_box(shell):
    # GIVEN a bead in a box (the shell fixture provides the box)
    shell.bead('new', 'source_bead')
    shell.cd('source_bead')
    shell.write_file('output/data.txt', 'hello world')
    shell.bead('save')
    shell.cd('..')
    shell.bead('new', 'consumer_workspace')
    shell.cd('consumer_workspace')

    # WHEN the box is disabled
    shell.bead('box', 'disable', 'box')

    # THEN adding an input from that box fails
    shell.bead('input', 'add', 'the_input', 'source_bead', expect_failure=True)
    assert 'ERROR: Not a known bead name: source_bead' in shell.stderr
    assert not (shell.cwd / 'input' / 'the_input').is_dir()

    # WHEN the box is re-enabled
    shell.bead('box', 'enable', 'box')

    # THEN adding the input succeeds
    shell.bead('input', 'add', 'the_input', 'source_bead')
    input_file = shell.cwd / 'input' / 'the_input' / 'data.txt'
    assert input_file.is_file()
    assert 'hello world' in input_file.read_text()


def test_reindex_handles_corrupted_index_without_circular_error(shell):
    """Test that 'bead box reindex' can handle corrupted box index without circular dependency."""

    # Create a box with valid setup
    testbox_dir = shell.cwd / 'testbox_dir'
    testbox_dir.mkdir()
    shell.bead('box', 'add', 'testbox', testbox_dir)

    # Get the box and corrupt its index by writing invalid schema version
    with shell.environment as env:
        box = env.get_box('testbox')
        index_path = box.index.index_path

        # Corrupt the database by setting invalid schema version (use version 1, current is 3)
        sqlite.execute(index_path, "PRAGMA user_version = 1")

    # Now the box index is corrupted. Trying to get_boxes() would raise BoxIndexError
    # that suggests running reindex, but reindex itself calls get_boxes() - circular!

    # The reindex command should handle this by using get_all_boxes() instead of get_boxes()
    # or by directly accessing the specific box without going through get_boxes()
    shell.bead('box', 'reindex', 'testbox')  # This should work without circular error

    # Verify the command succeeded (no error output about circular reindex suggestion)
    assert 'ERROR' not in shell.stderr
    assert 'reindex' not in shell.stderr.lower()  # No circular suggestion to run reindex


def test_reindex_auto_detect_with_corrupted_index(shell):
    """Test that 'bead box reindex' (no args) handles corrupted index without circular dependency."""
    # Create a single box - this should allow auto-detection
    testbox_dir = shell.cwd / 'testbox_dir'
    testbox_dir.mkdir()
    shell.bead('box', 'add', 'testbox', testbox_dir)

    # Corrupt the box index directly without creating Box object
    with shell.environment as env:
        index_path = env.get_box_index_path('testbox')

        # Corrupt the database by setting invalid schema version
        sqlite.execute(index_path, "PRAGMA user_version = 1")

    # Now try reindex without specifying box name - should auto-detect
    shell.bead('box', 'reindex')  # No box name - should auto-detect single box

    # Verify the command succeeded
    assert shell.exit_code == 0
    assert 'ERROR' not in shell.stderr


def test_reindex_all_with_corrupted_indexes(shell):
    """Test that 'bead box reindex --all' handles corrupted indexes without circular dependency."""
    # Create two boxes
    testbox1_dir = shell.cwd / 'testbox1_dir'
    testbox1_dir.mkdir()
    shell.bead('box', 'add', 'testbox1', testbox1_dir)

    testbox2_dir = shell.cwd / 'testbox2_dir'
    testbox2_dir.mkdir()
    shell.bead('box', 'add', 'testbox2', testbox2_dir)

    # Corrupt both box indexes directly without creating Box objects
    with shell.environment as env:
        for box_name in ['testbox1', 'testbox2']:
            index_path = env.get_box_index_path(box_name)

            # Corrupt the database by setting invalid schema version
            sqlite.execute(index_path, "PRAGMA user_version = 1")

    # Now try reindex --all - should handle both corrupted indexes
    shell.bead('box', 'reindex', '--all')

    # Verify the command succeeded
    assert shell.exit_code == 0
    assert 'ERROR' not in shell.stderr
