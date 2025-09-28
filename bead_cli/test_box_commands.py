import os
import sqlite3

import pytest

from bead.tech.timestamp import timestamp as now_ts
from bead.workspace import Workspace

from .test_robot import Robot


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
    with Robot() as robot:
        robot.cli('box', 'add', 'bobbox', box)
        yield robot


@pytest.fixture
def bob(box):
    with Robot() as robot:
        robot.cli('box', 'add', 'alicebox', box)
        yield robot


def test_shared_box_update(alice, bob, bead):
    bob.cli('new', 'bobbead')
    bob.cd('bobbead')
    bob.cli('input', 'add', 'alicebead1', bead)
    bob.cli('input', 'add', 'alicebead2', bead)

    alice.cli('edit', bead)
    alice.cd('bead')
    alice.write_file('output/datafile', '''Alice's new data''')
    alice.cli('save')

    # update only one input
    bob.cli('input', 'update', 'alicebead1', '--no-name')

    datafile1 = bob.cwd / 'input/alicebead1/datafile'
    assert datafile1.exists()
    assert '''Alice's new data''' in datafile1.read_text()

    # second input directory not changed
    datafile2 = bob.cwd / 'input/alicebead2/datafile'
    assert not datafile2.exists()

    # update all inputs
    bob.cli('input', 'update', '--no-name')

    assert datafile2.exists()
    assert '''Alice's new data''' in datafile2.read_text()


@pytest.fixture
def dir1(robot):
    os.makedirs(robot.cwd / 'dir1')
    return 'dir1'


@pytest.fixture
def dir2(robot):
    os.makedirs(robot.cwd / 'dir2')
    return 'dir2'


def test_list_when_there_are_no_boxes():
    with Robot() as robot:
        robot.cli('box', 'list')
        assert 'There are no defined boxes' in robot.stdout


def test_add_non_existing_directory_fails(robot):
    robot.cli('box', 'add', 'notadded', 'non-existing', expect_failure=True)
    assert 'ERROR' in robot.stderr
    assert 'notadded' not in robot.stdout


def test_add_multiple(robot, dir1, dir2):
    robot.cli('box', 'add', 'name1', 'dir1')
    robot.cli('box', 'add', 'name2', 'dir2')
    assert 'ERROR' not in robot.stdout

    robot.cli('box', 'list')
    assert 'name1' in robot.stdout
    assert 'name2' in robot.stdout
    assert 'dir1' in robot.stdout
    assert 'dir2' in robot.stdout


def test_add_with_same_name_fails(robot, dir1, dir2):
    robot.cli('box', 'add', 'name', 'dir1')
    assert 'ERROR' not in robot.stdout

    robot.cli('box', 'add', 'name', 'dir2', expect_failure=True)
    assert 'ERROR' in robot.stderr


def test_add_same_directory_twice_fails(robot, dir1):
    robot.cli('box', 'add', 'name1', dir1)
    assert 'ERROR' not in robot.stdout

    robot.cli('box', 'add', 'name2', dir1, expect_failure=True)
    assert 'ERROR' in robot.stderr


def test_forget_box(robot, dir1, dir2):
    robot.cli('box', 'add', 'box-to-delete', dir1)
    robot.cli('box', 'add', 'another-box', dir2)

    robot.cli('box', 'forget', 'box-to-delete')
    assert 'forgotten' in robot.stdout

    robot.cli('box', 'list')
    assert 'box-to-delete' not in robot.stdout
    assert 'another-box' in robot.stdout


def test_forget_nonexisting_box(robot):
    robot.cli('box', 'forget', 'non-existing')
    assert 'WARNING' in robot.stdout


def test_box_list_shows_disabled_status(robot):
    # GIVEN a box (provided by the robot fixture)
    # WHEN it is disabled
    robot.cli('box', 'disable', 'box')
    robot.cli('box', 'list')
    # THEN the status is shown
    assert '(disabled)' in robot.stdout
    assert '(enabled)' not in robot.stdout

    # WHEN it is enabled
    robot.cli('box', 'enable', 'box')
    robot.cli('box', 'list')
    # THEN the status is shown
    assert '(disabled)' not in robot.stdout
    assert '(enabled)' in robot.stdout


def test_input_add_respects_disabled_box(robot):
    # GIVEN a bead in a box (the robot fixture provides the box)
    robot.cli('new', 'source_bead')
    robot.cd('source_bead')
    robot.write_file('output/data.txt', 'hello world')
    robot.cli('save')
    robot.cd('..')
    robot.cli('new', 'consumer_workspace')
    robot.cd('consumer_workspace')

    # WHEN the box is disabled
    robot.cli('box', 'disable', 'box')

    # THEN adding an input from that box fails
    robot.cli('input', 'add', 'the_input', 'source_bead', expect_failure=True)
    assert 'ERROR: Not a known bead name: source_bead' in robot.stderr
    assert not (robot.cwd / 'input' / 'the_input').is_dir()

    # WHEN the box is re-enabled
    robot.cli('box', 'enable', 'box')

    # THEN adding the input succeeds
    robot.cli('input', 'add', 'the_input', 'source_bead')
    input_file = robot.cwd / 'input' / 'the_input' / 'data.txt'
    assert input_file.is_file()
    assert 'hello world' in input_file.read_text()


def test_reindex_handles_corrupted_index_without_circular_error(robot):
    """Test that 'bead box reindex' can handle corrupted box index without circular dependency."""

    # Create a box with valid setup
    testbox_dir = robot.cwd / 'testbox_dir'
    testbox_dir.mkdir()
    robot.cli('box', 'add', 'testbox', testbox_dir)

    # Get the box and corrupt its index by writing invalid schema version
    with robot.environment as env:
        box = env.get_box('testbox')
        index_path = box.index.index_path

        # Corrupt the database by setting invalid schema version (use version 1, current is 3)
        with sqlite3.connect(str(index_path)) as conn:
            conn.execute("PRAGMA user_version = 1")
            conn.commit()

    # Now the box index is corrupted. Trying to get_boxes() would raise BoxIndexError
    # that suggests running reindex, but reindex itself calls get_boxes() - circular!

    # The reindex command should handle this by using get_all_boxes() instead of get_boxes()
    # or by directly accessing the specific box without going through get_boxes()
    robot.cli('box', 'reindex', 'testbox')  # This should work without circular error

    # Verify the command succeeded (no error output about circular reindex suggestion)
    assert 'ERROR' not in robot.stderr
    assert 'reindex' not in robot.stderr.lower()  # No circular suggestion to run reindex


def test_reindex_auto_detect_with_corrupted_index(robot):
    """Test that 'bead box reindex' (no args) handles corrupted index without circular dependency."""
    # Create a single box - this should allow auto-detection
    testbox_dir = robot.cwd / 'testbox_dir'
    testbox_dir.mkdir()
    robot.cli('box', 'add', 'testbox', testbox_dir)

    # Corrupt the box index directly without creating Box object
    with robot.environment as env:
        index_path = env.get_box_index_path('testbox')

        # Corrupt the database by setting invalid schema version
        with sqlite3.connect(str(index_path)) as conn:
            conn.execute("PRAGMA user_version = 1")
            conn.commit()

    # Now try reindex without specifying box name - should auto-detect
    robot.cli('box', 'reindex')  # No box name - should auto-detect single box

    # Verify the command succeeded
    assert robot.exit_code == 0
    assert 'ERROR' not in robot.stderr


def test_reindex_all_with_corrupted_indexes(robot):
    """Test that 'bead box reindex --all' handles corrupted indexes without circular dependency."""
    # Create two boxes
    testbox1_dir = robot.cwd / 'testbox1_dir'
    testbox1_dir.mkdir()
    robot.cli('box', 'add', 'testbox1', testbox1_dir)

    testbox2_dir = robot.cwd / 'testbox2_dir'
    testbox2_dir.mkdir()
    robot.cli('box', 'add', 'testbox2', testbox2_dir)

    # Corrupt both box indexes directly without creating Box objects
    with robot.environment as env:
        for box_name in ['testbox1', 'testbox2']:
            index_path = env.get_box_index_path(box_name)

            # Corrupt the database by setting invalid schema version
            with sqlite3.connect(str(index_path)) as conn:
                conn.execute("PRAGMA user_version = 1")
                conn.commit()

    # Now try reindex --all - should handle both corrupted indexes
    robot.cli('box', 'reindex', '--all')

    # Verify the command succeeded
    assert robot.exit_code == 0
    assert 'ERROR' not in robot.stderr
