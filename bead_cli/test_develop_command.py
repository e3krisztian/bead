import os

import pytest

from bead import layouts
from bead.workspace import Workspace


def test_by_name(robot, bead_a):
    robot.cli('develop', bead_a)

    assert Workspace(robot.cwd / bead_a).is_valid
    assert bead_a in robot.read_file(robot.cwd / bead_a / 'README')


def test_missing_bead(robot, bead_a):
    robot.cli('box', 'forget', 'box')
    with pytest.raises(SystemExit):
        robot.cli('develop', bead_a)
    assert 'Bead' in robot.stderr
    assert 'not found' in robot.stderr


def assert_develop_version(robot, timestamp, *bead_spec):
    assert bead_spec[0] == 'bead_with_history'
    robot.cli('develop', *bead_spec)
    assert os.path.exists(robot.cwd / 'bead_with_history' / f'sentinel-{timestamp}')


def test_last_version(robot, bead_with_history, times):
    assert_develop_version(robot, times.TS_LAST, bead_with_history)


def test_at_time(robot, bead_with_history, times):
    assert_develop_version(robot, times.TS1, 'bead_with_history', '-t', times.TS1)


def test_hacked_bead_is_detected(robot, hacked_bead):
    with pytest.raises(SystemExit):
        robot.cli('develop', hacked_bead)
    assert 'ERROR' in robot.stderr


def test_extract_output(robot, bead_a):
    robot.cli('develop', '-x', bead_a)
    ws = robot.cwd / bead_a

    assert Workspace(ws).is_valid

    # output must be unpacked as well!
    assert bead_a in robot.read_file(ws / layouts.Workspace.OUTPUT / 'README')


def test_dies_if_directory_exists(robot, bead_a):
    os.makedirs(robot.cwd / bead_a)
    with pytest.raises(SystemExit):
        robot.cli('develop', bead_a)
    assert 'ERROR' in robot.stderr
