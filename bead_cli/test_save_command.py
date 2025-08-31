import os

from bead.tech.fs import rmtree
import pytest

from bead.box import Box
from bead.workspace import Workspace

from .test_robot import Robot


def test_invalid_workspace_causes_error(robot):
    with pytest.raises(SystemExit):
        robot.cli('save')
    assert 'ERROR' in robot.stderr


def test_on_success_there_is_feedback(robot, box):
    robot.cli('new', 'bead')
    robot.cd('bead')
    robot.cli('save')
    assert robot.stdout != '', 'Expected some feedback, but got none :('


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='missing os.symlink')
def test_symlink_is_resolved_on_save(robot, box):
    # create a workspace with a symlink to a file
    robot.cli('new', 'bead')
    robot.cd('bead')
    robot.write_file('file', 'content')
    with robot.environment:
        os.symlink('file', 'symlink')
    # save to box & clean up
    robot.cli('save')
    robot.cd('..')
    robot.cli('discard', 'bead')

    robot.cli('develop', 'bead')
    assert 'content' in robot.read_file(robot.cwd / 'bead/symlink')


@pytest.fixture
def robot_no_box():
    with Robot() as robot_instance:
        yield robot_instance


def test_a_box_is_created_with_known_name(robot_no_box):
    robot = robot_no_box
    robot.cli('new', 'bead')
    robot.cd('bead')
    robot.cli('save')
    # there is a message on stderr that a new box has been created
    assert 'home' in robot.stderr
    # a new box with name `home` has been indeed created and it has exactly one bead
    with robot.environment as env:
        homebox = env.get_box('home')
    assert 1 == bead_count(homebox)


def bead_count(box, kind=None):
    return sum(1 for bead in box.all_beads() if kind in [None, bead.kind])


@pytest.fixture
def robot_multi_box(tmp_path_factory):
    with Robot() as robot_instance:
        yield robot_instance


@pytest.fixture
def make_box(robot_multi_box, tmp_path_factory):
    def _make_box(name):
        directory = tmp_path_factory.mktemp(f"box_{name}")
        robot_multi_box.cli('box', 'add', name, directory)
        return Box(name, directory)
    return _make_box


@pytest.fixture
def box1(make_box):
    return make_box('box1')


@pytest.fixture
def box2(make_box):
    return make_box('box2')


def test_save_dies_without_explicit_box(robot_multi_box, box1, box2):
    robot = robot_multi_box
    robot.cli('new', 'bead')
    with pytest.raises(SystemExit):
        robot.cli('save', 'bead')
    assert 'ERROR' in robot.stderr


def test_save_stores_bead_in_specified_box(robot_multi_box, box1, box2):
    robot = robot_multi_box
    robot.cli('new', 'bead')
    robot.cli('save', box1.name, '--workspace=bead')
    with robot.environment:
        kind = Workspace('bead').kind
    assert 1 == bead_count(box1, kind)
    assert 0 == bead_count(box2, kind)
    robot.cli('save', box2.name, '-w', 'bead')
    assert 1 == bead_count(box1, kind)
    assert 1 == bead_count(box2, kind)


def test_invalid_box_specified(robot_multi_box, box1, box2):
    robot = robot_multi_box
    robot.cli('new', 'bead')
    with pytest.raises(SystemExit):
        robot.cli('save', 'unknown-box', '--workspace', 'bead')
    assert 'ERROR' in robot.stderr


def test_save_to_box_without_backing_directory(robot_multi_box, box1, box2):
    robot = robot_multi_box
    robot.cli('new', 'bead')
    rmtree(box2.directory)
    with pytest.raises(SystemExit):
        robot.cli('save', box2.name, '-w', 'bead')
    assert 'ERROR' in robot.stderr
    assert 'does not exist' in robot.stderr
