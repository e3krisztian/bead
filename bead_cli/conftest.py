import os
import warnings
import zipfile

import pytest

from bead import layouts
from bead import tech
from bead.workspace import Workspace
from bead.ziparchive import ZipArchive
from tracelog import TRACELOG

from .test_robot import Robot

# timestamps
TS1 = '20150901T151015000001+0200'
TS2 = '20150901T151016000002+0200'
TS3 = '20150901T151017000003+0200'
TS4 = '20150901T151018000004+0200'
TS5 = '20150901T151019000005+0200'
TS_LAST = TS5


class Times:
    """Container for timestamp constants."""
    TS1 = TS1
    TS2 = TS2
    TS3 = TS3
    TS4 = TS4
    TS5 = TS5
    TS_LAST = TS_LAST


@pytest.fixture
def times():
    """Fixture providing access to timestamp constants."""
    return Times()


class CheckAssertions:
    """Helper class for test assertions."""

    def __init__(self, robot):
        self.robot = robot

    def loaded(self, input_nick, readme_content):
        """
        Fail if an incorrect bead was loaded.

        Test beads are assumed to have different README-s, so the test goes by the expected value
        of the README.
        """
        readme_path = self.robot.cwd / f'input/{input_nick}/README'
        assert readme_path.exists(), f"README file not found at {readme_path}"
        content = readme_path.read_text()
        assert readme_content in content, f"Expected '{readme_content}' in README content: {content}"

    def not_loaded(self, input_nick):
        readme_path = self.robot.cwd / f'input/{input_nick}/README'
        assert not readme_path.exists(), f"README file should not exist at {readme_path}"


@pytest.fixture
def robot():
    """
    I am a robot user with a box
    """
    with Robot() as robot_instance:
        box_dir = robot_instance.cwd / 'box'
        os.makedirs(box_dir)
        robot_instance.cli('box', 'add', 'box', box_dir)
        yield robot_instance


@pytest.fixture
def box(robot):
    with robot.environment as env:
        return env.get_box('box')


@pytest.fixture
def beads():
    return {}


@pytest.fixture
def check(robot):
    """Fixture providing assertion helpers."""
    return CheckAssertions(robot)


def _new_bead(robot, beads, box, bead_name, inputs=None, tmp_path_factory=None):
    """Helper function to create a new bead."""
    robot.cli('new', bead_name)
    robot.cd(bead_name)
    robot.write_file('README', bead_name)
    robot.write_file('output/README', bead_name)
    _add_inputs(robot, inputs)
    with robot.environment:
        TRACELOG('store', robot.cwd, TS1, 'to', box.location)
        beads[bead_name] = ZipArchive(box.store(Workspace('.'), TS1))
    robot.cd('..')
    robot.cli('discard', bead_name)
    return bead_name


def _add_inputs(robot, inputs):
    """Helper function to add inputs to a bead."""
    inputs = inputs or {}
    for name in inputs:
        robot.cli('input', 'add', name, inputs[name])


@pytest.fixture
def bead_a(robot, beads, box):
    return _new_bead(robot, beads, box, 'bead_a')


@pytest.fixture
def bead_b(robot, beads, box):
    return _new_bead(robot, beads, box, 'bead_b')


@pytest.fixture
def hacked_bead(tmp_path_factory):
    hacked_bead_path = tmp_path_factory.mktemp('hacked') / 'hacked_bead.zip'
    workspace_dir = tmp_path_factory.mktemp('workspace') / 'hacked_bead'
    ws = Workspace(workspace_dir)
    ws.create('hacked-kind')
    tech.fs.write_file(ws.directory / 'code', 'code')
    tech.fs.write_file(ws.directory / 'output/README', 'README')
    ws.pack(hacked_bead_path, TS1, comment='hacked bead')
    with zipfile.ZipFile(hacked_bead_path, 'a') as z:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            # this would cause a warning from zipfile for duplicate
            # name in zip file (which is perfectly valid, though hacky)
            z.writestr(f'{layouts.Archive.CODE}/code', 'HACKED')
            z.writestr(f'{layouts.Archive.DATA}/README', 'HACKED')
    return hacked_bead_path


def _bead_with_history(robot, box, bead_name, bead_kind, tmp_path_factory):
    """Helper function to create a bead with history."""
    def make_bead(freeze_time):
        workspace_dir = tmp_path_factory.mktemp('workspace') / bead_name
        ws = Workspace(workspace_dir)
        ws.create(bead_kind)
        sentinel_file = ws.directory / f'sentinel-{freeze_time}'
        tech.fs.write_file(sentinel_file, freeze_time)
        tech.fs.write_file(ws.directory / 'output/README', freeze_time)
        box.store(ws, freeze_time)
        tech.fs.rmtree(workspace_dir)

    with robot.environment:
        make_bead(TS1)
        make_bead(TS2)
        make_bead(TS3)
        make_bead(TS4)
        make_bead(TS5)
    return bead_name


@pytest.fixture
def bead_with_history(robot, box, tmp_path_factory):
    """
    NOTE: these beads are not added to `beads`, as they share the same name.
    """
    return _bead_with_history(
        robot, box, 'bead_with_history', 'KIND:bead_with_history', tmp_path_factory)


@pytest.fixture
def bead_with_inputs(robot, beads, box, bead_a, bead_b):
    inputs = dict(input_a=bead_a, input_b=bead_b)
    return _new_bead(robot, beads, box, 'bead_with_inputs', inputs)
