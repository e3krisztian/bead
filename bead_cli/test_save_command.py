import os

from bead.test import TestCase, skipIf

from . import test_fixtures as fixtures
from bead.workspace import Workspace
from bead.box import Box


class Test(TestCase, fixtures.RobotAndBeads):

    def test_invalid_workspace_causes_error(self, robot):
        self.assertRaises(SystemExit, robot.cli, 'save')
        assert 'ERROR' in robot.stderr

    def test_on_success_there_is_feedback(self, robot, box):
        robot.cli('new', 'bead')
        robot.cd('bead')
        robot.cli('save')
        assert robot.stdout != '', 'Expected some feedback, but got none :('

    @skipIf(not hasattr(os, 'symlink'), 'missing os.symlink')
    def test_symlink_is_resolved_on_save(self, robot, box):
        # create a workspace with a symlink to a file
        robot.cli('new', 'bead')
        robot.cd('bead')
        robot.write_file('file', 'content')
        with robot.environment:
            os.symlink('file', 'symlink')
        # save to box & clean up
        robot.cli('save')
        robot.cd('..')
        robot.cli('zap', 'bead')

        robot.cli('develop', 'bead')
        self.assert_file_contains(robot.cwd / 'bead/symlink', 'content')


class Test_no_box(TestCase):

    # fixtures
    def robot(self):
        return self.useFixture(fixtures.Robot())

    # tests
    def test_a_box_is_created_with_known_name(self, robot):
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


class Test_more_than_one_boxes(TestCase):
    # fixtures
    def robot(self):
        return self.useFixture(fixtures.Robot())

    def make_box(self, robot, name):
        directory = self.new_temp_dir()
        robot.cli('box', 'add', name, directory)
        return Box(name, directory)

    def box1(self, robot):
        return self.make_box(robot, 'box1')

    def box2(self, robot):
        return self.make_box(robot, 'box2')

    # tests
    def test_save_dies_without_explicit_box(self, robot, box1, box2):
        robot.cli('new', 'bead')
        self.assertRaises(SystemExit, robot.cli, 'save', 'bead')
        assert 'ERROR' in robot.stderr

    def test_save_stores_bead_in_specified_box(self, robot, box1, box2):
        robot.cli('new', 'bead')
        robot.cli('save', box1.name, '--workspace=bead')
        with robot.environment:
            kind = Workspace('bead').kind
        assert 1 == bead_count(box1, kind)
        assert 0 == bead_count(box2, kind)
        robot.cli('save', box2.name, '-w', 'bead')
        assert 1 == bead_count(box1, kind)
        assert 1 == bead_count(box2, kind)

    def test_invalid_box_specified(self, robot, box1, box2):
        robot.cli('new', 'bead')
        self.assertRaises(
            SystemExit,
            robot.cli, 'save', 'unknown-box', '--workspace', 'bead')
        assert 'ERROR' in robot.stderr
