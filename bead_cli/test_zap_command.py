import os

import pytest


def test_with_default_workspace(robot, bead_with_inputs):
    robot.cli('develop', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('zap')

    assert bead_with_inputs in robot.stdout


def test_with_explicit_workspace(robot, bead_with_inputs):
    robot.cli('develop', bead_with_inputs)
    robot.cli('zap', bead_with_inputs)

    assert bead_with_inputs in robot.stdout


def test_invalid_workspace(robot):
    with pytest.raises(SystemExit):
        robot.cli('zap')
    assert 'ERROR' in robot.stderr


def test_force_invalid_workspace(robot):
    robot.cli('zap', '--force')
    assert not os.path.exists(robot.cwd)
    assert 'ERROR' not in robot.stderr
