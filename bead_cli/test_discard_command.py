import os
import platform

import pytest


def test_with_default_workspace(robot, bead_with_inputs):
    robot.cli('edit', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('discard')

    assert bead_with_inputs in robot.stdout


def test_with_explicit_workspace(robot, bead_with_inputs):
    robot.cli('edit', bead_with_inputs)
    robot.cli('discard', bead_with_inputs)

    assert bead_with_inputs in robot.stdout


def test_invalid_workspace(robot):
    with pytest.raises(SystemExit):
        robot.cli('discard')
    assert 'ERROR' in robot.stderr


def test_force_invalid_workspace(robot):
    robot.cli('discard', '--force')
    # On Windows, the current working directory cannot be removed while in use
    if platform.system() != 'Windows':
        assert not os.path.exists(robot.cwd)
    assert 'ERROR' not in robot.stderr
