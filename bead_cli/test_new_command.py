import os

import pytest

from .test_robot import Robot


@pytest.fixture
def robot():
    with Robot() as robot_instance:
        yield robot_instance


@pytest.fixture
def cli(robot):
    return robot.cli


@pytest.fixture
def cwd(robot):
    return robot.cwd


def test_new_fails_if_directory_exists(cli, cwd, robot):
    os.makedirs(cwd / 'workspace')
    with pytest.raises(SystemExit):
        cli('new', 'workspace')
    assert 'ERROR' in robot.stderr
    assert 'workspace' not in robot.stdout
