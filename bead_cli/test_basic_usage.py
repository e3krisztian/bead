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
def cd(robot):
    return robot.cd


@pytest.fixture
def ls(robot):
    return robot.ls


@pytest.fixture
def box_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("box")


def test_basic_command_line(robot, cli, cd, ls, box_dir):
    print(f'home: {robot.home}')

    cli('new', 'something')
    assert 'something' in robot.stdout

    cd('something')
    cli('status')
    assert 'Inputs' not in robot.stdout

    cli('box', 'add', 'default', box_dir)
    cli('save')

    cd('..')
    cli('develop', 'something', 'something-develop')
    assert robot.cwd / 'something-develop' in ls()

    cd('something-develop')
    cli('input', 'add', 'older-self', 'something')
    cli('status')
    assert 'Inputs' in robot.stdout
    assert 'older-self' in robot.stdout

    cli('web')

    # this might leave behind the empty directory on windows
    cli('zap')
    cd('..')
    cli('zap', 'something')

    something_develop_dir = robot.home / 'something-develop'
    if os.path.exists(something_develop_dir):
        # on windows it is not possible to remove
        # the current working directory (zap does this)
        assert os.name != 'posix', 'Must be removed on posix'
        assert [] == ls(something_develop_dir)
        os.rmdir(something_develop_dir)
    assert [] == ls(robot.home)
