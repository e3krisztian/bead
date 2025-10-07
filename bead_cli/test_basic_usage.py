import os

import pytest

from .test_shell import Shell


@pytest.fixture
def shell():
    with Shell() as shell_instance:
        yield shell_instance


@pytest.fixture
def bead(shell):
    return shell.bead


@pytest.fixture
def cd(shell):
    return shell.cd


@pytest.fixture
def ls(shell):
    return shell.ls


@pytest.fixture
def box_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("box")


def test_basic_command_line(shell, bead, cd, ls, box_dir):
    print(f'home: {shell.home}')

    bead('new', 'something')
    assert 'something' in shell.stdout

    cd('something')
    bead('status')
    assert 'Inputs' not in shell.stdout

    bead('box', 'add', 'default', box_dir)
    bead('save')

    cd('..')
    bead('edit', 'something', 'something-derived')
    assert shell.cwd / 'something-derived' in ls()

    cd('something-derived')
    bead('input', 'add', 'older-self', 'something')
    bead('status')
    assert 'Inputs' in shell.stdout
    assert 'older-self' in shell.stdout

    bead('graph')

    # this might leave behind the empty directory on windows
    bead('discard')
    cd('..')
    bead('discard', 'something')

    something_derived_dir = shell.home / 'something-derived'
    if os.path.exists(something_derived_dir):
        # on windows it is not possible to remove
        # the current working directory (discard does this)
        assert os.name != 'posix', 'Must be removed on posix'
        assert [] == ls(something_derived_dir)
        os.rmdir(something_derived_dir)
    assert [] == ls(shell.home)
