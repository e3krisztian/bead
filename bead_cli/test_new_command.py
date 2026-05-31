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
def cwd(shell):
    return shell.cwd


def test_new_fails_if_directory_exists(bead, cwd, shell):
    os.makedirs(cwd / 'workspace')
    bead('new', 'workspace', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'workspace' not in shell.stdout


def test_new_prints_onboarding(bead, cwd, shell):
    bead('new', 'workspace')
    assert 'bead save' in shell.stdout
    assert 'input/' in shell.stdout
    assert 'output/' in shell.stdout
