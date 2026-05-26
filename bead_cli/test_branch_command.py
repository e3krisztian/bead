import pytest
from bead.workspace import Workspace
from .test_shell import Shell


@pytest.fixture
def shell():
    with Shell() as shell_instance:
        yield shell_instance


def test_branch_changes_kind(shell):
    shell.bead('new', 'ws')
    shell.cd('ws')
    with shell.environment:
        kind_before = Workspace('.').kind
    shell.bead('branch')
    with shell.environment:
        kind_after = Workspace('.').kind
    assert kind_before != kind_after


def test_branch_keeps_inputs(shell):
    shell.bead('new', 'ws')
    shell.cd('ws')
    shell.cd('..')
    shell.bead('new', 'source')
    shell.cd('source')
    shell.bead('save')
    shell.cd('..')
    shell.cd('ws')
    shell.bead('input', 'add', 'src', 'source')
    with shell.environment:
        inputs_before = [i.name for i in Workspace('.').inputs]
    shell.bead('branch')
    with shell.environment:
        inputs_after = [i.name for i in Workspace('.').inputs]
    assert inputs_before == inputs_after


def test_branch_fails_outside_workspace(shell):
    shell.bead('branch', expect_failure=True)
    assert 'ERROR' in shell.stderr
