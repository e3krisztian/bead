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


def test_init_empty_directory(bead, cwd, shell):
    bead('init')
    assert (cwd / 'input').is_dir()
    assert (cwd / 'output').is_dir()
    assert (cwd / 'temp').is_dir()
    assert (cwd / '.bead-meta' / 'bead').exists()
    assert 'Initialized workspace' in shell.stdout


def test_init_already_valid_workspace(bead, cwd, shell):
    bead('init')
    kind_before = (cwd / '.bead-meta' / 'bead').read_text()
    bead('init')
    kind_after = (cwd / '.bead-meta' / 'bead').read_text()
    assert 'Already a workspace.' in shell.stdout
    assert kind_before == kind_after


def test_init_existing_empty_input_dir(bead, cwd, shell):
    os.makedirs(cwd / 'input')
    bead('init')
    assert (cwd / '.bead-meta' / 'bead').exists()
    assert 'Initialized workspace' in shell.stdout


def test_init_nonempty_input_dir_no_workspace(bead, cwd, shell):
    os.makedirs(cwd / 'input')
    (cwd / 'input' / 'some_file.txt').write_text('data')
    bead('init', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'input/' in shell.stderr


def test_init_partial_workspace_preserves_kind(bead, cwd, shell):
    os.makedirs(cwd / '.bead-meta')
    os.makedirs(cwd / 'input')
    import json
    meta_content = json.dumps({'kind': 'test-kind-uuid', 'inputs': {}})
    (cwd / '.bead-meta' / 'bead').write_text(meta_content)
    bead('init')
    assert (cwd / 'output').is_dir()
    assert (cwd / 'temp').is_dir()
    assert 'test-kind-uuid' in (cwd / '.bead-meta' / 'bead').read_text()
