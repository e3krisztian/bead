import os
import platform

from .test_helpers import create_bead_family



def test_with_default_workspace(shell, box, times, tmp_path_factory):
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory)
    shell.bead('edit', 'test_bead')
    shell.cd('test_bead')
    shell.bead('discard')

    assert 'test_bead' in shell.stdout


def test_with_explicit_workspace(shell, box, times, tmp_path_factory):
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory)
    shell.bead('edit', 'test_bead')
    shell.bead('discard', 'test_bead')

    assert 'test_bead' in shell.stdout


def test_discard_invalid_workspace_fails(shell):
    shell.bead('discard', expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_force_invalid_workspace(shell):
    shell.bead('discard', '--force')
    # On Windows, the current working directory cannot be removed while in use
    if platform.system() != 'Windows':
        assert not os.path.exists(shell.cwd)
    assert 'ERROR' not in shell.stderr
