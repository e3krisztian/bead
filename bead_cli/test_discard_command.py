import os
import platform



def test_with_default_workspace(shell, bead_with_inputs):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.bead('discard')

    assert bead_with_inputs in shell.stdout


def test_with_explicit_workspace(shell, bead_with_inputs):
    shell.bead('edit', bead_with_inputs)
    shell.bead('discard', bead_with_inputs)

    assert bead_with_inputs in shell.stdout


def test_invalid_workspace(shell):
    shell.bead('discard', expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_force_invalid_workspace(shell):
    shell.bead('discard', '--force')
    # On Windows, the current working directory cannot be removed while in use
    if platform.system() != 'Windows':
        assert not os.path.exists(shell.cwd)
    assert 'ERROR' not in shell.stderr
