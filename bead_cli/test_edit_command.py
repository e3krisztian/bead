import os


from bead import layouts
from bead.workspace import Workspace


def test_by_name(shell, bead_a):
    shell.bead('edit', bead_a)

    assert Workspace(shell.cwd / bead_a).is_valid
    assert bead_a in shell.read_file(shell.cwd / bead_a / 'README')


def test_missing_bead(shell, bead_a):
    shell.bead('box', 'forget', 'box')
    shell.bead('edit', bead_a, expect_failure=True)
    assert 'Bead' in shell.stderr
    assert 'not found' in shell.stderr


def assert_edit_version(shell, timestamp, *bead_spec):
    assert bead_spec[0] == 'bead_with_history'
    shell.bead('edit', *bead_spec)
    assert os.path.exists(shell.cwd / 'bead_with_history' / f'sentinel-{timestamp}')


def test_last_version(shell, bead_with_history, times):
    assert_edit_version(shell, times.TS_LAST, bead_with_history)


def test_at_time(shell, bead_with_history, times):
    assert_edit_version(shell, times.TS1, 'bead_with_history', '-t', times.TS1)


def test_hacked_bead_is_detected(shell, hacked_bead):
    shell.bead('edit', hacked_bead, expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_review_flag(shell, bead_a):
    shell.bead('edit', '--review', bead_a)
    ws = shell.cwd / bead_a

    assert Workspace(ws).is_valid

    # output must be unpacked as well!
    assert bead_a in shell.read_file(ws / layouts.Workspace.OUTPUT / 'README')


def test_dies_if_directory_exists(shell, bead_a):
    os.makedirs(shell.cwd / bead_a)
    shell.bead('edit', bead_a, expect_failure=True)
    assert 'ERROR' in shell.stderr
