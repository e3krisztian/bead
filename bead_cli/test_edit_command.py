import os

from bead import layouts
from bead.workspace import Workspace
from .test_helpers import create_bead_family


def test_by_name(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory)
    shell.bead('edit', 'test_bead')

    assert Workspace(shell.cwd / 'test_bead').is_valid
    readme_content = shell.read_file(shell.cwd / 'test_bead' / 'README')
    assert 'test_bead' in readme_content


def test_missing_bead(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'missing_bead', [times.TS1], tmp_path_factory)
    shell.bead('box', 'forget', 'box')
    shell.bead('edit', 'missing_bead', expect_failure=True)
    assert 'Bead' in shell.stderr
    assert 'not found' in shell.stderr


def assert_edit_version(shell, timestamp, bead_spec):
    shell.bead('edit', bead_spec)
    assert os.path.exists(shell.cwd / 'test_history_bead' / f'sentinel-{timestamp}')


def test_last_version(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'test_history_bead', [times.TS1, times.TS2, times.TS3, times.TS4, times.TS5], tmp_path_factory)
    assert_edit_version(shell, times.TS5, 'test_history_bead')


def test_at_time(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'test_history_bead', [times.TS1, times.TS2, times.TS3, times.TS4, times.TS5], tmp_path_factory)
    assert_edit_version(shell, times.TS1, f'test_history_bead@{times.TS1}')


def test_hacked_bead_is_detected(shell, hacked_bead):
    shell.bead('edit', hacked_bead, expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_review_flag(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'review_bead', [times.TS1], tmp_path_factory)
    shell.bead('edit', '--review', 'review_bead')
    ws = shell.cwd / 'review_bead'

    assert Workspace(ws).is_valid

    # output must be unpacked as well!
    output_readme = shell.read_file(ws / layouts.Workspace.OUTPUT / 'README')
    assert 'review_bead' in output_readme


def test_dies_if_directory_exists(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'existing_bead', [times.TS1], tmp_path_factory)
    os.makedirs(shell.cwd / 'existing_bead')
    shell.bead('edit', 'existing_bead', expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_edit_with_invalid_time_expression_shows_error(shell, box, check, times, tmp_path_factory):
    """Test that invalid time expressions show user-friendly errors, not tracebacks."""
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory)
    shell.bead('edit', 'test_bead@invalidtime', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'Invalid time expression' in shell.stderr
    assert 'invalidtime' in shell.stderr
    # Should NOT show traceback
    assert 'Traceback' not in shell.stderr
    assert 'ValueError' not in shell.stderr


