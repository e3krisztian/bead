import os

from bead.tech.fs import rmtree
from bead.workspace import Workspace
from .test_helpers import create_bead_family, get_bead_archive, create_bead_with_inputs


# Test Quality Guidelines:
# - Always verify actual file content changes, not just stdout messages
# - Use README files consistently rather than inventing new files
# - Ensure original bead files aren't mixed with new bead output files
# - Content verification proves updates actually worked, not just that commands succeeded


def test_basic_usage(shell, box, check, times, tmp_path_factory):
    # Create a bead family with multiple versions
    create_bead_family(box, 'history_bead', [times.TS1, times.TS2, times.TS3, times.TS4, times.TS5], tmp_path_factory)

    # nextbead with input1 as databead1
    shell.bead('new', 'nextbead')
    shell.cd('nextbead')
    # add version TS2
    shell.bead('input', 'add', 'input1', 'history_bead', '--time', times.TS2)
    check.loaded('input1', times.TS2)
    shell.bead('save')
    shell.cd('..')
    shell.bead('discard', 'nextbead')

    shell.bead('edit', 'nextbead')
    shell.cd('nextbead')
    assert not os.path.exists(shell.cwd / 'input/input1')

    shell.bead('input', 'load')
    assert os.path.exists(shell.cwd / 'input/input1')

    shell.bead('input', 'add', 'input2', 'history_bead')
    assert os.path.exists(shell.cwd / 'input/input2')

    shell.bead('input', 'delete', 'input1')
    assert not os.path.exists(shell.cwd / 'input/input1')

    # no-op load do not crash
    shell.bead('input', 'load')

    shell.bead('status')


def test_update_unloaded_input_w_another_bead(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'input_bead_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'input_bead_b', [times.TS2], tmp_path_factory)
    create_bead_family(box, 'replacement_bead', [times.TS3], tmp_path_factory)

    # Create bead with inputs
    create_bead_with_inputs(shell, box, 'test_with_inputs',
                          {'input_a': 'input_bead_a', 'input_b': 'input_bead_b'},
                          times.TS4, tmp_path_factory)

    shell.bead('edit', 'test_with_inputs')
    shell.cd('test_with_inputs')

    shell.bead('status')
    assert 'input_bead_b' in shell.stdout

    assert not Workspace(shell.cwd).is_loaded('input_b')

    shell.bead('input', 'update', 'input_b', 'replacement_bead')
    check.loaded('input_b', 'replacement_bead')

    shell.bead('status')
    assert 'input_bead_b' not in shell.stdout


def test_load_on_workspace_without_input_gives_feedback(shell, box, check, times, tmp_path_factory):
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'load')

    assert 'WARNING' in shell.stderr
    assert 'No inputs defined to load.' in shell.stderr


def test_load_with_missing_bead_gives_warning(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'missing_input_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'missing_input_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    create_bead_with_inputs(shell, box, 'test_with_inputs',
                          {'input_a': 'missing_input_a', 'input_b': 'missing_input_b'},
                          times.TS3, tmp_path_factory)

    shell.bead('edit', 'test_with_inputs')
    shell.cd('test_with_inputs')
    shell.reset()
    shell.bead('input', 'load')
    assert 'WARNING' in shell.stderr
    assert 'input_a' in shell.stderr
    assert 'input_b' in shell.stderr


def test_load_only_one_input(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'load_input_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'load_input_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    create_bead_with_inputs(shell, box, 'test_with_inputs',
                          {'input_a': 'load_input_a', 'input_b': 'load_input_b'},
                          times.TS3, tmp_path_factory)

    shell.bead('edit', 'test_with_inputs')
    shell.cd('test_with_inputs')
    shell.bead('input', 'load', 'input_a')
    check.loaded('input_a', 'load_input_a')
    with shell.environment:
        assert not Workspace('.').is_loaded('input_b')


def test_deleted_box_does_not_stop_load(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'deleted_input_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'deleted_input_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    create_bead_with_inputs(shell, box, 'test_with_inputs',
                          {'input_a': 'deleted_input_a', 'input_b': 'deleted_input_b'},
                          times.TS3, tmp_path_factory)

    deleted_box = tmp_path_factory.mktemp("deleted_box")
    shell.bead('box', 'add', 'missing', deleted_box)
    rmtree(deleted_box)
    shell.bead('edit', 'test_with_inputs')
    shell.cd('test_with_inputs')
    shell.bead('input', 'load')


def test_add_with_unrecognized_bead_name_exits_with_error(shell, box, check, times, tmp_path_factory):
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'x', 'non-existing-bead', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'non-existing-bead' in shell.stderr


def test_add_with_path_separator_in_name_is_error(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'test_bead_b', [times.TS2], tmp_path_factory)
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'name/with/path/separator', 'test_bead_b', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'name/with/path/separator' in shell.stderr

    shell.bead('status')
    assert 'test_bead_b' not in shell.stdout
    assert [] == list(shell.ls('input'))


def test_add_with_hacked_bead_is_refused(shell, hacked_bead, box, check, times, tmp_path_factory):
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'hack', hacked_bead)
    assert not Workspace(shell.cwd).has_input('hack')
    assert 'WARNING' in shell.stderr


def test_update_with_hacked_bead_is_refused(shell, hacked_bead, box, check, times, tmp_path_factory):
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory)
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'intelligence', 'test_bead')
    shell.bead('input', 'update', 'intelligence', hacked_bead)
    check.loaded('intelligence', 'test_bead')
    assert 'WARNING' in shell.stderr


def test_update_to_next_version(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'history_bead', [times.TS1, times.TS2, times.TS3, times.TS4, times.TS5], tmp_path_factory)

    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    # add version TS1
    shell.bead('input', 'add', 'input1', 'history_bead', '--time', times.TS1)
    check.loaded('input1', times.TS1)

    shell.bead('input', 'update', 'input1', '--next', '--no-name')
    check.loaded('input1', times.TS2)

    shell.bead('input', 'update', 'input1', '-N', '--no-name')
    check.loaded('input1', times.TS3)


def test_update_to_previous_version(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'history_bead', [times.TS1, times.TS2, times.TS3, times.TS4, times.TS5], tmp_path_factory)

    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    # add version TS4
    shell.bead('input', 'add', 'input1', 'history_bead', '--time', times.TS4)
    check.loaded('input1', times.TS4)

    shell.bead('input', 'update', 'input1', '--prev', '--no-name')
    check.loaded('input1', times.TS3)

    shell.bead('input', 'update', 'input1', '-P', '--no-name')
    check.loaded('input1', times.TS2)


def test_update_up_to_date_inputs_is_noop(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'test_bead_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'test_bead_b', [times.TS2], tmp_path_factory)
    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    shell.bead('input', 'add', 'test_bead_a')
    shell.bead('input', 'add', 'test_bead_b')

    def files_with_times():
        basepath = shell.cwd
        for dirpath, dirs, files in os.walk(basepath):
            for file in files:
                filename = os.path.join(dirpath, file)
                yield filename, os.path.getctime(filename)

    orig_files = sorted(files_with_times())
    shell.bead('input', 'update')
    after_update_files = sorted(files_with_times())
    assert orig_files == after_update_files

    assert 'Skipping update of test_bead_a:' in shell.stdout
    assert 'Skipping update of test_bead_b:' in shell.stdout


def test_update_with_same_bead_is_noop(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory)
    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    shell.bead('input', 'add', 'test_bead')

    def files_with_times():
        basepath = shell.cwd
        for dirpath, dirs, files in os.walk(basepath):
            for file in files:
                filename = os.path.join(dirpath, file)
                yield filename, os.path.getctime(filename)

    orig_files = sorted(files_with_times())
    shell.bead('input', 'update', 'test_bead')
    after_update_files = sorted(files_with_times())
    assert orig_files == after_update_files

    assert 'Skipping update of test_bead:' in shell.stdout


def test_unload_all(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'unload_input_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'unload_input_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    create_bead_with_inputs(shell, box, 'test_with_inputs',
                          {'input_a': 'unload_input_a', 'input_b': 'unload_input_b'},
                          times.TS3, tmp_path_factory)

    shell.bead('edit', 'test_with_inputs')
    shell.cd('test_with_inputs')
    shell.bead('input', 'load')
    assert os.path.exists(shell.cwd / 'input/input_a')
    assert os.path.exists(shell.cwd / 'input/input_b')

    shell.bead('input', 'unload')
    assert not os.path.exists(shell.cwd / 'input/input_a')
    assert not os.path.exists(shell.cwd / 'input/input_b')


def test_unload(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'selective_input_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'selective_input_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    create_bead_with_inputs(shell, box, 'test_with_inputs',
                          {'input_a': 'selective_input_a', 'input_b': 'selective_input_b'},
                          times.TS3, tmp_path_factory)

    shell.bead('edit', 'test_with_inputs')
    shell.cd('test_with_inputs')
    shell.bead('input', 'load')
    assert os.path.exists(shell.cwd / 'input/input_a')
    assert os.path.exists(shell.cwd / 'input/input_b')

    shell.bead('input', 'unload', 'input_a')
    assert not os.path.exists(shell.cwd / 'input/input_a')
    assert os.path.exists(shell.cwd / 'input/input_b')

    shell.bead('input', 'unload', 'input_b')
    assert not os.path.exists(shell.cwd / 'input/input_a')
    assert not os.path.exists(shell.cwd / 'input/input_b')

    shell.bead('input', 'unload', 'input_a')
    assert not os.path.exists(shell.cwd / 'input/input_a')
    assert not os.path.exists(shell.cwd / 'input/input_b')


def test_delete_nonexisting_input(shell, box, check, times, tmp_path_factory):
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'delete', 'nonexisting', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'does not exist' in shell.stderr


def test_update_default_name_and_kind_matching(shell, box, check, times, tmp_path_factory):
    """Test default behavior: matches by both input name and kind."""
    # Create a bead family with two versions
    create_bead_family(box, 'test_bead', [times.TS1, times.TS2], tmp_path_factory)

    # Create workspace with older version as input
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'test_bead', 'test_bead', '--time', times.TS1)

    # Update should find the newer version
    shell.bead('input', 'update', 'test_bead')
    assert 'Loading new data' in shell.stdout

    # Verify we got the newer version
    readme_content = (shell.cwd / 'input' / 'test_bead' / 'README').read_text()
    assert f'test_bead_{times.TS2}' in readme_content


def test_update_explicit_bead_bypasses_matching_constraints(shell, box, check, times, tmp_path_factory):
    """Test that explicit bead references bypass matching constraints."""
    # Create initial bead
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory)

    # Create workspace with test_bead as input
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', 'test_bead')
    shell.cd('..')

    # Create a completely different bead with different name and kind
    create_bead_family(box, 'different_bead', [times.TS2], tmp_path_factory, kind='KIND:different')

    # Go back to consumer and update with explicit bead reference
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a', 'different_bead')
    assert 'Loading new data' in shell.stdout
    # Verify the content was actually updated
    readme_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert f'different_bead_{times.TS2}' in readme_content


def test_strict_matching_shows_no_update_when_only_incompatible_beads_exist(shell, box, check, times, tmp_path_factory):
    """Test that strict matching shows no update when only incompatible beads exist."""
    # Create original bead with KIND:test
    create_bead_family(box, 'test_bead', [times.TS1], tmp_path_factory, kind='KIND:test')

    # Create workspace with input
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', 'test_bead')

    # Store original content for verification
    original_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    shell.cd('..')

    # Create a different bead with same name but different kind and newer timestamp
    create_bead_family(box, 'test_bead', [times.TS2], tmp_path_factory, kind='KIND:different')

    # Go back to consumer and try to update - should succeed but show "no update" message
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a')
    # Should show that it's already at the requested version (no compatible update found)
    assert 'already at requested version' in shell.stdout

    # Verify content remains unchanged
    current_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content, "Content should remain unchanged when no compatible update exists"


def test_strict_matching_blocks_wrong_name_but_no_name_allows_it(shell, box, check, times, tmp_path_factory):
    """Test that strict matching blocks wrong name but --no-name allows it."""
    # Create original bead with name 'original_bead'
    create_bead_family(box, 'original_bead', [times.TS1], tmp_path_factory, kind='KIND:test')

    # Create workspace with input
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', 'original_bead')

    # Store original content
    original_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    shell.cd('..')

    # Create newer bead with same kind but different name
    create_bead_family(box, 'renamed_bead', [times.TS2], tmp_path_factory, kind='KIND:test')

    # Try to update with strict matching - should find no newer version with same name
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a')
    assert 'already at requested version' in shell.stdout

    # Verify content remains unchanged (renamed_bead was ignored due to name mismatch)
    current_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content

    # But --no-name should find the renamed_bead (newest by kind regardless of name)
    shell.bead('input', 'update', 'bead_a', '--no-name')
    assert 'Loading new data' in shell.stdout
    readme_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert f'renamed_bead_{times.TS2}' in readme_content


def test_strict_matching_ignores_wrong_kind_but_no_kind_allows_it(shell, box, check, times, tmp_path_factory):
    """Test that strict matching ignores wrong kind but --no-kind allows it."""
    # Create original bead with specific kind
    create_bead_family(box, 'same_name_bead', [times.TS1], tmp_path_factory, kind='KIND:original')

    # Create workspace with input from original bead
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', 'same_name_bead')

    # Store original content
    original_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    shell.cd('..')

    # Create newer bead with same name but different kind
    create_bead_family(box, 'same_name_bead', [times.TS2], tmp_path_factory, kind='KIND:different')

    # Try to update with strict matching - should find no newer version with same kind
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a')
    # Strict matching finds the original bead and sees it's already current
    assert 'already at requested version' in shell.stdout

    # Verify content remains unchanged (different kind bead was ignored)
    current_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content

    # But --no-kind should allow the update to the newer different-kind bead (matches by name only)
    shell.bead('input', 'update', 'bead_a', '--no-kind')
    assert 'Loading new data' in shell.stdout
    readme_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert f'same_name_bead_{times.TS2}' in readme_content


