import os

from bead.workspace import Workspace
from ..test_helpers import create_bead_family, create_bead_with_inputs


def test_update_unloaded_input_with_another_bead(shell, box, check, times, tmp_path_factory):
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


def test_update_finds_newest_by_kind_not_name(shell, box, check, times, tmp_path_factory):
    """
    Test that update command finds the newest bead by kind, ignoring bead names.
    This demonstrates the shift from name-based to kind-based updates.
    """

    # Create beads of SAME KIND but different names and times
    # create_bead_family creates beads with same kind by default
    create_bead_family(box, 'same_kind_v1', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'same_kind_v2', [times.TS2], tmp_path_factory)
    create_bead_family(box, 'same_kind_latest', [times.TS5], tmp_path_factory)  # Same kind, newest time

    # Set up workspace with inputs pointing to older copies
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'input1', 'same_kind_v1')
    shell.bead('input', 'add', 'input2', 'same_kind_v2')
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)

    # Update should find the newest bead of the same kind (same_kind_latest at TS5)
    # regardless of the original bead names, because all beads have the same kind
    shell.bead('input', 'update', '--no-name')
    check.loaded('input1', times.TS5)  # updated to newest of kind
    check.loaded('input2', times.TS5)  # updated to newest of kind


def test_explicit_bead_update_with_new_reference(shell, box, check, times, tmp_path_factory):
    """
    Test updating a specific input with an explicit bead reference.
    """

    # Set up workspace with one input
    # create_bead_family creates beads with same kind by default
    create_bead_family(box, 'source_bead', [times.TS1], tmp_path_factory)
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'test_input', 'source_bead')
    check.loaded('test_input', times.TS1)

    # Create a new bead to update to (same kind)
    create_bead_family(box, 'target_bead', [times.TS3], tmp_path_factory)
    # Create newest bead of same kind for --no-name test
    create_bead_family(box, 'latest_by_kind', [times.TS5], tmp_path_factory)

    # Update specific input with explicit bead reference
    shell.bead('input', 'update', 'test_input', 'target_bead')
    check.loaded('test_input', times.TS3)

    # Update without explicit reference should find newest by kind
    shell.bead('input', 'update', 'test_input', '--no-name')
    check.loaded('test_input', times.TS5)  # finds newest of the kind (latest_by_kind)


def test_input_mapping_preserved_during_navigation(shell, box, check, times, tmp_path_factory):
    """
    Test that input mappings are preserved during --prev/--next navigation.
    """

    # Create bead with version history for navigation testing
    create_bead_family(box, 'navigate_bead', [times.TS2, times.TS3], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up input with specific version and mapping
    shell.bead('input', 'add', 'nav_input', 'navigate_bead', '--time', times.TS2)
    check.loaded('nav_input', times.TS2)
    shell.bead('input', 'map', 'nav_input', 'navigate_bead')

    # Test navigation preserves mapping
    shell.bead('input', 'update', 'nav_input', '--next')  # Should go from TS2 to TS3
    check.loaded('nav_input', times.TS3)
    shell.bead('input', 'update', 'nav_input', '--prev')  # Should go from TS3 back to TS2
    check.loaded('nav_input', times.TS2)


def test_update_with_nonexistent_bead_shows_error(shell, box, times, tmp_path_factory):
    """Test that updating with a non-existent bead name shows proper error."""
    create_bead_family(box, 'existing_bead', [times.TS1], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'myinput', 'existing_bead')

    # Try to update with non-existent bead - should fail gracefully
    shell.bead('input', 'update', 'myinput', 'nonexistent_bead', expect_failure=True)

    # Should show error message, not crash with unhandled exception
    assert 'ERROR' in shell.stderr or 'not found' in shell.stderr.lower()


def test_update_with_new_bead_name_respects_kind_matching(shell, box, check, times, tmp_path_factory):
    """Test that updating with new bead name respects kind unless --no-kind is given."""
    # Create original bead with KIND:original
    create_bead_family(box, 'original_bead', [times.TS1], tmp_path_factory, kind='KIND:original')

    # Create two beads with same name 'new_bead' but different kinds
    create_bead_family(box, 'new_bead', [times.TS2], tmp_path_factory, kind='KIND:original')  # Matching kind
    create_bead_family(box, 'new_bead', [times.TS3], tmp_path_factory, kind='KIND:different')  # Different kind, newer

    # Create workspace with input from original_bead
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'myinput', 'original_bead')
    check.loaded('myinput', times.TS1)

    # Update to 'new_bead' WITHOUT --no-kind: should match by name AND kind
    # Should find new_bead with KIND:original (TS2), not the newer one with KIND:different (TS3)
    shell.bead('input', 'update', 'myinput', 'new_bead')
    check.loaded('myinput', times.TS2)

    # Verify the mapping was updated to new_bead
    workspace = Workspace(shell.cwd)
    assert workspace.get_input_bead_name('myinput') == 'new_bead'

    # Reset to original state
    shell.bead('input', 'update', 'myinput', 'original_bead')
    check.loaded('myinput', times.TS1)

    # Update to 'new_bead' WITH --no-kind: should match by name only
    # Should find the newest new_bead regardless of kind (TS3 with KIND:different)
    shell.bead('input', 'update', 'myinput', 'new_bead', '--no-kind')
    check.loaded('myinput', times.TS3)