import os

from bead.tech.fs import rmtree

from bead.workspace import Workspace


# Test Quality Guidelines:
# - Always verify actual file content changes, not just stdout messages
# - Use README files consistently rather than inventing new files
# - Ensure original bead files aren't mixed with new bead output files
# - Content verification proves updates actually worked, not just that commands succeeded


def test_basic_usage(shell, bead_with_history, check, times):
    # nextbead with input1 as databead1
    shell.bead('new', 'nextbead')
    shell.cd('nextbead')
    # add version TS2
    shell.bead('input', 'add', 'input1', 'bead_with_history', '--time', times.TS2)
    check.loaded('input1', times.TS2)
    shell.bead('save')
    shell.cd('..')
    shell.bead('discard', 'nextbead')

    shell.bead('edit', 'nextbead')
    shell.cd('nextbead')
    assert not os.path.exists(shell.cwd / 'input/input1')

    shell.bead('input', 'load')
    assert os.path.exists(shell.cwd / 'input/input1')

    shell.bead('input', 'add', 'input2', 'bead_with_history')
    assert os.path.exists(shell.cwd / 'input/input2')

    shell.bead('input', 'delete', 'input1')
    assert not os.path.exists(shell.cwd / 'input/input1')

    # no-op load do not crash
    shell.bead('input', 'load')

    shell.bead('status')


def test_update_unloaded_input_w_another_bead(shell, bead_with_inputs, bead_a, bead_b, check):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)

    shell.bead('status')
    assert bead_b in shell.stdout

    assert not Workspace(shell.cwd).is_loaded('input_b')

    shell.bead('input', 'update', 'input_b', bead_a)
    check.loaded('input_b', bead_a)

    shell.bead('status')
    assert bead_b not in shell.stdout


def test_load_on_workspace_without_input_gives_feedback(shell, bead_a):
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    shell.bead('input', 'load')

    assert 'WARNING' in shell.stderr
    assert 'No inputs defined to load.' in shell.stderr


def test_load_with_missing_bead_gives_warning(shell, bead_with_inputs, bead_a):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.reset()
    shell.bead('input', 'load')
    assert 'WARNING' in shell.stderr
    assert 'input_a' in shell.stderr
    assert 'input_b' in shell.stderr


def test_load_only_one_input(shell, bead_with_inputs, bead_a, check):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.bead('input', 'load', 'input_a')
    check.loaded('input_a', bead_a)
    with shell.environment:
        assert not Workspace('.').is_loaded('input_b')


def test_deleted_box_does_not_stop_load(shell, bead_with_inputs, tmp_path_factory):
    deleted_box = tmp_path_factory.mktemp("deleted_box")
    shell.bead('box', 'add', 'missing', deleted_box)
    rmtree(deleted_box)
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.bead('input', 'load')


def test_add_with_unrecognized_bead_name_exits_with_error(shell, bead_a):
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    shell.bead('input', 'add', 'x', 'non-existing-bead', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'non-existing-bead' in shell.stderr


def test_add_with_path_separator_in_name_is_error(shell, bead_a, bead_b):
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    shell.bead('input', 'add', 'name/with/path/separator', bead_b, expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'name/with/path/separator' in shell.stderr

    shell.bead('status')
    assert bead_b not in shell.stdout
    assert [] == list(shell.ls('input'))


def test_add_with_hacked_bead_is_refused(shell, hacked_bead, bead_a):
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    shell.bead('input', 'add', 'hack', hacked_bead)
    assert not Workspace(shell.cwd).has_input('hack')
    assert 'WARNING' in shell.stderr


def test_update_with_hacked_bead_is_refused(shell, hacked_bead, bead_a, check):
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    shell.bead('input', 'add', 'intelligence', bead_a)
    shell.bead('input', 'update', 'intelligence', hacked_bead)
    check.loaded('intelligence', bead_a)
    assert 'WARNING' in shell.stderr


def test_update_to_next_version(shell, bead_with_history, check, times):
    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    # add version TS1
    shell.bead('input', 'add', 'input1', 'bead_with_history', '--time', times.TS1)
    check.loaded('input1', times.TS1)

    shell.bead('input', 'update', 'input1', '--next', '--no-name')
    check.loaded('input1', times.TS2)

    shell.bead('input', 'update', 'input1', '-N', '--no-name')
    check.loaded('input1', times.TS3)


def test_update_to_previous_version(shell, bead_with_history, check, times):
    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    # add version TS1
    shell.bead('input', 'add', 'input1', 'bead_with_history', '--time', times.TS4)
    check.loaded('input1', times.TS4)

    shell.bead('input', 'update', 'input1', '--prev', '--no-name')
    check.loaded('input1', times.TS3)

    shell.bead('input', 'update', 'input1', '-P', '--no-name')
    check.loaded('input1', times.TS2)


def test_update_up_to_date_inputs_is_noop(shell, bead_a, bead_b):
    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    shell.bead('input', 'add', bead_a)
    shell.bead('input', 'add', bead_b)

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

    assert f'Skipping update of {bead_a}:' in shell.stdout
    assert f'Skipping update of {bead_b}:' in shell.stdout


def test_update_with_same_bead_is_noop(shell, bead_a):
    shell.bead('new', 'test-workspace')
    shell.cd('test-workspace')
    shell.bead('input', 'add', bead_a)

    def files_with_times():
        basepath = shell.cwd
        for dirpath, dirs, files in os.walk(basepath):
            for file in files:
                filename = os.path.join(dirpath, file)
                yield filename, os.path.getctime(filename)

    orig_files = sorted(files_with_times())
    shell.bead('input', 'update', bead_a)
    after_update_files = sorted(files_with_times())
    assert orig_files == after_update_files

    assert f'Skipping update of {bead_a}:' in shell.stdout


def test_unload_all(shell, bead_with_inputs):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.bead('input', 'load')
    assert os.path.exists(shell.cwd / 'input/input_a')
    assert os.path.exists(shell.cwd / 'input/input_b')

    shell.bead('input', 'unload')
    assert not os.path.exists(shell.cwd / 'input/input_a')
    assert not os.path.exists(shell.cwd / 'input/input_b')


def test_unload(shell, bead_with_inputs):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
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


def test_delete_nonexisting_input(shell, bead_a):
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    shell.bead('input', 'delete', 'nonexisting', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'does not exist' in shell.stderr


def test_update_default_name_and_kind_matching(shell, bead_a):
    """Test default behavior: matches by both input name and kind."""
    # Create workspace with bead_a as input
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', bead_a)
    shell.cd('..')

    # Create newer version of bead_a (same name, same kind)
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    (shell.cwd / 'output' / 'README').write_text('updated content')
    shell.bead('save')
    shell.cd('..')

    # Go back to consumer and update
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a')
    assert 'Loading new data' in shell.stdout
    # Verify the content was actually updated
    assert (shell.cwd / 'input' / 'bead_a' / 'README').read_text() == 'updated content'


def test_update_explicit_bead_bypasses_matching_constraints(shell, bead_a):
    """Test that explicit bead references bypass matching constraints."""
    # Create workspace with bead_a as input
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', bead_a)
    shell.cd('..')

    # Create a completely different bead with different name and kind
    shell.bead('new', 'different_bead')
    shell.cd('different_bead')
    (shell.cwd / 'output' / 'README').write_text('completely different content')
    shell.bead('save')
    shell.cd('..')

    # Go back to consumer and update with explicit bead reference
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a', 'different_bead')
    assert 'Loading new data' in shell.stdout
    # Verify the content was actually updated
    assert (shell.cwd / 'input' / 'bead_a' / 'README').read_text() == 'completely different content'


def test_strict_matching_shows_no_update_when_only_incompatible_beads_exist(shell, bead_a):
    """Test that strict matching shows no update when only incompatible beads exist."""
    # Create workspace with input named 'bead_a'
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', bead_a)

    # Store original content for verification
    original_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    shell.cd('..')

    # Create a different bead also named 'bead_a' but with different kind
    shell.bead('new', 'bead_a')
    shell.cd('bead_a')
    (shell.cwd / 'output' / 'different_kind.txt').write_text('different kind content')
    shell.bead('save')
    shell.cd('..')

    # Go back to consumer and try to update - should succeed but show "no update" message
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a')
    # Should show that it's already at the requested version (no compatible update found)
    assert 'already at requested version' in shell.stdout

    # Verify content remains unchanged
    current_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content, "Content should remain unchanged when no compatible update exists"


def test_strict_matching_blocks_wrong_name_but_no_name_allows_it(shell, bead_a):
    """Test that strict matching blocks wrong name but --no-name allows it."""
    # Create workspace with input named bead_a (consistent with other tests)
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', bead_a)

    # Store original content
    original_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    shell.cd('..')

    # Create newer bead with same kind as bead_a but different name
    shell.bead('edit', bead_a)
    shell.cd(bead_a)
    (shell.cwd / 'output' / 'README').write_text('newer content')
    shell.cd('..')
    # Rename the directory to different name before saving
    old_path = shell.cwd / bead_a
    new_path = shell.cwd / 'renamed_bead'
    old_path.rename(new_path)
    shell.cd('renamed_bead')
    shell.bead('save')
    shell.cd('..')

    # Try to update with strict matching - finds the original bead_a (no newer version with same name)
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a')
    assert 'already at requested version' in shell.stdout

    # Verify content remains unchanged (renamed_bead was ignored due to name mismatch)
    current_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content

    # But --no-name should find the renamed_bead (newest by kind regardless of name)
    shell.bead('input', 'update', 'bead_a', '--no-name')
    assert 'Loading new data' in shell.stdout
    assert (shell.cwd / 'input' / 'bead_a' / 'README').read_text() == 'newer content'


def test_strict_matching_ignores_wrong_kind_but_no_kind_allows_it(shell, bead_a):
    """Test that strict matching ignores wrong kind but --no-kind allows it."""
    # Create workspace with input from bead_a (establishes the original kind)
    shell.bead('new', 'consumer')
    shell.cd('consumer')
    shell.bead('input', 'add', 'bead_a', bead_a)

    # Store original content
    original_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    shell.cd('..')

    # Create newer bead with same name but different kind (using 'new' creates different kind)
    shell.bead('new', 'bead_a')  # Different kind UUID but same name
    shell.cd('bead_a')
    (shell.cwd / 'output' / 'README').write_text('newer content with different kind')
    shell.bead('save')
    shell.cd('..')

    # Try to update with strict matching - should find original bead and show no update needed
    shell.cd('consumer')
    shell.bead('input', 'update', 'bead_a')
    # Strict matching finds the original bead_a and sees it's already current
    assert 'already at requested version' in shell.stdout

    # Verify content remains unchanged (different kind bead was ignored)
    current_content = (shell.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content

    # But --no-kind should allow the update to the newer different-kind bead (matches by name only)
    shell.bead('input', 'update', 'bead_a', '--no-kind')
    assert 'Loading new data' in shell.stdout
    assert (shell.cwd / 'input' / 'bead_a' / 'README').read_text() == 'newer content with different kind'
