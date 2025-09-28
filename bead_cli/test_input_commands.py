import os

from bead.tech.fs import rmtree

from bead.workspace import Workspace


# Test Quality Guidelines:
# - Always verify actual file content changes, not just stdout messages
# - Use README files consistently rather than inventing new files
# - Ensure original bead files aren't mixed with new bead output files
# - Content verification proves updates actually worked, not just that commands succeeded


def test_basic_usage(robot, bead_with_history, check, times):
    # nextbead with input1 as databead1
    robot.cli('new', 'nextbead')
    robot.cd('nextbead')
    # add version TS2
    robot.cli('input', 'add', 'input1', 'bead_with_history', '--time', times.TS2)
    check.loaded('input1', times.TS2)
    robot.cli('save')
    robot.cd('..')
    robot.cli('discard', 'nextbead')

    robot.cli('edit', 'nextbead')
    robot.cd('nextbead')
    assert not os.path.exists(robot.cwd / 'input/input1')

    robot.cli('input', 'load')
    assert os.path.exists(robot.cwd / 'input/input1')

    robot.cli('input', 'add', 'input2', 'bead_with_history')
    assert os.path.exists(robot.cwd / 'input/input2')

    robot.cli('input', 'delete', 'input1')
    assert not os.path.exists(robot.cwd / 'input/input1')

    # no-op load do not crash
    robot.cli('input', 'load')

    robot.cli('status')


def test_update_unloaded_input_w_another_bead(robot, bead_with_inputs, bead_a, bead_b, check):
    robot.cli('edit', bead_with_inputs)
    robot.cd(bead_with_inputs)

    robot.cli('status')
    assert bead_b in robot.stdout

    assert not Workspace(robot.cwd).is_loaded('input_b')

    robot.cli('input', 'update', 'input_b', bead_a)
    check.loaded('input_b', bead_a)

    robot.cli('status')
    assert bead_b not in robot.stdout


def test_load_on_workspace_without_input_gives_feedback(robot, bead_a):
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'load')

    assert 'WARNING' in robot.stderr
    assert 'No inputs defined to load.' in robot.stderr


def test_load_with_missing_bead_gives_warning(robot, bead_with_inputs, bead_a):
    robot.cli('edit', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.reset()
    robot.cli('input', 'load')
    assert 'WARNING' in robot.stderr
    assert 'input_a' in robot.stderr
    assert 'input_b' in robot.stderr


def test_load_only_one_input(robot, bead_with_inputs, bead_a, check):
    robot.cli('edit', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('input', 'load', 'input_a')
    check.loaded('input_a', bead_a)
    with robot.environment:
        assert not Workspace('.').is_loaded('input_b')


def test_deleted_box_does_not_stop_load(robot, bead_with_inputs, tmp_path_factory):
    deleted_box = tmp_path_factory.mktemp("deleted_box")
    robot.cli('box', 'add', 'missing', deleted_box)
    rmtree(deleted_box)
    robot.cli('edit', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('input', 'load')


def test_add_with_unrecognized_bead_name_exits_with_error(robot, bead_a):
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'add', 'x', 'non-existing-bead', expect_failure=True)
    assert 'ERROR' in robot.stderr
    assert 'non-existing-bead' in robot.stderr


def test_add_with_path_separator_in_name_is_error(robot, bead_a, bead_b):
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'add', 'name/with/path/separator', bead_b, expect_failure=True)
    assert 'ERROR' in robot.stderr
    assert 'name/with/path/separator' in robot.stderr

    robot.cli('status')
    assert bead_b not in robot.stdout
    assert [] == list(robot.ls('input'))


def test_add_with_hacked_bead_is_refused(robot, hacked_bead, bead_a):
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'add', 'hack', hacked_bead)
    assert not Workspace(robot.cwd).has_input('hack')
    assert 'WARNING' in robot.stderr


def test_update_with_hacked_bead_is_refused(robot, hacked_bead, bead_a, check):
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'add', 'intelligence', bead_a)
    robot.cli('input', 'update', 'intelligence', hacked_bead)
    check.loaded('intelligence', bead_a)
    assert 'WARNING' in robot.stderr


def test_update_to_next_version(robot, bead_with_history, check, times):
    robot.cli('new', 'test-workspace')
    robot.cd('test-workspace')
    # add version TS1
    robot.cli('input', 'add', 'input1', 'bead_with_history', '--time', times.TS1)
    check.loaded('input1', times.TS1)

    robot.cli('input', 'update', 'input1', '--next', '--no-name')
    check.loaded('input1', times.TS2)

    robot.cli('input', 'update', 'input1', '-N', '--no-name')
    check.loaded('input1', times.TS3)


def test_update_to_previous_version(robot, bead_with_history, check, times):
    robot.cli('new', 'test-workspace')
    robot.cd('test-workspace')
    # add version TS1
    robot.cli('input', 'add', 'input1', 'bead_with_history', '--time', times.TS4)
    check.loaded('input1', times.TS4)

    robot.cli('input', 'update', 'input1', '--prev', '--no-name')
    check.loaded('input1', times.TS3)

    robot.cli('input', 'update', 'input1', '-P', '--no-name')
    check.loaded('input1', times.TS2)


def test_update_up_to_date_inputs_is_noop(robot, bead_a, bead_b):
    robot.cli('new', 'test-workspace')
    robot.cd('test-workspace')
    robot.cli('input', 'add', bead_a)
    robot.cli('input', 'add', bead_b)

    def files_with_times():
        basepath = robot.cwd
        for dirpath, dirs, files in os.walk(basepath):
            for file in files:
                filename = os.path.join(dirpath, file)
                yield filename, os.path.getctime(filename)

    orig_files = sorted(files_with_times())
    robot.cli('input', 'update')
    after_update_files = sorted(files_with_times())
    assert orig_files == after_update_files

    assert f'Skipping update of {bead_a}:' in robot.stdout
    assert f'Skipping update of {bead_b}:' in robot.stdout


def test_update_with_same_bead_is_noop(robot, bead_a):
    robot.cli('new', 'test-workspace')
    robot.cd('test-workspace')
    robot.cli('input', 'add', bead_a)

    def files_with_times():
        basepath = robot.cwd
        for dirpath, dirs, files in os.walk(basepath):
            for file in files:
                filename = os.path.join(dirpath, file)
                yield filename, os.path.getctime(filename)

    orig_files = sorted(files_with_times())
    robot.cli('input', 'update', bead_a)
    after_update_files = sorted(files_with_times())
    assert orig_files == after_update_files

    assert f'Skipping update of {bead_a}:' in robot.stdout


def test_unload_all(robot, bead_with_inputs):
    robot.cli('edit', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('input', 'load')
    assert os.path.exists(robot.cwd / 'input/input_a')
    assert os.path.exists(robot.cwd / 'input/input_b')

    robot.cli('input', 'unload')
    assert not os.path.exists(robot.cwd / 'input/input_a')
    assert not os.path.exists(robot.cwd / 'input/input_b')


def test_unload(robot, bead_with_inputs):
    robot.cli('edit', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('input', 'load')
    assert os.path.exists(robot.cwd / 'input/input_a')
    assert os.path.exists(robot.cwd / 'input/input_b')

    robot.cli('input', 'unload', 'input_a')
    assert not os.path.exists(robot.cwd / 'input/input_a')
    assert os.path.exists(robot.cwd / 'input/input_b')

    robot.cli('input', 'unload', 'input_b')
    assert not os.path.exists(robot.cwd / 'input/input_a')
    assert not os.path.exists(robot.cwd / 'input/input_b')

    robot.cli('input', 'unload', 'input_a')
    assert not os.path.exists(robot.cwd / 'input/input_a')
    assert not os.path.exists(robot.cwd / 'input/input_b')


def test_delete_nonexisting_input(robot, bead_a):
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'delete', 'nonexisting', expect_failure=True)
    assert 'ERROR' in robot.stderr
    assert 'does not exist' in robot.stderr


def test_update_default_name_and_kind_matching(robot, bead_a):
    """Test default behavior: matches by both input name and kind."""
    # Create workspace with bead_a as input
    robot.cli('new', 'consumer')
    robot.cd('consumer')
    robot.cli('input', 'add', 'bead_a', bead_a)
    robot.cd('..')

    # Create newer version of bead_a (same name, same kind)
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    (robot.cwd / 'output' / 'README').write_text('updated content')
    robot.cli('save')
    robot.cd('..')

    # Go back to consumer and update
    robot.cd('consumer')
    robot.cli('input', 'update', 'bead_a')
    assert 'Loading new data' in robot.stdout
    # Verify the content was actually updated
    assert (robot.cwd / 'input' / 'bead_a' / 'README').read_text() == 'updated content'


def test_update_explicit_bead_bypasses_matching_constraints(robot, bead_a):
    """Test that explicit bead references bypass matching constraints."""
    # Create workspace with bead_a as input
    robot.cli('new', 'consumer')
    robot.cd('consumer')
    robot.cli('input', 'add', 'bead_a', bead_a)
    robot.cd('..')

    # Create a completely different bead with different name and kind
    robot.cli('new', 'different_bead')
    robot.cd('different_bead')
    (robot.cwd / 'output' / 'README').write_text('completely different content')
    robot.cli('save')
    robot.cd('..')

    # Go back to consumer and update with explicit bead reference
    robot.cd('consumer')
    robot.cli('input', 'update', 'bead_a', 'different_bead')
    assert 'Loading new data' in robot.stdout
    # Verify the content was actually updated
    assert (robot.cwd / 'input' / 'bead_a' / 'README').read_text() == 'completely different content'


def test_strict_matching_shows_no_update_when_only_incompatible_beads_exist(robot, bead_a):
    """Test that strict matching shows no update when only incompatible beads exist."""
    # Create workspace with input named 'bead_a'
    robot.cli('new', 'consumer')
    robot.cd('consumer')
    robot.cli('input', 'add', 'bead_a', bead_a)

    # Store original content for verification
    original_content = (robot.cwd / 'input' / 'bead_a' / 'README').read_text()
    robot.cd('..')

    # Create a different bead also named 'bead_a' but with different kind
    robot.cli('new', 'bead_a')
    robot.cd('bead_a')
    (robot.cwd / 'output' / 'different_kind.txt').write_text('different kind content')
    robot.cli('save')
    robot.cd('..')

    # Go back to consumer and try to update - should succeed but show "no update" message
    robot.cd('consumer')
    robot.cli('input', 'update', 'bead_a')
    # Should show that it's already at the requested version (no compatible update found)
    assert 'already at requested version' in robot.stdout

    # Verify content remains unchanged
    current_content = (robot.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content, "Content should remain unchanged when no compatible update exists"


def test_strict_matching_blocks_wrong_name_but_no_name_allows_it(robot, bead_a):
    """Test that strict matching blocks wrong name but --no-name allows it."""
    # Create workspace with input named bead_a (consistent with other tests)
    robot.cli('new', 'consumer')
    robot.cd('consumer')
    robot.cli('input', 'add', 'bead_a', bead_a)

    # Store original content
    original_content = (robot.cwd / 'input' / 'bead_a' / 'README').read_text()
    robot.cd('..')

    # Create newer bead with same kind as bead_a but different name
    robot.cli('edit', bead_a)
    robot.cd(bead_a)
    (robot.cwd / 'output' / 'README').write_text('newer content')
    robot.cd('..')
    # Rename the directory to different name before saving
    old_path = robot.cwd / bead_a
    new_path = robot.cwd / 'renamed_bead'
    old_path.rename(new_path)
    robot.cd('renamed_bead')
    robot.cli('save')
    robot.cd('..')

    # Try to update with strict matching - finds the original bead_a (no newer version with same name)
    robot.cd('consumer')
    robot.cli('input', 'update', 'bead_a')
    assert 'already at requested version' in robot.stdout

    # Verify content remains unchanged (renamed_bead was ignored due to name mismatch)
    current_content = (robot.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content

    # But --no-name should find the renamed_bead (newest by kind regardless of name)
    robot.cli('input', 'update', 'bead_a', '--no-name')
    assert 'Loading new data' in robot.stdout
    assert (robot.cwd / 'input' / 'bead_a' / 'README').read_text() == 'newer content'


def test_strict_matching_ignores_wrong_kind_but_no_kind_allows_it(robot, bead_a):
    """Test that strict matching ignores wrong kind but --no-kind allows it."""
    # Create workspace with input from bead_a (establishes the original kind)
    robot.cli('new', 'consumer')
    robot.cd('consumer')
    robot.cli('input', 'add', 'bead_a', bead_a)

    # Store original content
    original_content = (robot.cwd / 'input' / 'bead_a' / 'README').read_text()
    robot.cd('..')

    # Create newer bead with same name but different kind (using 'new' creates different kind)
    robot.cli('new', 'bead_a')  # Different kind UUID but same name
    robot.cd('bead_a')
    (robot.cwd / 'output' / 'README').write_text('newer content with different kind')
    robot.cli('save')
    robot.cd('..')

    # Try to update with strict matching - should find original bead and show no update needed
    robot.cd('consumer')
    robot.cli('input', 'update', 'bead_a')
    # Strict matching finds the original bead_a and sees it's already current
    assert 'already at requested version' in robot.stdout

    # Verify content remains unchanged (different kind bead was ignored)
    current_content = (robot.cwd / 'input' / 'bead_a' / 'README').read_text()
    assert current_content == original_content

    # But --no-kind should allow the update to the newer different-kind bead (matches by name only)
    robot.cli('input', 'update', 'bead_a', '--no-kind')
    assert 'Loading new data' in robot.stdout
    assert (robot.cwd / 'input' / 'bead_a' / 'README').read_text() == 'newer content with different kind'
