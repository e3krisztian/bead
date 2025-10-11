import glob
import os

from bead.infra.fs import rmtree
from bead.workspace import Workspace
from ..test_helpers import create_bead_family, create_bead_with_inputs


# Test Quality Guidelines:
# - Always verify actual file content changes, not just stdout messages
# - Use README files consistently rather than inventing new files
# - Ensure original bead files aren't mixed with new bead output files
# - Content verification proves updates actually worked, not just that commands succeeded


def test_basic_input_add_load_delete(shell, box, check, times, tmp_path_factory):
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


def test_unload_selective(shell, box, check, times, tmp_path_factory):
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


def test_status_displays_input_information_correctly(shell, box, check, times, tmp_path_factory):
    """
    Test that status command displays basic input information including timestamps.
    """

    # Create beads with different names but same kind (default behavior)
    create_bead_family(box, 'status_data_early', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'status_data_late', [times.TS2], tmp_path_factory)

    # Add inputs using the status test beads
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'input1', 'status_data_early')
    shell.bead('input', 'add', 'input2', 'status_data_late')

    # Verify inputs are loaded with correct timestamps
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)

    shell.bead('status')
    # Just verify that status shows some meaningful bead information
    assert 'input1' in shell.stdout
    assert 'input2' in shell.stdout
    assert times.TS1 in shell.stdout
    assert times.TS2 in shell.stdout


def test_load_different_name_warn(shell, box, check, times, tmp_path_factory):
    """
    Test that load warns when a bead is found by content_id but under a different name.

    Scenario:
    1. Create bead "original_name"
    2. Create workspace with input from "original_name"
    3. Rename archive to "different_name" (same content_id)
    4. Load input -> should succeed but warn about name mismatch
    """
    # 1. Create bead "original_name"
    create_bead_family(box, 'original_name', [times.TS1], tmp_path_factory)

    # 2. Create workspace with input from "original_name"
    create_bead_with_inputs(
        shell, box, 'test_workspace',
        {'test_input': 'original_name'},
        times.TS2, tmp_path_factory
    )

    # Get the content_id and archive path
    shell.bead('edit', 'test_workspace')
    shell.cd('test_workspace')
    with shell.environment:
        ws = Workspace('.')
        input_spec = ws.get_input('test_input')
        original_content_id = input_spec.content_id

    # Unload the input so we can test loading it fresh
    shell.bead('input', 'unload', 'test_input')

    # 3. Simulate renaming: move/rename the archive file
    original_archives = glob.glob(str(box.directory / f'original_name_{times.TS1}.zip'))
    assert len(original_archives) == 1, "Should find exactly one archive"
    original_archive_path = original_archives[0]

    # Rename to different name (simulating a renamed bead with same content)
    new_archive_path = str(box.directory / f'different_name_{times.TS1}.zip')
    os.rename(original_archive_path, new_archive_path)

    # Use CLI to reindex the box (this will remove the old entry and add the new one)
    shell.bead('box', 'index', box.name)

    # Verify: should find by content_id under different name
    try:
        found_bead = box.search().by_content_id(original_content_id).first()
        assert found_bead.name == 'different_name', f"Should find bead under new name, got {found_bead.name}"
    except LookupError:
        raise AssertionError("Should be able to find bead by content_id")

    # 4. Try to load - should succeed but warn
    shell.bead('input', 'load', 'test_input')

    # Should succeed in loading
    with shell.environment:
        assert Workspace('.').is_loaded('test_input'), "Input should be loaded"

    # Should warn about name mismatch
    assert 'WARNING' in shell.stderr, "Should show warning"
    assert 'original_name' in shell.stderr, "Should mention expected name"
    assert 'different_name' in shell.stderr, "Should mention actual name"