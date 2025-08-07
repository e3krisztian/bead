import os
import shutil
from typing import Dict

from bead.ziparchive import ZipArchive


def test_status_displays_input_information_correctly(robot, bead_a, bead_with_history, box, check, times):
    """
    Test that status command displays basic input information including timestamps.
    """
    cd = robot.cd
    cli = robot.cli
    
    # Create copies of the original bead with different names
    _copy(box, bead_with_history, times.TS1, 'copied_bead1')
    _copy(box, bead_with_history, times.TS2, 'copied_bead2')

    # Add inputs using the copied bead names
    cli('develop', bead_a)
    cd(bead_a)
    cli('input', 'add', 'input1', 'copied_bead1')
    cli('input', 'add', 'input2', 'copied_bead2')
    
    # Verify inputs are loaded with correct timestamps
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)

    cli('status')
    # Just verify that status shows some meaningful bead information
    assert 'input1' in robot.stdout
    assert 'input2' in robot.stdout
    assert times.TS1 in robot.stdout
    assert times.TS2 in robot.stdout


def test_update_finds_newest_by_kind_not_name(robot, bead_a, bead_with_history, box, check, times):
    """
    Test that update command finds the newest bead by kind, ignoring bead names.
    This demonstrates the shift from name-based to kind-based updates.
    """
    cd = robot.cd
    cli = robot.cli
    
    # Create older copies with different names
    _copy(box, bead_with_history, times.TS1, 'old_copy1')
    _copy(box, bead_with_history, times.TS2, 'old_copy2')

    # Set up workspace with inputs pointing to older copies
    cli('develop', bead_a)
    cd(bead_a)
    cli('input', 'add', 'input1', 'old_copy1')
    cli('input', 'add', 'input2', 'old_copy2')
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)

    # Update should find the newest bead of the same kind (times.TS5)
    # regardless of the copied bead names
    cli('input', 'update')
    check.loaded('input1', times.TS5)  # updated to newest of kind
    check.loaded('input2', times.TS5)  # updated to newest of kind


def test_explicit_bead_update_with_new_reference(robot, bead_a, bead_with_history, box, check, times):
    """
    Test updating a specific input with an explicit bead reference.
    """
    cd = robot.cd
    cli = robot.cli
    
    # Set up workspace with one input
    _copy(box, bead_with_history, times.TS1, 'initial_bead')
    cli('develop', bead_a)
    cd(bead_a)
    cli('input', 'add', 'test_input', 'initial_bead')
    check.loaded('test_input', times.TS1)

    # Create a new copy to update to
    _copy(box, bead_with_history, times.TS3, 'newer_bead')
    
    # Update specific input with explicit bead reference
    cli('input', 'update', 'test_input', 'newer_bead')
    check.loaded('test_input', times.TS3)
    
    # Update without explicit reference should find newest by kind
    cli('input', 'update', 'test_input')
    check.loaded('test_input', times.TS5)  # finds newest of the kind


def test_save_and_develop_preserves_content_id_references(robot, bead_a, bead_with_history, box, check, times):
    """
    Test that save/develop cycle preserves content_id-based input references.
    """
    cd = robot.cd
    cli = robot.cli
    
    # Set up workspace with inputs
    _copy(box, bead_with_history, times.TS1, 'test_bead1')
    _copy(box, bead_with_history, times.TS2, 'test_bead2')
    
    cli('develop', bead_a)
    cd(bead_a)
    cli('input', 'add', 'input1', 'test_bead1')
    cli('input', 'add', 'input2', 'test_bead2')
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)

    # Save and develop new version
    cli('save')
    cd('..')
    cli('zap', bead_a)
    cli('develop', bead_a)
    cd(bead_a)

    cli('input', 'load')
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)

    # Update should still work by kind, finding newest versions
    cli('input', 'update')
    check.loaded('input1', times.TS5)  # finds newest of the kind
    check.loaded('input2', times.TS5)  # finds newest of the kind


def test_load_finds_renamed_bead_by_content_id(
    robot, bead_a, bead_b, box, beads: Dict[str, ZipArchive], check, times
):
    # Test that loading works even after renaming bead files, since content_id matching is used
    # This demonstrates the robustness of content_id-based loading
    cd = robot.cd
    cli = robot.cli

    # develop new version of A using B as its input
    cli('develop', bead_a)
    cd(bead_a)
    cli('input', 'add', 'b', bead_b)
    check.loaded('b', bead_b)

    # rename B to C
    os.rename(beads[bead_b].archive_filename, box.directory / f'c_{times.TS1}.zip')

    # unload input
    cli('input', 'unload', 'b')

    # try to load input again - should succeed because content_id matching is used
    cli('input', 'load', 'b')
    check.loaded('b', bead_b)


def _copy(box, bead_name, bead_freeze_time, new_name):
    """
    Copy a bead to a new name within box.
    """
    # FIXME: this test helper uses private to box implementation information
    # namely how the current box implementation stores archives in zip files
    source = box.directory / f'{bead_name}_{bead_freeze_time}.zip'
    destination = box.directory / f'{new_name}_{bead_freeze_time}.zip'
    shutil.copy(source, destination)
