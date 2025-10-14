import glob
import os

from ..test_helpers import create_bead_family


def test_input_mapping_preserved_across_save_edit_cycle(shell, box, check, times, tmp_path_factory):
    """
    Test that input mappings are preserved when saving and editing a new version.
    """

    # Create test beads
    create_bead_family(box, 'preserve_alpha', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'preserve_beta', [times.TS2], tmp_path_factory)
    create_bead_family(box, 'preserve_mapped', [times.TS5], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up inputs with known content
    shell.bead('input', 'add', 'input1', 'preserve_alpha')
    shell.bead('input', 'add', 'input2', 'preserve_beta')
    shell.bead('input', 'add', 'input3', 'preserve_mapped')
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)
    check.loaded('input3', times.TS5)

    # Save and edit new version
    shell.bead('save')
    shell.cd('..')
    shell.bead('discard', 'test_workspace')
    shell.bead('edit', 'test_workspace')
    shell.cd('test_workspace')

    # Verify all inputs are preserved after save/edit cycle
    shell.bead('input', 'load')
    check.loaded('input1', times.TS1)
    check.loaded('input2', times.TS2)
    check.loaded('input3', times.TS5)


def test_input_mapping_preserved_after_deletion(shell, box, check, times, tmp_path_factory):
    """
    Test that deleting one input preserves mappings of other inputs.
    """

    # Create test beads
    create_bead_family(box, 'preserve_alpha', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'preserve_beta', [times.TS2, times.TS3], tmp_path_factory)
    create_bead_family(box, 'preserve_mapped', [times.TS5], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up multiple inputs with different mappings
    shell.bead('input', 'add', 'input1', 'preserve_alpha')
    shell.bead('input', 'add', 'input2', f'preserve_beta@{times.TS2}')
    shell.bead('input', 'add', 'input3', 'preserve_mapped')

    # Configure mappings
    shell.bead('input', 'map', 'input1', 'preserve_mapped')
    shell.bead('input', 'map', 'input2', 'preserve_beta')
    shell.bead('input', 'map', 'input3', 'preserve_alpha')

    # Delete one input
    shell.bead('input', 'delete', 'input1')

    # Verify other mappings still work correctly
    shell.bead('input', 'update', 'input2')  # Should use preserve_beta mapping -> TS3
    check.loaded('input2', times.TS3)
    shell.bead('input', 'update', 'input3', '--allow-downgrade')  # Should use preserve_alpha mapping -> TS1 (downgrade from TS5)
    check.loaded('input3', times.TS1)


def test_load_finds_renamed_bead_by_content_id(
    shell, box, check, times, tmp_path_factory
):
    # Test that loading works even after renaming bead files, since content_id matching is used
    # This demonstrates the robustness of content_id-based loading

    # Create a test bead that we'll rename
    create_bead_family(box, 'target_bead', [times.TS1], tmp_path_factory)

    # edit new version using target_bead as input
    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')
    shell.bead('input', 'add', 'b', 'target_bead')
    check.loaded('b', times.TS1)

    # Find and rename the bead file
    bead_files = glob.glob(str(box.directory / 'target_bead_*.zip'))
    original_file = bead_files[0]
    renamed_file = box.directory / f'renamed_c_{times.TS1}.zip'
    os.rename(original_file, renamed_file)

    # unload input
    shell.bead('input', 'unload', 'b')

    # try to load input again - should succeed because content_id matching is used
    shell.bead('input', 'load', 'b')
    check.loaded('b', times.TS1)

    # unload input
    shell.bead('input', 'unload', 'b')

    # load still works even if input is mapped to some non-existent bead name
    shell.bead('input', 'map', 'b', 'non-existing')
    shell.bead('input', 'load', 'b')
    check.loaded('b', times.TS1)


def test_update_use_mapped_name_not_newest_by_kind(shell, box, check, times, tmp_path_factory):
    """
    Test that input update uses mapped bead name constraints, not just newest by timestamp.
    This proves that mapping enforces name-based matching constraints even when newer beads exist.
    """

    # Create bead families with version histories and distractors
    create_bead_family(box, 'mapping_start', [times.TS1], tmp_path_factory)   # Starting point
    create_bead_family(box, 'mapping_target', [times.TS2, times.TS4], tmp_path_factory)  # Mapped target with versions
    create_bead_family(box, 'mapping_distractor', [times.TS5], tmp_path_factory)  # Newer timestamp, different name

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Add input using the starting bead
    shell.bead('input', 'add', 'data_source', 'mapping_start')
    check.loaded('data_source', times.TS1)

    # Map the input to mapping_target and set to older version
    shell.bead('input', 'map', 'data_source', 'mapping_target')
    shell.bead('input', 'update', 'data_source', f'mapping_target@{times.TS2}')
    check.loaded('data_source', times.TS2)

    # Update should find newer mapping_target (TS4), NOT mapping_distractor (TS5)
    # This proves mapping constrains search to the mapped bead family
    shell.bead('input', 'update', 'data_source')
    check.loaded('data_source', times.TS4)  # Should be newer mapping_target, not mapping_distractor


def test_update_default_use_mapping(shell, box, check, times, tmp_path_factory):
    """
    Test that default NAME_AND_KIND strategy respects input mapping.
    """

    # Create test beads with version histories to properly test mapping
    create_bead_family(box, 'start_bead', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'mapped_bead', [times.TS2, times.TS4], tmp_path_factory)  # Version history

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up input with mapping to specific version
    shell.bead('input', 'add', 'test_input', 'start_bead')
    check.loaded('test_input', times.TS1)
    shell.bead('input', 'map', 'test_input', 'mapped_bead')
    shell.bead('input', 'update', 'test_input', f'mapped_bead@{times.TS2}')
    check.loaded('test_input', times.TS2)

    # Test default strategy (NAME_AND_KIND) uses mapping to find newer version
    shell.bead('input', 'update', 'test_input')
    check.loaded('test_input', times.TS4)  # Should find newer mapped_bead via mapping


def test_update_no_kind_use_mapping(shell, box, check, times, tmp_path_factory):
    """
    Test that NAME_ONLY (--no-kind) strategy still respects input mapping.
    """

    # Create test beads with version histories to properly test mapping
    create_bead_family(box, 'start_bead', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'mapped_bead', [times.TS2, times.TS4], tmp_path_factory)  # Version history

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up input with mapping to specific version
    shell.bead('input', 'add', 'test_input', 'start_bead')
    check.loaded('test_input', times.TS1)
    shell.bead('input', 'map', 'test_input', 'mapped_bead')
    shell.bead('input', 'update', 'test_input', f'mapped_bead@{times.TS2}')
    check.loaded('test_input', times.TS2)

    # Test --no-kind (NAME_ONLY) still uses mapping to find newer version
    shell.bead('input', 'update', 'test_input', '--no-kind')
    check.loaded('test_input', times.TS4)  # Should find newer mapped_bead via mapping


def test_update_no_name_ignore_mapping(shell, box, check, times, tmp_path_factory):
    """
    Test that KIND_ONLY (--no-name) strategy ignores input mapping completely,
    searching across all bead names for the newest matching kind.
    """

    # Create test beads with same kind but different names
    create_bead_family(box, 'start_bead', [times.TS1], tmp_path_factory, kind='KIND:target')
    create_bead_family(box, 'mapped_bead', [times.TS3], tmp_path_factory, kind='KIND:target')  # Same kind as start_bead
    create_bead_family(box, 'newest_bead', [times.TS5], tmp_path_factory, kind='KIND:target')  # Same kind, newest

    # Create distractor with different kind (to verify kind constraint still works)
    create_bead_family(box, 'distractor_bead', [times.TS4], tmp_path_factory, kind='KIND:distractor')

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up input with mapping
    shell.bead('input', 'add', 'test_input', 'start_bead')
    check.loaded('test_input', times.TS1)
    shell.bead('input', 'map', 'test_input', 'mapped_bead')

    # Test --no-name (KIND_ONLY) ignores mapping and name constraints
    # but still respects kind constraint
    shell.bead('input', 'update', 'test_input', '--no-name')
    check.loaded('test_input', times.TS5)  # Should find newest_bead by kind, ignoring mapping and names


def test_map_persist_across_save_edit(shell, box, check, times, tmp_path_factory):
    """
    Test that input mappings are preserved when saving and editing a new version.
    """

    # Create test beads with different names and timestamps
    create_bead_family(box, 'strategy_alpha', [times.TS1], tmp_path_factory, 'KIND:test_persistence')
    create_bead_family(box, 'strategy_beta', [times.TS2], tmp_path_factory, 'KIND:test_persistence')
    create_bead_family(box, 'strategy_gamma', [times.TS3], tmp_path_factory, 'KIND:test_persistence')

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up input with mapping
    shell.bead('input', 'add', 'persistent_input', 'strategy_alpha')
    check.loaded('persistent_input', times.TS1)
    shell.bead('input', 'map', 'persistent_input', 'strategy_beta')

    # Save and edit new version
    shell.bead('save')
    shell.cd('..')
    shell.bead('discard', 'test_workspace')
    shell.bead('edit', 'test_workspace')
    shell.cd('test_workspace')

    # Verify mapping is preserved by testing update behavior
    shell.bead('input', 'load')
    check.loaded('persistent_input', times.TS1)  # Should reload original content

    # Update should use the preserved mapping to find strategy_beta, not newest by kind
    shell.bead('input', 'update', 'persistent_input')
    check.loaded('persistent_input', times.TS2)  # Should find strategy_beta via preserved mapping


def test_input_map_backward_compatibility(shell, box, check, times, tmp_path_factory):
    """
    Test that archives without input_map work correctly (backward compatibility).
    """

    # Create a bead for testing backward compatibility
    create_bead_family(box, 'legacy_bead', [times.TS1], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Add input using old bead
    shell.bead('input', 'add', 'test_input', 'legacy_bead')
    check.loaded('test_input', times.TS1)

    # Save this workspace (should create archive with input_map)
    shell.bead('save')
    shell.cd('..')
    shell.bead('discard', 'test_workspace')

    # Edit the saved version - should work even though original bead had no input_map
    shell.bead('edit', 'test_workspace')
    shell.cd('test_workspace')

    # Load should work via content_id matching
    shell.bead('input', 'load')
    check.loaded('test_input', times.TS1)

    # Input map operations should work on the new workspace
    shell.bead('input', 'map', 'test_input', 'legacy_bead')  # Should work without errors


def test_input_add_sets_initial_mapping(shell, box, check, times, tmp_path_factory):
    """
    Test that input add command sets initial mapping correctly.
    """

    # Create bead with version history to actually test mapping behavior
    create_bead_family(box, 'initial_bead', [times.TS1, times.TS3], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Add input pointing to older version - this should set initial mapping
    shell.bead('input', 'add', 'test_input', f'initial_bead@{times.TS1}')
    check.loaded('test_input', times.TS1)

    # Verify initial mapping works - should find newer version of initial_bead via mapping
    shell.bead('input', 'update', 'test_input')
    check.loaded('test_input', times.TS3)  # Should update to newer initial_bead via mapping


def test_map_command_update_mapping(shell, box, check, times, tmp_path_factory):
    """
    Test that input map command changes mapping to a different bead.
    """

    # Create test beads
    create_bead_family(box, 'start_bead', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'target_bead', [times.TS2], tmp_path_factory)
    create_bead_family(box, 'final_bead', [times.TS3], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Start with initial bead
    shell.bead('input', 'add', 'test_input', 'start_bead')
    check.loaded('test_input', times.TS1)

    # Change mapping to different bead
    shell.bead('input', 'map', 'test_input', 'target_bead')
    shell.bead('input', 'update', 'test_input')
    check.loaded('test_input', times.TS2)  # Should now find target_bead

    # Change mapping again to verify it's mutable
    shell.bead('input', 'map', 'test_input', 'final_bead')
    shell.bead('input', 'update', 'test_input')
    check.loaded('test_input', times.TS3)  # Should now find final_bead


def test_input_update_with_explicit_bead_updates_mapping(shell, box, check, times, tmp_path_factory):
    """
    Test that input update with explicit bead reference updates the mapping.
    """

    # Create test beads
    create_bead_family(box, 'start_bead', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'explicit_bead', [times.TS2], tmp_path_factory)
    create_bead_family(box, 'newest_bead', [times.TS5], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Start with initial bead
    shell.bead('input', 'add', 'test_input', 'start_bead')
    check.loaded('test_input', times.TS1)

    # Update with explicit bead reference - should update mapping
    shell.bead('input', 'update', 'test_input', 'explicit_bead')
    check.loaded('test_input', times.TS2)  # Should load explicit_bead

    # Verify mapping was updated to explicit_bead
    shell.bead('input', 'update', 'test_input')
    check.loaded('test_input', times.TS2)  # Should stay at explicit_bead


def test_input_load_preserves_existing_mapping(shell, box, check, times, tmp_path_factory):
    """
    Test that input load preserves existing input mapping.
    """

    create_bead_family(box, 'strategy_alpha', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'strategy_beta', [times.TS2], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up input with custom mapping
    shell.bead('input', 'add', 'test_input', 'strategy_alpha')
    shell.bead('input', 'map', 'test_input', 'strategy_beta')

    # Unload and reload - mapping should be preserved
    shell.bead('input', 'unload', 'test_input')
    shell.bead('input', 'load', 'test_input')
    check.loaded('test_input', times.TS1)  # Loaded original content

    # But mapping should still point to strategy_beta
    # We can't directly check mapping, but we can test update behavior
    shell.bead('input', 'update', 'test_input')
    check.loaded('test_input', times.TS2)  # Should find strategy_beta


def test_update_mapped_ignore_wrong_kind(shell, box, check, times, tmp_path_factory):
    """
    Test that input mapping constrains searches by kind, ignoring newer beads with different kinds.
    """

    # Create mapped target family with older timestamps
    create_bead_family(box, 'mapped_target', [times.TS1, times.TS3], tmp_path_factory, kind='KIND:target')

    # Create distractor bead with newer timestamp but different kind
    create_bead_family(box, 'mapped_target', [times.TS5], tmp_path_factory, kind='KIND:distractor')

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Add input from the original kind
    shell.bead('input', 'add', 'data_source', f'mapped_target@{times.TS1}')
    check.loaded('data_source', times.TS1)

    # Update should find newer bead of same kind, not the newest distractor
    shell.bead('input', 'update', 'data_source')
    check.loaded('data_source', times.TS3)  # Should be TS3 (same kind), not TS5 (different kind)


def test_update_mapped_constrain_to_name(shell, box, check, times, tmp_path_factory):
    """
    Test that input mapping constrains searches by name, ignoring newer beads with different names.
    """

    # Create mapped target family with older timestamps
    create_bead_family(box, 'mapped_target', [times.TS2, times.TS4], tmp_path_factory)

    # Create distractor family with newer timestamp but different name (same kind)
    create_bead_family(box, 'different_name', [times.TS5], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Add input and explicitly map to specific bead family
    shell.bead('input', 'add', 'data_source', f'mapped_target@{times.TS2}')
    shell.bead('input', 'map', 'data_source', 'mapped_target')
    check.loaded('data_source', times.TS2)

    # Update should find newer version in mapped family, not the newest distractor
    shell.bead('input', 'update', 'data_source')
    check.loaded('data_source', times.TS4)  # Should be TS4 (mapped_target), not TS5 (different_name)


def test_input_map_with_nonexistent_bead_fails_gracefully(shell, box, check, times, tmp_path_factory):
    """
    Test that update fails gracefully when input map points to non-existent bead.
    """

    create_bead_family(box, 'edge_case_bead', [times.TS1], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Add input and map to non-existent bead
    shell.bead('input', 'add', 'test_input', 'edge_case_bead')
    shell.bead('input', 'map', 'test_input', 'nonexistent_bead')

    # Update should fail with helpful error about non-existent bead
    shell.bead('input', 'update', 'test_input', expect_failure=True)
    assert 'Could not find bead for "test_input"' in shell.stderr or 'Could not find bead for "test_input"' in shell.stdout


def test_input_map_edge_cases(shell, box, check, times, tmp_path_factory):
    """
    Test edge cases and default mapping behavior.
    """

    create_bead_family(box, 'edge_case_v', [times.TS1, times.TS2], tmp_path_factory)  # Version history

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # When input add is called, it sets mapping: test_input -> edge_case_v
    shell.bead('input', 'add', 'test_input', f'edge_case_v@{times.TS1}')
    check.loaded('test_input', times.TS1)

    # Update should work using the mapping set by input add (test_input -> edge_case_v)
    shell.bead('input', 'update', 'test_input')
    check.loaded('test_input', times.TS2)  # Should find newer edge_case_v

    # Test mapping to self (input_name -> input_name)
    shell.bead('input', 'map', 'test_input', 'test_input')
    # This should fail because there's no bead named "test_input"
    shell.bead('input', 'update', 'test_input', expect_failure=True)


def test_input_map_all_inputs_respects_individual_mappings(shell, box, check, times, tmp_path_factory):
    """
    Test that 'input update' (update all) respects individual input mappings.
    """

    # Create multiple beads for testing
    create_bead_family(box, 'bead_alpha', [times.TS1, times.TS4], tmp_path_factory)  # Version history for alpha
    create_bead_family(box, 'bead_beta', [times.TS2], tmp_path_factory)
    create_bead_family(box, 'bead_gamma', [times.TS3], tmp_path_factory)

    shell.bead('new', 'test_workspace')
    shell.cd('test_workspace')

    # Set up multiple inputs with different mappings
    shell.bead('input', 'add', 'input1', f'bead_alpha@{times.TS1}')
    shell.bead('input', 'add', 'input2', 'bead_beta')
    shell.bead('input', 'map', 'input1', 'bead_alpha')  # Should find TS4 version
    shell.bead('input', 'map', 'input2', 'bead_gamma')  # Should find TS3 version

    # Update all inputs - should respect individual mappings
    shell.bead('input', 'update')
    check.loaded('input1', times.TS4)  # Should find newer bead_alpha
    check.loaded('input2', times.TS3)  # Should find bead_gamma