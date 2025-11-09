from .test_helpers import create_bead_family, get_bead_archive, create_bead_with_inputs, split_status_by_inputs


def test_status(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'status_bead_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'status_bead_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    status_archive = create_bead_with_inputs(shell, box, 'status_with_inputs',
                                           {'input_a': 'status_bead_a', 'input_b': 'status_bead_b'},
                                           times.TS3, tmp_path_factory)

    shell.bead('edit', 'status_with_inputs')
    shell.cd('status_with_inputs')
    shell.bead('input', 'load', 'input_a')
    shell.bead('status')

    assert 'status_with_inputs' in shell.stdout
    assert 'status_bead_a' in shell.stdout

    assert status_archive.kind not in shell.stdout
    assert times.TS1 in shell.stdout
    assert status_archive.content_id not in shell.stdout


def test_verbose(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'verbose_bead_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'verbose_bead_b', [times.TS2], tmp_path_factory)

    # Get the input bead archives to access their content_ids
    bead_a_archive = get_bead_archive(box, 'verbose_bead_a', times.TS1)

    # Create bead with inputs
    verbose_archive = create_bead_with_inputs(shell, box, 'verbose_with_inputs',
                                            {'input_a': 'verbose_bead_a', 'input_b': 'verbose_bead_b'},
                                            times.TS3, tmp_path_factory)

    shell.bead('edit', 'verbose_with_inputs')
    shell.cd('verbose_with_inputs')
    shell.bead('status', '-v')

    assert 'verbose_with_inputs' in shell.stdout
    assert 'verbose_bead_a' in shell.stdout

    assert verbose_archive.kind in shell.stdout
    assert bead_a_archive.kind in shell.stdout
    assert bead_a_archive.freeze_time_iso in shell.stdout
    assert bead_a_archive.content_id in shell.stdout


def test_inputs_not_in_known_boxes(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'missing_bead_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'missing_bead_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    create_bead_with_inputs(shell, box, 'missing_with_inputs',
                            {'input_a': 'missing_bead_a', 'input_b': 'missing_bead_b'},
                            times.TS3, tmp_path_factory)

    shell.bead('edit', 'missing_with_inputs')
    shell.cd('missing_with_inputs')

    shell.reset()
    shell.bead('status')

    assert 'missing_with_inputs' in shell.stdout
    assert '**NO CANDIDATES**' in shell.stdout

    assert 'missing_with_inputs' in shell.stdout
    assert times.TS1 in shell.stdout


def test_verbose_inputs_not_in_known_boxes(shell, box, check, times, tmp_path_factory):
    # Create input beads
    create_bead_family(box, 'verbose_missing_bead_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'verbose_missing_bead_b', [times.TS2], tmp_path_factory)

    # Get the input bead archives to access their content_ids
    missing_a_archive = get_bead_archive(box, 'verbose_missing_bead_a', times.TS1)

    # Create bead with inputs
    verbose_missing_archive = create_bead_with_inputs(shell, box, 'verbose_missing_with_inputs',
                                                    {'input_a': 'verbose_missing_bead_a', 'input_b': 'verbose_missing_bead_b'},
                                                    times.TS3, tmp_path_factory)

    shell.bead('edit', 'verbose_missing_with_inputs')
    shell.cd('verbose_missing_with_inputs')
    shell.reset()
    shell.bead('status', '--verbose')

    assert 'verbose_missing_with_inputs' in shell.stdout
    assert '**NO CANDIDATES**' in shell.stdout

    assert verbose_missing_archive.kind in shell.stdout
    assert missing_a_archive.kind in shell.stdout
    assert missing_a_archive.freeze_time_iso in shell.stdout
    assert missing_a_archive.content_id in shell.stdout


def test_status_invalid_workspace_warns(shell):
    shell.bead('status')
    assert 'WARNING' in shell.stderr


def test_status_multiple_scenarios(shell, box, check, times, tmp_path_factory):
    """Test multiple input scenarios in single workspace using split_status_by_inputs."""
    # Create input beads
    create_bead_family(box, 'loaded_bead', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'not_loaded_bead', [times.TS2], tmp_path_factory)
    create_bead_family(box, 'update_bead', [times.TS3, times.TS4], tmp_path_factory)  # v1 and v2

    # Create workspace manually to control which versions are used
    shell.bead('new', 'multi_scenario_ws')
    shell.cd('multi_scenario_ws')
    shell.write_file('README', 'test')
    shell.write_file('output/README', 'test')

    # Add inputs - for update_input, explicitly add v1 (TS3)
    # Inputs are automatically loaded when added
    shell.bead('input', 'add', 'loaded_input', 'loaded_bead')
    shell.bead('input', 'add', 'not_loaded_input', 'not_loaded_bead')
    shell.bead('input', 'add', 'update_input', f'update_bead@{times.TS3}')

    # Unload the one we want not loaded
    shell.bead('input', 'unload', 'not_loaded_input')

    # Get status and split by inputs
    shell.bead('status')
    sections = split_status_by_inputs(shell.stdout)

    # Assert loaded_input section
    assert 'loaded_input' in sections
    loaded_section = sections['loaded_input']
    assert 'From:        box:loaded_bead@' in loaded_section
    assert 'Status:' not in loaded_section  # Non-verbose, loaded = no Status line

    # Assert not_loaded_input section
    assert 'not_loaded_input' in sections
    not_loaded_section = sections['not_loaded_input']
    assert 'Status:      **NOT LOADED**' in not_loaded_section
    assert 'From:        box:not_loaded_bead@' in not_loaded_section

    # Assert update_input section (has newer version available - v1 loaded, v2 available)
    assert 'update_input' in sections
    update_section = sections['update_input']
    assert '**UPDATE AVAILABLE**' in update_section
    assert f'update_bead@{times.TS3}' in update_section  # Has v1


def test_status_single_box_format(shell, box, check, times, tmp_path_factory):
    """Test From: format for single box, loaded input."""
    create_bead_family(box, 'single_box_bead', [times.TS1], tmp_path_factory)
    create_bead_with_inputs(shell, box, 'single_box_ws',
                            {'test_input': 'single_box_bead'},
                            times.TS2, tmp_path_factory)

    shell.bead('edit', 'single_box_ws')
    shell.cd('single_box_ws')
    shell.bead('input', 'load', 'test_input')
    shell.bead('status')

    assert 'From:        box:single_box_bead@' in shell.stdout
    assert 'Status:' not in shell.stdout  # Non-verbose, loaded

    # Verbose mode should show Status
    shell.bead('status', '-v')
    assert 'Status:      loaded' in shell.stdout


def test_status_multiple_boxes(shell, box, check, times, tmp_path_factory):
    """Test Box: comma-separated format for multiple boxes."""
    import os
    import shutil

    # Create input bead
    create_bead_family(box, 'multi_box_bead', [times.TS1], tmp_path_factory)

    # Add second box and copy bead to it
    box2_dir = shell.cwd / 'box2'
    os.makedirs(box2_dir)
    shell.bead('box', 'add', 'box2', box2_dir)

    # Get the bead archive and copy to box2
    bead_archive = get_bead_archive(box, 'multi_box_bead', times.TS1)
    filename = bead_archive.zipfile.filename
    assert filename is not None
    shutil.copy(filename, box2_dir)

    # Reindex box2
    shell.bead('box', 'reindex', 'box2')

    # Create workspace with input
    create_bead_with_inputs(shell, box, 'multi_box_ws',
                            {'test_input': 'multi_box_bead'},
                            times.TS2, tmp_path_factory)

    shell.bead('edit', 'multi_box_ws')
    shell.cd('multi_box_ws')
    shell.bead('input', 'load', 'test_input')
    shell.bead('status')

    assert 'Bead:        multi_box_bead@' in shell.stdout
    # Box order might vary, check both possibilities
    assert ('Box:         box, box2' in shell.stdout or 'Box:         box2, box' in shell.stdout)


def test_status_update_verbose(shell, box, check, times, tmp_path_factory):
    """Test Update: detail line in verbose mode."""
    # Create v1 and v2
    create_bead_family(box, 'versioned_bead', [times.TS1, times.TS2], tmp_path_factory)

    # Create workspace with v1 explicitly
    shell.bead('new', 'update_ws')
    shell.cd('update_ws')
    shell.write_file('README', 'test')
    shell.write_file('output/README', 'test')
    shell.bead('input', 'add', 'test_input', f'versioned_bead@{times.TS1}')  # Add v1 explicitly
    shell.bead('input', 'load', 'test_input')

    # Non-verbose: should show UPDATE AVAILABLE but no Update line
    shell.bead('status')
    assert '**UPDATE AVAILABLE**' in shell.stdout
    assert 'Update:' not in shell.stdout

    # Verbose: should show Update line with details
    shell.bead('status', '-v')
    assert '**UPDATE AVAILABLE**' in shell.stdout
    assert 'Update:      box:versioned_bead@' in shell.stdout
    assert times.TS2 in shell.stdout


def test_status_match_strategy(shell, box, check, times, tmp_path_factory):
    """Test --no-kind/--no-name flags for update checking."""
    # Create two beads: same kind, different names
    create_bead_family(box, 'original_name', [times.TS1], tmp_path_factory, kind='SAME_KIND')
    create_bead_family(box, 'renamed_bead', [times.TS2], tmp_path_factory, kind='SAME_KIND')

    # Create workspace with original_name
    create_bead_with_inputs(shell, box, 'match_ws',
                            {'test_input': 'original_name'},
                            times.TS3, tmp_path_factory)

    shell.bead('edit', 'match_ws')
    shell.cd('match_ws')

    # Default (NAME_AND_KIND): no update since names differ
    shell.bead('status')
    assert '**UPDATE AVAILABLE**' not in shell.stdout

    # With --no-name (KIND_ONLY): should find update
    shell.bead('status', '--no-name')
    assert '**UPDATE AVAILABLE**' in shell.stdout


