from .test_helpers import create_bead_family, get_bead_archive, create_bead_with_inputs


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
    assert 'no candidates :(' in shell.stdout

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
    assert 'no candidates :(' in shell.stdout

    assert verbose_missing_archive.kind in shell.stdout
    assert missing_a_archive.kind in shell.stdout
    assert missing_a_archive.freeze_time_iso in shell.stdout
    assert missing_a_archive.content_id in shell.stdout


def test_invalid_workspace(shell):
    shell.bead('status')
    assert 'WARNING' in shell.stderr


