def test_status(shell, beads, bead_with_inputs, bead_a):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.bead('input', 'load', 'input_a')
    shell.bead('status')

    assert bead_with_inputs in shell.stdout
    assert bead_a in shell.stdout

    bead_a = beads[bead_a]
    bead_with_inputs = beads[bead_with_inputs]
    assert bead_with_inputs.kind not in shell.stdout
    assert bead_a.kind not in shell.stdout
    assert bead_a.freeze_time_str in shell.stdout
    assert bead_a.content_id not in shell.stdout


def test_verbose(shell, beads, bead_with_inputs, bead_a):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.bead('status', '-v')

    assert bead_with_inputs in shell.stdout
    assert bead_a in shell.stdout

    bead_a = beads[bead_a]
    bead_with_inputs = beads[bead_with_inputs]
    assert bead_with_inputs.kind in shell.stdout
    assert bead_a.kind in shell.stdout
    assert bead_a.freeze_time_str in shell.stdout
    assert bead_a.content_id in shell.stdout


def test_inputs_not_in_known_boxes(shell, beads, bead_with_inputs, bead_a):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)

    shell.reset()
    shell.bead('status')

    assert bead_with_inputs in shell.stdout
    assert 'no candidates :(' in shell.stdout

    bead_a = beads[bead_a]
    assert bead_with_inputs in shell.stdout
    assert bead_a.freeze_time_str in shell.stdout


def test_verbose_inputs_not_in_known_boxes(shell, beads, bead_with_inputs, bead_a):
    shell.bead('edit', bead_with_inputs)
    shell.cd(bead_with_inputs)
    shell.reset()
    shell.bead('status', '--verbose')

    assert bead_with_inputs in shell.stdout
    assert 'no candidates :(' in shell.stdout

    bead_a = beads[bead_a]
    bead_with_inputs = beads[bead_with_inputs]
    assert bead_with_inputs.kind in shell.stdout
    assert bead_a.kind in shell.stdout
    assert bead_a.freeze_time_str in shell.stdout
    assert bead_a.content_id in shell.stdout


def test_invalid_workspace(shell):
    shell.bead('status')
    assert 'WARNING' in shell.stderr
