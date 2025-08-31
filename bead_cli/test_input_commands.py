import os

from bead.tech.fs import rmtree
import pytest

from bead.workspace import Workspace


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

    robot.cli('develop', 'nextbead')
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
    robot.cli('develop', bead_with_inputs)
    robot.cd(bead_with_inputs)

    robot.cli('status')
    assert bead_b in robot.stdout

    assert not Workspace(robot.cwd).is_loaded('input_b')

    robot.cli('input', 'update', 'input_b', bead_a)
    check.loaded('input_b', bead_a)

    robot.cli('status')
    assert bead_b not in robot.stdout


def test_load_on_workspace_without_input_gives_feedback(robot, bead_a):
    robot.cli('develop', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'load')

    assert 'WARNING' in robot.stderr
    assert 'No inputs defined to load.' in robot.stderr


def test_load_with_missing_bead_gives_warning(robot, bead_with_inputs, bead_a):
    robot.cli('develop', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.reset()
    robot.cli('input', 'load')
    assert 'WARNING' in robot.stderr
    assert 'input_a' in robot.stderr
    assert 'input_b' in robot.stderr


def test_load_only_one_input(robot, bead_with_inputs, bead_a, check):
    robot.cli('develop', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('input', 'load', 'input_a')
    check.loaded('input_a', bead_a)
    with robot.environment:
        assert not Workspace('.').is_loaded('input_b')


def test_deleted_box_does_not_stop_load(robot, bead_with_inputs, tmp_path_factory):
    deleted_box = tmp_path_factory.mktemp("deleted_box")
    robot.cli('box', 'add', 'missing', deleted_box)
    rmtree(deleted_box)
    robot.cli('develop', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('input', 'load')


def test_add_with_unrecognized_bead_name_exits_with_error(robot, bead_a):
    robot.cli('develop', bead_a)
    robot.cd(bead_a)
    with pytest.raises(SystemExit):
        robot.cli('input', 'add', 'x', 'non-existing-bead')
    assert 'ERROR' in robot.stderr
    assert 'non-existing-bead' in robot.stderr


def test_add_with_path_separator_in_name_is_error(robot, bead_a, bead_b):
    robot.cli('develop', bead_a)
    robot.cd(bead_a)
    with pytest.raises(SystemExit):
        robot.cli('input', 'add', 'name/with/path/separator', bead_b)
    assert 'ERROR' in robot.stderr
    assert 'name/with/path/separator' in robot.stderr

    robot.cli('status')
    assert bead_b not in robot.stdout
    assert [] == list(robot.ls('input'))


def test_add_with_hacked_bead_is_refused(robot, hacked_bead, bead_a):
    robot.cli('develop', bead_a)
    robot.cd(bead_a)
    robot.cli('input', 'add', 'hack', hacked_bead)
    assert not Workspace(robot.cwd).has_input('hack')
    assert 'WARNING' in robot.stderr


def test_update_with_hacked_bead_is_refused(robot, hacked_bead, bead_a, check):
    robot.cli('develop', bead_a)
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

    robot.cli('input', 'update', 'input1', '--next')
    check.loaded('input1', times.TS2)

    robot.cli('input', 'update', 'input1', '-N')
    check.loaded('input1', times.TS3)


def test_update_to_previous_version(robot, bead_with_history, check, times):
    robot.cli('new', 'test-workspace')
    robot.cd('test-workspace')
    # add version TS1
    robot.cli('input', 'add', 'input1', 'bead_with_history', '--time', times.TS4)
    check.loaded('input1', times.TS4)

    robot.cli('input', 'update', 'input1', '--prev')
    check.loaded('input1', times.TS3)

    robot.cli('input', 'update', 'input1', '-P')
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
    robot.cli('develop', bead_with_inputs)
    robot.cd(bead_with_inputs)
    robot.cli('input', 'load')
    assert os.path.exists(robot.cwd / 'input/input_a')
    assert os.path.exists(robot.cwd / 'input/input_b')

    robot.cli('input', 'unload')
    assert not os.path.exists(robot.cwd / 'input/input_a')
    assert not os.path.exists(robot.cwd / 'input/input_b')


def test_unload(robot, bead_with_inputs):
    robot.cli('develop', bead_with_inputs)
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
    robot.cli('develop', bead_a)
    robot.cd(bead_a)
    with pytest.raises(SystemExit):
        robot.cli('input', 'delete', 'nonexisting')
    assert 'ERROR' in robot.stderr
    assert 'does not exist' in robot.stderr
