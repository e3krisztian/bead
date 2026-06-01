import os
import zipfile

from bead.infra.fs import rmtree
import pytest

from bead.workspace import Workspace

from .test_shell import Shell, setenv


def test_invalid_workspace_causes_error(shell):
    shell.bead('save', expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_on_success_there_is_feedback(shell, box):
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.bead('save')
    assert shell.stdout != '', 'Expected some feedback, but got none :('


@pytest.mark.skipif(not hasattr(os, 'symlink'), reason='missing os.symlink')
def test_symlink_is_resolved_on_save(shell, box):
    # create a workspace with a symlink to a file
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.write_file('file', 'content')
    with shell.environment:
        os.symlink('file', 'symlink')
    # save to box & clean up
    shell.bead('save')
    shell.cd('..')
    shell.bead('discard', 'bead')

    shell.bead('edit', 'bead')
    assert 'content' in shell.read_file(shell.cwd / 'bead/symlink')


@pytest.fixture
def shell_no_box():
    with Shell() as shell_instance:
        yield shell_instance


def test_a_box_is_created_with_known_name(shell_no_box):
    shell = shell_no_box
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.bead('save')
    # there is a message on stderr that a new box has been created
    assert 'home' in shell.stderr
    # a new box with name `home` has been indeed created and it has exactly one bead
    with shell.environment as env:
        homebox = env.get_box('home')
    assert 1 == bead_count(homebox)


def bead_count(box, kind=None):
    return sum(1 for bead in box.all_beads() if kind in [None, bead.kind])


@pytest.fixture
def shell_multi_box(tmp_path_factory):
    with Shell() as shell_instance:
        yield shell_instance


@pytest.fixture
def make_box(shell_multi_box, tmp_path_factory):
    def _make_box(name):
        directory = tmp_path_factory.mktemp(f"box_{name}")
        shell_multi_box.bead('box', 'add', name, directory)
        # Use the environment to get the proper index file path
        with shell_multi_box.environment as env:
            box = env.get_box(name)
            return box
    return _make_box


@pytest.fixture
def box1(make_box):
    return make_box('box1')


@pytest.fixture
def box2(make_box):
    return make_box('box2')


def test_save_dies_without_explicit_box(shell_multi_box, box1, box2):
    shell = shell_multi_box
    shell.bead('new', 'bead')
    shell.bead('save', 'bead', expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_save_stores_bead_in_specified_box(shell_multi_box, box1, box2):
    shell = shell_multi_box
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.bead('save', box1.name)
    with shell.environment:
        kind = Workspace('.').kind
    assert 1 == bead_count(box1, kind)
    assert 0 == bead_count(box2, kind)
    shell.bead('save', box2.name)
    assert 1 == bead_count(box1, kind)
    assert 1 == bead_count(box2, kind)


def test_invalid_box_specified(shell_multi_box, box1, box2):
    shell = shell_multi_box
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.bead('save', 'unknown-box', expect_failure=True)
    assert 'ERROR' in shell.stderr


def test_save_to_box_without_backing_directory(shell_multi_box, box1, box2):
    shell = shell_multi_box
    shell.bead('new', 'bead')
    shell.cd('bead')
    rmtree(box2.directory)
    shell.bead('save', box2.name, expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert 'does not exist' in shell.stderr


def _zip_compression_types(zip_path):
    with zipfile.ZipFile(zip_path) as zf:
        return {info.compress_type for info in zf.infolist()}


def _find_saved_zip(box):
    zips = list(box.directory.glob('*.zip'))
    assert len(zips) == 1, f'Expected 1 zip, found {zips}'
    return zips[0]


def test_save_default_uses_compression(shell, box):
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.write_file('output/data.txt', 'x' * 1000)
    shell.bead('save')
    zip_path = _find_saved_zip(box)
    types = _zip_compression_types(zip_path)
    assert zipfile.ZIP_DEFLATED in types


def test_save_no_compress_flag_uses_stored(shell, box):
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.write_file('output/data.txt', 'some content')
    shell.bead('save', '--no-compress')
    zip_path = _find_saved_zip(box)
    types = _zip_compression_types(zip_path)
    assert zipfile.ZIP_STORED in types
    assert zipfile.ZIP_DEFLATED not in types


def test_save_dash_zero_flag_uses_stored(shell, box):
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.write_file('output/data.txt', 'some content')
    shell.bead('save', '-0')
    zip_path = _find_saved_zip(box)
    types = _zip_compression_types(zip_path)
    assert zipfile.ZIP_STORED in types
    assert zipfile.ZIP_DEFLATED not in types


def test_save_env_var_off_uses_stored(shell, box):
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.write_file('output/data.txt', 'some content')
    with setenv('BEAD_ZIP_COMPRESSION', 'off'):
        shell.bead('save')
    zip_path = _find_saved_zip(box)
    types = _zip_compression_types(zip_path)
    assert zipfile.ZIP_STORED in types
    assert zipfile.ZIP_DEFLATED not in types


def test_save_no_compress_overrides_env_var(shell, box):
    shell.bead('new', 'bead')
    shell.cd('bead')
    shell.write_file('output/data.txt', 'some content')
    with setenv('BEAD_ZIP_COMPRESSION', 'deflated'):
        shell.bead('save', '--no-compress')
    zip_path = _find_saved_zip(box)
    types = _zip_compression_types(zip_path)
    assert zipfile.ZIP_STORED in types
    assert zipfile.ZIP_DEFLATED not in types
