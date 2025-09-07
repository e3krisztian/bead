import os
import zipfile

import pytest

from bead.exceptions import InvalidArchive

from . import layouts
from . import tech
from . import workspace as m
from .ziparchive import ZipArchive

write_file = tech.fs.write_file
ensure_directory = tech.fs.ensure_directory
temp_dir = tech.fs.temp_dir
timestamp = tech.timestamp.timestamp
Path = tech.fs.Path

A_KIND = 'an arbitrary identifier that is not used by chance'


@pytest.fixture
def workspace_dir(tmp_path_factory):
    """Provide a workspace directory."""
    return tmp_path_factory.mktemp('workspaces') / 'new_workspace'


def test_create_valid(workspace_dir):
    """Test that a newly created workspace is valid."""
    workspace = m.Workspace(workspace_dir)
    workspace.create(A_KIND)
    assert workspace.is_valid


def test_create_has_no_inputs(workspace_dir):
    """Test that a newly created workspace has no inputs."""
    workspace = m.Workspace(workspace_dir)
    workspace.create(A_KIND)
    assert not workspace.has_input('bead1')
    assert not workspace.is_loaded('bead1')
    assert not workspace.inputs


def test_create_of_specified_kind(workspace_dir):
    """Test that a newly created workspace has the specified kind."""
    workspace = m.Workspace(workspace_dir)
    workspace.create(A_KIND)
    assert A_KIND == workspace.kind


def test_for_current_working_directory_non_workspace(tmp_path, monkeypatch):
    """Test workspace detection in non-workspace directory."""
    monkeypatch.chdir(tmp_path)
    ws = m.Workspace.for_current_working_directory()
    assert tmp_path.resolve() == ws.directory


def test_for_current_working_directory_workspace_root(tmp_path, monkeypatch):
    """Test workspace detection from workspace root."""
    root = tmp_path / 'new_workspace'
    workspace = m.Workspace(root)
    workspace.create(A_KIND)
    monkeypatch.chdir(root)
    ws = m.Workspace.for_current_working_directory()
    assert root.resolve() == ws.directory


def test_for_current_working_directory_above_root(tmp_path, monkeypatch):
    """Test workspace detection from subdirectory."""
    root = tmp_path / 'new_workspace'
    workspace = m.Workspace(root)
    workspace.create(A_KIND)
    monkeypatch.chdir(root / layouts.Workspace.INPUT)
    ws = m.Workspace.for_current_working_directory()
    assert root.resolve() == ws.directory


SOURCE1 = b's1'
SOURCE2 = b's2'
OUTPUT1 = b'o1'
assert SOURCE2 != SOURCE1
BEAD_COMMENT = 'custom bead comment'


@pytest.fixture
def pack_workspace(tmp_path_factory):
    """Create a workspace with test files for packing."""
    workspace_dir = tmp_path_factory.mktemp('root') / 'workspace'
    ws = m.Workspace(workspace_dir)
    ws.create(A_KIND)
    layout = layouts.Workspace

    write_file(
        workspace_dir / layout.TEMP / 'README',
        'temporary directory')
    write_file(
        workspace_dir / layout.OUTPUT / 'output1',
        OUTPUT1)
    write_file(workspace_dir / 'source1', SOURCE1)
    ensure_directory(workspace_dir / 'subdir')
    write_file(workspace_dir / 'subdir/source2', SOURCE2)
    return ws


@pytest.fixture
def packed_archive(pack_workspace, tmp_path_factory):
    """Create a packed archive from the workspace."""
    zipfile_path = tmp_path_factory.mktemp('dir') / 'bead.zip'
    pack_workspace.pack(zipfile_path, timestamp(), BEAD_COMMENT)
    return zipfile_path


def test_pack_creates_valid_archive(packed_archive):
    """Test that packing creates a valid archive."""
    bead = ZipArchive(packed_archive)
    bead.validate()


def test_pack_archives_all_content(packed_archive):
    """Test that packing includes all expected content."""
    archive_path = packed_archive.as_posix()
    with zipfile.ZipFile(archive_path) as z:
        layout = layouts.Archive

        output1_content = z.read(f'{layout.DATA}/output1')
        source1_content = z.read(f'{layout.CODE}/source1')
        source2_content = z.read(f'{layout.CODE}/subdir/source2')

        assert OUTPUT1 == output1_content
        assert SOURCE1 == source1_content
        assert SOURCE2 == source2_content

        files = z.namelist()
        assert layout.BEAD_META in files
        assert layout.MANIFEST in files


def test_pack_not_saved_content(packed_archive):
    """Test that packing excludes workspace meta and temp files."""
    def does_not_contain(workspace_path):
        with zipfile.ZipFile(packed_archive) as z:
            archive_path = layouts.Archive.CODE / workspace_path
            with pytest.raises(KeyError):
                z.getinfo(str(archive_path))

    does_not_contain(layouts.Workspace.BEAD_META)
    does_not_contain(layouts.Workspace.TEMP / 'README')


def test_pack_archive_has_comment(packed_archive):
    """Test that the archive has the expected comment."""
    with zipfile.ZipFile(packed_archive) as z:
        comment = z.comment.decode('utf-8')
        assert BEAD_COMMENT == comment


def test_pack_stability_directory_name_data_and_timestamp_determines_content_ids(tmp_path_factory):
    """Test that content IDs are stable based on directory name, data, and timestamp."""
    TS = '20150910T093724802366+0200'

    # note: it is important to create the same bead in
    # two different directories
    def make_bead():
        temp_dir = tmp_path_factory.mktemp("bead_stability")
        output = temp_dir / 'bead.zip'
        ws_dir = temp_dir / 'workspace'
        ws = m.Workspace(ws_dir)
        ws.create(A_KIND)
        write_file(ws.directory / 'source1', 'code to produce output')
        write_file(ws.directory / 'output/output1', TS)
        ws.pack(output, TS, comment='')
        return ZipArchive(output)

    bead1 = make_bead()
    bead2 = make_bead()
    assert bead1.content_id == bead2.content_id


def make_bead(path, filespecs, tmp_path_factory):
    """Helper function to create a bead with specified files."""
    temp_dir = tmp_path_factory.mktemp("make_bead")
    workspace = m.Workspace(temp_dir / 'workspace')
    workspace.create(A_KIND)
    for filename, content in filespecs.items():
        write_file(workspace.directory / filename, content)
    workspace.pack(path, timestamp(), 'no comment')


@pytest.fixture
def load_workspace(tmp_path_factory):
    """Create a workspace for loading tests."""
    workspace_dir = tmp_path_factory.mktemp('workspaces') / 'workspace'
    ws = m.Workspace(workspace_dir)
    ws.create(A_KIND)
    return ws


def _load_a_bead(workspace, input_nick, tmp_path_factory):
    """Helper function to load a bead into workspace."""
    temp_dir = tmp_path_factory.mktemp(f"load_{input_nick}")
    path_of_bead_to_load = temp_dir / f'{input_nick}.zip'
    make_bead(
        path_of_bead_to_load,
        {
            'output/output1':
            f'data for {input_nick}'.encode()
        },
        tmp_path_factory
    )
    workspace.load(input_nick, ZipArchive(path_of_bead_to_load))


def test_load_makes_bead_files_available_under_input(load_workspace, tmp_path_factory):
    """Test that loading a bead makes its files available under input."""
    _load_a_bead(load_workspace, 'bead1', tmp_path_factory)

    input_file = load_workspace.directory / 'input/bead1/output1'
    content = input_file.read_bytes()
    assert content == b'data for bead1'


def test_load_loaded_inputs_are_read_only(load_workspace, tmp_path_factory):
    """Test that loaded input files are read-only."""
    _load_a_bead(load_workspace, 'bead1', tmp_path_factory)

    root = load_workspace.directory / 'input/bead1'
    assert root.exists()
    with pytest.raises(IOError):
        open(root / 'output1', 'ab')
    # also folders are read only - this does not work on Windows
    if os.name == 'posix':
        with pytest.raises(IOError):
            (root / 'new-file').write_bytes(b'')


def test_load_adds_input_to_bead_meta(load_workspace, tmp_path_factory):
    """Test that loading adds input info to bead meta."""
    _load_a_bead(load_workspace, 'bead1', tmp_path_factory)

    assert load_workspace.has_input('bead1')
    assert load_workspace.is_loaded('bead1')


def test_load_loading_more_than_one_bead(load_workspace, tmp_path_factory):
    """Test that multiple beads can be loaded."""
    _load_a_bead(load_workspace, 'bead1', tmp_path_factory)
    _load_a_bead(load_workspace, 'bead2', tmp_path_factory)

    assert load_workspace.has_input('bead1')
    assert load_workspace.has_input('bead2')


@pytest.fixture
def input_nick():
    """Provide a test input nickname."""
    return 'input_nick'


@pytest.fixture
def workspace_with_input(tmp_path_factory, input_nick):
    """Create a workspace with an input."""
    workspace_dir = tmp_path_factory.mktemp('workspaces') / 'workspace'
    ws = m.Workspace(workspace_dir)
    ws.create(A_KIND)
    ws.add_input(input_nick, A_KIND, 'content_id', timestamp())
    return ws


def add_input(workspace, input_nick):
    """Helper function to add an input to workspace."""
    workspace.add_input(input_nick, A_KIND, 'content_id', timestamp())


def unzip(archive_path, directory):
    """Helper function to unzip an archive."""
    ensure_directory(directory)
    with zipfile.ZipFile(archive_path) as z:
        z.extractall(directory)


def zip_up(directory, archive_path):
    """Helper function to zip up a directory."""
    with zipfile.ZipFile(archive_path, 'w') as z:
        def add(path, zip_path):
            if os.path.isdir(path):
                for name in os.listdir(path):
                    add(path / name, zip_path / name)
            else:
                z.write(path, zip_path)
        add(directory, Path('/'))


@pytest.fixture
def validation_workspace(tmp_path_factory):
    """Create a workspace for validation tests."""
    workspace = m.Workspace(tmp_path_factory.mktemp('workspaces') / 'workspace')
    workspace.create(A_KIND)
    return workspace


@pytest.fixture
def validation_timestamp():
    """Provide a timestamp for validation tests."""
    return '20150930T093724802366+0200'


@pytest.fixture
def archive_path(validation_workspace, validation_timestamp, tmp_path_factory):
    """Create an archive path."""
    archive_path = tmp_path_factory.mktemp('archives') / 'bead.zip'
    validation_workspace.pack(archive_path, validation_timestamp, comment=archive_path.as_posix())
    return archive_path


@pytest.fixture
def archive_with_two_files_path(validation_workspace, validation_timestamp, tmp_path_factory):
    """Create an archive with two files."""
    write_file(validation_workspace.directory / 'code1', 'code1')
    write_file(validation_workspace.directory / 'output/data1', 'data1')
    archive_path = tmp_path_factory.mktemp('archives') / 'bead.zip'
    validation_workspace.pack(archive_path, validation_timestamp, comment=archive_path.as_posix())
    return archive_path


@pytest.fixture
def unzipped_archive_path(archive_with_two_files_path, tmp_path_factory):
    """Create an unzipped archive directory."""
    path = tmp_path_factory.mktemp('unzipped') / 'unzipped'
    unzip(archive_with_two_files_path, path)
    return path


def test_newly_created_bead_is_valid(archive_with_two_files_path):
    """Test that a newly created bead is valid."""
    ZipArchive(archive_with_two_files_path).validate()


def test_adding_a_data_file_to_an_archive_makes_bead_invalid(archive_path):
    """Test that adding a data file makes the bead invalid."""
    with zipfile.ZipFile(archive_path, 'a') as z:
        z.writestr(f'{layouts.Archive.DATA}/extra_file', b'something')

    with pytest.raises(InvalidArchive):
        ZipArchive(archive_path).validate()


def test_adding_a_code_file_to_an_archive_makes_bead_invalid(archive_path):
    """Test that adding a code file makes the bead invalid."""
    with zipfile.ZipFile(archive_path, 'a') as z:
        z.writestr(f'{layouts.Archive.CODE}/extra_file', b'something')

    with pytest.raises(InvalidArchive):
        ZipArchive(archive_path).validate()


def test_unzipping_and_zipping_an_archive_remains_valid(unzipped_archive_path, tmp_path):
    """Test that unzipping and rezipping keeps the archive valid."""
    rezipped_archive_path = tmp_path / 'rezipped_archive.zip'
    zip_up(unzipped_archive_path, rezipped_archive_path)

    ZipArchive(rezipped_archive_path).validate()


def test_deleting_a_file_in_the_manifest_makes_the_bead_invalid(unzipped_archive_path, tmp_path):
    """Test that deleting a file from the manifest makes the bead invalid."""
    os.remove(unzipped_archive_path / layouts.Archive.CODE / 'code1')
    modified_archive_path = tmp_path / 'modified_archive.zip'
    zip_up(unzipped_archive_path, modified_archive_path)

    with pytest.raises(InvalidArchive):
        ZipArchive(modified_archive_path).validate()


def test_changing_a_file_makes_the_bead_invalid(unzipped_archive_path, tmp_path):
    """Test that changing a file makes the bead invalid."""
    write_file(unzipped_archive_path / layouts.Archive.CODE / 'code1', b'HACKED')
    modified_archive_path = tmp_path / 'modified_archive.zip'
    zip_up(unzipped_archive_path, modified_archive_path)

    with pytest.raises(InvalidArchive):
        ZipArchive(modified_archive_path).validate()
