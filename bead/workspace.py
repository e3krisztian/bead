'''
Proto-Beads & their filesystem layout
'''

import os
import zipfile

from . import layouts
from . import meta
from .bead import Archive, Computation
from .infra import persistence
from .infra import securehash
from .infra.fs import Path
from .infra.fs import all_subpaths
from .infra.fs import ensure_directory
from .infra.fs import make_readonly
from .infra.fs import make_writable
from .infra.fs import rmtree
from .infra.fs import write_file
from .infra.identifier import uuid


# generated with `uuidgen -t`
META_VERSION = 'aaa947a6-1f7a-11e6-ba3a-0021cc73492e'


class Workspace(Computation):
    '''
    Mutable workspace for developing a computation.

    Like a kitchen workspace where you're actively working with
    ingredients (inputs) and code to produce outputs. The computation
    can be modified, rerun, and eventually frozen into a Bead.
    '''

    directory: Path

    def __init__(self, directory):
        self.directory = Path(directory).resolve()

    @property
    def is_valid(self):
        dir = self.directory
        return all(
            (
                (dir / layouts.Workspace.INPUT).is_dir(),
                (dir / layouts.Workspace.OUTPUT).is_dir(),
                (dir / layouts.Workspace.TEMP).is_dir(),
                (dir / layouts.Workspace.BEAD_META).is_file()))

    @property
    def _meta_filename(self):
        return self.directory / layouts.Workspace.BEAD_META

    @property
    def meta(self):
        return persistence.file_load(self._meta_filename)

    @meta.setter
    def meta(self, meta):
        persistence.file_dump(meta, self._meta_filename)

    # Computation properties
    @property
    def kind(self):  # type: ignore[override]
        return self.meta[meta.KIND]

    def set_kind(self, kind):
        m = self.meta
        m[meta.KIND] = kind
        self.meta = m

    @property
    def name(self):  # type: ignore[override]
        return self.directory.name

    @property
    def inputs(self):  # type: ignore[override]
        return tuple(meta.parse_inputs(self.meta))

    # workspace constructors
    def create(self, kind):
        '''
        Set up an empty project structure.

        Works with either an empty directory or a directory to be created.
        '''
        dir = self.directory
        assert not dir.exists()

        self.create_directories()

        bead_meta = {
            meta.KIND: kind,
            meta.INPUTS: {}}
        write_file(
            dir / layouts.Workspace.BEAD_META,
            persistence.dumps(bead_meta))

        assert self.is_valid

    def init(self):
        self.create_directories()
        if not self._meta_filename.exists():
            bead_meta = {meta.KIND: uuid(), meta.INPUTS: {}}
            write_file(self._meta_filename, persistence.dumps(bead_meta))

    def create_directories(self):
        dir = self.directory
        ensure_directory(dir)
        ensure_directory(dir / layouts.Workspace.INPUT)
        make_readonly(dir / layouts.Workspace.INPUT)
        ensure_directory(dir / layouts.Workspace.OUTPUT)
        ensure_directory(dir / layouts.Workspace.TEMP)
        ensure_directory(dir / layouts.Workspace.META)

    def pack(self, zipfilename: Path, freeze_time, comment: str):
        '''
        Create archive from workspace.
        '''
        zipfilename = Path(zipfilename)
        assert not zipfilename.exists()
        try:
            _ZipCreator().create(zipfilename, self, freeze_time, comment)
        except (RuntimeError, Exception):
            if zipfilename.exists():
                zipfilename.unlink()
            raise

    def has_input(self, input_name):
        '''
        Is there an input defined for input_name?

        NOTE: it is not necessarily loaded!
        '''
        return input_name in self.meta[meta.INPUTS]

    def is_loaded(self, input_name):
        return (self.directory / layouts.Workspace.INPUT / input_name).is_dir()

    def add_input(self, input_name, kind, content_id, freeze_time_iso):
        m = self.meta
        m[meta.INPUTS][input_name] = {
            meta.INPUT_KIND: kind,
            meta.INPUT_CONTENT_ID: content_id,
            meta.INPUT_FREEZE_TIME: freeze_time_iso}
        self.meta = m

    def delete_input(self, input_name):
        assert self.has_input(input_name)
        if self.is_loaded(input_name):
            self.unload(input_name)
        m = self.meta
        del m[meta.INPUTS][input_name]
        self.meta = m

    def load(self, input_name, archive: Archive):
        '''
        Make output data files in archive available under input directory
        '''
        input_dir = self.directory / layouts.Workspace.INPUT
        make_writable(input_dir)
        try:
            self.add_input(
                input_name,
                archive.kind, archive.content_id, archive.freeze_time_iso)
            destination_dir = input_dir / input_name
            archive.unpack_data_to(destination_dir)
            for f in all_subpaths(destination_dir):
                make_readonly(f)
        finally:
            make_readonly(input_dir)

    def unload(self, input_name):
        '''
        Remove files for given input
        '''
        assert self.has_input(input_name)
        input_dir = self.directory / layouts.Workspace.INPUT
        make_writable(input_dir)
        try:
            rmtree(input_dir / input_name)
        finally:
            make_readonly(input_dir)

    @property
    def _input_map_filename(self):
        return self.directory / layouts.Workspace.INPUT_MAP

    @property
    def input_map(self):
        """
        Map from local (bead specific) input names to real (more widely recognised) bead names
        """
        try:
            return persistence.file_load(self._input_map_filename)
        except FileNotFoundError:
            return {}

    @input_map.setter
    def input_map(self, input_map):
        persistence.file_dump(input_map, self._input_map_filename)

    def get_source_name(self, input_name):
        '''
        Get the source bead name for an input.

        Returns the source bead name for the input, or the input name itself
        if no source mapping exists. This is the name used for update operations.
        '''
        return self.input_map.get(input_name, input_name)

    def set_source_name(self, input_name, bead_name):
        '''
        Set the source bead name for an input.

        Establishes or updates which bead is the source for this input.
        This determines the bead name used for future update operations.
        '''
        input_map = self.input_map
        input_map[input_name] = bead_name
        self.input_map = input_map

    def __repr__(self):
        # default values are printed as repr of the value
        return self.directory.as_posix()

    @classmethod
    def for_current_working_directory(cls):
        '''
        Create Workspace based on current working directory.

        Determine the correct Workspace for the current working directory.
        As a result, the returned workspace may be for a parent directory,
        if the cwd is under a valid workspace, but not at its root.

        Can return an invalid Workspace.
        '''
        cwd = cls(os.getcwd())
        ws = cwd
        while not ws.is_valid:
            parent = ws.directory.parent
            if parent == ws.directory:
                return cwd
            ws = cls(parent)
        return ws


class _ZipCreator:
    def __init__(self):
        self.hashes = {}
        self.zipfile = None

    def add_hash(self, path, hash):
        assert path not in self.hashes
        self.hashes[path] = hash

    def add_file(self, path, zip_path: str):
        assert self.zipfile
        self.zipfile.write(path, zip_path)
        self.add_hash(
            zip_path,
            securehash.file(open(path, 'rb'), os.path.getsize(path)))

    def add_path(self, path, zip_path):
        if os.path.isdir(path):
            self.add_directory(path, zip_path)
        else:
            assert os.path.isfile(path), '%s is neither a file nor a directory' % path
            self.add_file(path, zip_path)

    def add_directory(self, path, zip_path: str):
        for f in os.listdir(path):
            self.add_path(path / f, f'{zip_path}/{f}')

    def add_string_content(self, zip_path: str, string):
        assert self.zipfile
        bytes = string.encode('utf-8')
        self.zipfile.writestr(zip_path, bytes)
        self.add_hash(zip_path, securehash.bytes(bytes))

    def create(self, zip_file_name: Path, workspace, freeze_timestamp, comment: str, compress: bool = True):
        assert workspace.is_valid
        compression = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
        try:
            with zipfile.ZipFile(
                zip_file_name,
                mode='w',
                compression=compression,
                allowZip64=True,
            ) as self.zipfile:
                self.zipfile.comment = comment.encode('utf-8')
                self.add_data(workspace)
                self.add_code(workspace)
                self.add_meta(workspace, freeze_timestamp)
        finally:
            self.zipfile = None

    def add_code(self, workspace):
        source_directory = workspace.directory

        def is_code(f):
            return f not in {
                layouts.Workspace.INPUT.as_posix(),
                layouts.Workspace.OUTPUT.as_posix(),
                layouts.Workspace.META.as_posix(),
                layouts.Workspace.TEMP.as_posix()}

        for f in sorted(os.listdir(source_directory)):
            if is_code(f):
                self.add_path(
                    source_directory / f,
                    f'{layouts.Archive.CODE}/{f}')

    def add_data(self, workspace):
        self.add_directory(
            workspace.directory / layouts.Workspace.OUTPUT,
            layouts.Archive.DATA)

    def add_meta(self, workspace, freeze_timestamp):
        bead_meta = {
            meta.META_VERSION: META_VERSION,
            meta.KIND: workspace.kind,
            meta.FREEZE_TIME: freeze_timestamp,
            meta.INPUTS: {
                input.name: {
                    meta.INPUT_KIND: input.kind,
                    meta.INPUT_CONTENT_ID: input.content_id,
                    meta.INPUT_FREEZE_TIME: input.freeze_time_iso}
                for input in workspace.inputs},
            meta.FREEZE_NAME: workspace.name}

        self.add_string_content(layouts.Archive.BEAD_META, persistence.dumps(bead_meta))
        self.add_string_content(layouts.Archive.MANIFEST, persistence.dumps(self.hashes))
        # INPUT_MAP is operational metadata that should NOT be tracked in manifest (manifest only tracks immutable files)
        # so we need to save it directly, without recording its hash
        persistence.zip_dump(workspace.input_map, self.zipfile, layouts.Archive.INPUT_MAP)
