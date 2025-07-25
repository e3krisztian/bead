import os
import re

from cached_property import cached_property

from tracelog import TRACELOG
from .bead import UnpackableBead
from . import meta
from . import tech

from .ziparchive import ZipArchive
from .exceptions import InvalidArchive

persistence = tech.persistence

__all__ = ('Archive', 'InvalidArchive')



class Archive(UnpackableBead):
    def __init__(self, filename: tech.fs.Path, box_name=''):
        self.archive_filename = filename
        self.archive_path = tech.fs.Path(filename)
        self.box_name = box_name
        self.name = bead_name_from_file_path(filename)

        # Check that we can get access to metadata
        self.meta_version
        self.freeze_time
        self.kind

    @property
    def meta_version(self):
        return self.ziparchive.meta_version

    @property
    def content_id(self):
        return self.ziparchive.content_id

    @property
    def kind(self):
        return self.ziparchive.kind

    @property
    def freeze_time_str(self):
        return self.ziparchive.freeze_time_str

    @property
    def input_map(self):
        return self.ziparchive.input_map

    @cached_property
    def ziparchive(self):
        return ZipArchive(self.archive_filename, self.box_name)

    def validate(self):
        self.ziparchive.validate()

    @property
    def inputs(self):
        return self.ziparchive.inputs

    def extract_dir(self, zip_dir, fs_dir):
        return self.ziparchive.extract_dir(zip_dir, fs_dir)

    def extract_file(self, zip_path, fs_path):
        return self.ziparchive.extract_file(zip_path, fs_path)

    def unpack_code_to(self, fs_dir):
        self.ziparchive.unpack_code_to(fs_dir)

    def unpack_data_to(self, fs_dir):
        self.ziparchive.unpack_data_to(fs_dir)

    def unpack_meta_to(self, workspace):
        workspace.meta = self.ziparchive.meta
        workspace.input_map = self.input_map


def bead_name_from_file_path(path):
    '''
    Parse bead name from a file path.

    Might return a simpler name than intended
    '''
    name_with_timestamp, ext = os.path.splitext(os.path.basename(path))
    # assert ext == '.zip'  # not enforced to allow having beads with different extensions
    name = re.sub('_[0-9]{8}(?:[tT][-+0-9]*)?$', '', name_with_timestamp)
    return meta.BeadName(name)


assert 'bead-2015v3' == bead_name_from_file_path('bead-2015v3.zip')
assert 'bead-2015v3' == bead_name_from_file_path('bead-2015v3_20150923.zip')
assert 'bead-2015v3' == bead_name_from_file_path('bead-2015v3_20150923T010203012345+0200.zip')
assert 'bead-2015v3' == bead_name_from_file_path('bead-2015v3_20150923T010203012345-0200.zip')
assert 'bead-2015v3' == bead_name_from_file_path('path/to/bead-2015v3_20150923.zip')
