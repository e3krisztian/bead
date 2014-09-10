from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function

from ..test import TestCase
from . import workspace as m

import os
import zipfile

from ..path import write_file, ensure_directory, temp_dir
from .archive import Archive
from ..timestamp import timestamp


class Test_create(TestCase):

    def test_valid(self):
        self.given_an_empty_directory()
        self.when_initialized()
        self.then_directory_is_a_valid_pkg_dir()

    def test_has_no_inputs(self):
        self.given_an_empty_directory()
        self.when_initialized()
        self.then_workspace_has_no_inputs()

    # implementation

    __workspace_dir = None

    @property
    def workspace(self):
        return m.Workspace(self.__workspace_dir)

    def given_an_empty_directory(self):
        self.__workspace_dir = self.new_temp_dir()

    def when_initialized(self):
        self.workspace.create()

    def then_directory_is_a_valid_pkg_dir(self):
        self.assertTrue(self.workspace.is_valid)

    def then_workspace_has_no_inputs(self):
        self.assertFalse(self.workspace.has_input('pkg1'))
        self.assertFalse(self.workspace.is_mounted('pkg1'))


class Test_pack(TestCase):

    def test_creates_valid_archive(self):
        self.given_a_package_directory()
        self.when_archived()
        self.then_archive_is_valid_package()

    def test_archives_all_content(self):
        self.given_a_package_directory()
        self.when_archived()
        self.then_archive_contains_files_from_package_directory()

    # implementation

    __workspace_dir = None
    __zipdir = None
    __zipfile = None
    __SOURCE1 = b's1'
    __SOURCE2 = b's2'
    __OUTPUT1 = b'o1'
    assert __SOURCE2 != __SOURCE1

    @property
    def workspace(self):
        return m.Workspace(self.__workspace_dir)

    def given_a_package_directory(self):
        self.__workspace_dir = self.new_temp_dir()
        self.workspace.create()
        write_file(self.__workspace_dir / 'output/output1', self.__OUTPUT1)
        write_file(self.__workspace_dir / 'source1', self.__SOURCE1)
        ensure_directory(self.__workspace_dir / 'subdir')
        write_file(self.__workspace_dir / 'subdir/source2', self.__SOURCE2)

    def when_archived(self):
        self.__zipdir = self.new_temp_dir()
        self.__zipfile = self.__zipdir / 'pkg.zip'
        self.workspace.pack(self.__zipfile, timestamp())

    def then_archive_contains_files_from_package_directory(self):
        z = zipfile.ZipFile(self.__zipfile)

        self.assertEquals(self.__OUTPUT1, z.read('data/output1'))
        self.assertEquals(self.__SOURCE1, z.read('meta/code/source1'))
        self.assertEquals(self.__SOURCE2, z.read('meta/code/subdir/source2'))

        files = z.namelist()
        self.assertIn('meta/pkgmeta', files)
        self.assertIn('meta/checksums', files)

    def then_archive_is_valid_package(self):
        with Archive(self.__zipfile) as pkg:
            self.assertTrue(pkg.is_valid)


def make_package(path, filespecs):
    with temp_dir() as root:
        workspace = m.Workspace(root)
        workspace.create()
        for filename, content in filespecs.items():
            write_file(workspace.directory / filename, content)
        workspace.pack(path, timestamp())


class Test_mount(TestCase):

    def test_makes_package_files_available_under_input(self):
        self.given_a_package_directory()
        self.when_mounting_a_package()
        self.then_data_files_in_package_are_available_in_workspace()

    def test_mounted_inputs_are_read_only(self):
        self.given_a_package_directory()
        self.when_mounting_a_package()
        self.then_extracted_files_under_input_are_readonly()

    def test_mount_adds_input_to_pkgmeta(self):
        self.given_a_package_directory()
        self.when_mounting_a_package()
        self.then_mount_info_is_added_to_pkgmeta()

    def test_mounting_more_than_one_package(self):
        self.given_a_package_directory()
        self.when_mounting_a_package()
        self.then_another_package_can_be_mounted()

    # implementation

    __workspace_dir = None

    @property
    def workspace(self):
        return m.Workspace(self.__workspace_dir)

    def given_a_package_directory(self):
        self.__workspace_dir = self.new_temp_dir()
        self.workspace.create()

    def _mount_a_package(self, nick):
        mounted_pkg_path = self.new_temp_dir() / 'pkg.zip'
        make_package(
            mounted_pkg_path,
            {'output/output1': 'data for {}'.format(nick).encode('utf-8')}
        )
        self.workspace.mount(nick, Archive(mounted_pkg_path))

    def when_mounting_a_package(self):
        self._mount_a_package('pkg1')

    def then_data_files_in_package_are_available_in_workspace(self):
        with open(self.__workspace_dir / 'input/pkg1/output1', 'rb') as f:
            self.assertEquals(b'data for pkg1', f.read())

    def then_extracted_files_under_input_are_readonly(self):
        root = self.__workspace_dir / 'input/pkg1'
        self.assertTrue(os.path.exists(root))
        self.assertRaises(IOError, open, root / 'output1', 'ab')
        self.assertRaises(IOError, open, root / 'new-file', 'wb')

    def then_mount_info_is_added_to_pkgmeta(self):
        self.assertTrue(self.workspace.has_input('pkg1'))
        self.assertTrue(self.workspace.is_mounted('pkg1'))

    def then_another_package_can_be_mounted(self):
        self._mount_a_package('pkg2')
