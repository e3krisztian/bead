import os
import zipfile

import pytest

from . import layouts
from . import ziparchive as m


@pytest.fixture
def bead_archive(tmp_path_factory):
    """Create a test bead archive with sample files."""
    bead_path = tmp_path_factory.mktemp('bead_archive') / 'bead.zip'
    with zipfile.ZipFile(bead_path, 'w') as z:
        z.writestr(
            layouts.Archive.BEAD_META,
            b'''
                {
                    "meta_version": "aaa947a6-1f7a-11e6-ba3a-0021cc73492e",
                    "kind": "TEST-FAKE",
                    "freeze_time": "20200913T173910000000+0000",
                    "inputs": {}
                }
            ''')
        z.writestr('somefile1', b'''somefile1's known content''')
        z.writestr('path/file1', b'''?? file1's known content''')
        z.writestr('path/to/file1', b'''file1's known content''')
        z.writestr('path/to/file2', b'''file2's known content''')
        z.writestr(layouts.Archive.MANIFEST, b'some manifest')
    return bead_path


def test_extract_file(bead_archive, tmp_path):
    """Test extracting a single file from the archive."""
    # when file1 is extracted
    extracted_file = tmp_path / 'extracted_file'
    bead = m.ZipArchive(bead_archive)
    bead.extract_file('path/to/file1', extracted_file)

    # then file1 has the expected content
    content = extracted_file.read_bytes()
    assert content == b'''file1's known content'''


def test_extract_dir(bead_archive, tmp_path):
    """Test extracting a directory from the archive."""
    # when a directory is extracted
    extracted_dir = tmp_path / 'destination dir'
    bead = m.ZipArchive(bead_archive)
    bead.extract_dir('path/to', extracted_dir)
    extracted_file = extracted_dir / 'file1'

    # then directory has the expected files
    files = set(os.listdir(extracted_dir))
    assert {'file1', 'file2'} == files

    # then file1 has the expected content
    content = extracted_file.read_bytes()
    assert content == b'''file1's known content'''


def test_extract_nonexistent_dir(bead_archive, tmp_path):
    """Test extracting a non-existent directory creates an empty directory."""
    # when a nonexistent directory is extracted
    extracted_dir = tmp_path / 'destination dir'
    bead = m.ZipArchive(bead_archive)
    bead.extract_dir('path/to/nonexistent', extracted_dir)

    # then an empty directory is created
    is_dir = os.path.isdir(extracted_dir)
    files = os.listdir(extracted_dir)
    assert is_dir
    assert [] == files


def test_content_id(bead_archive):
    """Test that content_id returns a string."""
    # when content_id is checked
    bead = m.ZipArchive(bead_archive)
    content_id = bead.content_id

    # then content_id is a string
    assert isinstance(content_id, str)
