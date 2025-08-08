import os

import pytest

from . import fs as m


def test_make_readonly_file(tmp_path):
    """Test making a file read-only."""
    # given a file
    file_path = tmp_path / 'file'
    file_path.write_bytes(b'')

    # when made readonly
    m.make_readonly(file_path)

    # then file can not be written
    with pytest.raises(IOError):
        open(file_path, 'wb')


@pytest.mark.skipif(os.name != 'posix', reason='read only folders do not work e.g. on Windows')
# According to https://support.microsoft.com/EN-US/help/256614
# "Unlike the Read-only attribute for a file, the Read-only attribute
# for a folder is typically ignored by Windows, Windows components and
# accessories, and other programs. For example, you can delete, rename,
# and change a folder with the Read-only attribute by using Windows Explorer."
def test_make_readonly_directory(tmp_path):
    """Test making a directory read-only."""
    # given a directory
    dir_path = tmp_path

    # when made readonly
    m.make_readonly(dir_path)

    # then can not create file under directory
    with pytest.raises(IOError):
        open(dir_path / 'file', 'wb')


def test_make_writable_file(tmp_path):
    """Test making a read-only file writable."""
    # given a read only file
    file_path = tmp_path / 'file'
    file_path.write_bytes(b'')
    m.make_readonly(file_path)

    # when made writable
    m.make_writable(file_path)

    # then file can be written
    with open(file_path, 'ab') as f:
        f.write(b'little something')


def test_make_writable_directory(tmp_path):
    """Test making a read-only directory writable."""
    # given a read only directory
    dir_path = tmp_path
    m.make_readonly(dir_path)

    # when made writable
    m.make_writable(dir_path)

    # then file can be created under directory
    (dir_path / 'file').write_bytes(b'little something')


def test_all_subpaths(tmp_path):
    """Test collecting all subpaths from a directory structure."""
    DIRS = ('a', 'b', 'c', 'c/d')
    FILES = ('a/f', 'c/d/f1', 'c/d/f2')

    # given some directory structure with files
    root = tmp_path
    for dir in DIRS:
        dir_path = root / dir
        os.makedirs(dir_path)
    for f in FILES:
        file_path = root / f
        m.write_file(file_path, '')

    # when all paths are collected
    paths = set(m.all_subpaths(root))

    # then all paths are found
    dir_paths = {root / d for d in DIRS}
    file_paths = {root / f for f in FILES}
    all_paths = {root} | dir_paths | file_paths
    assert all_paths == paths


def test_read_write_file(tmp_path):
    """Test reading and writing files."""
    testfile = tmp_path / 'testfile'
    content = 'Test_read_write_file testfile content / áíőóüú@!#@!#$$@'
    m.write_file(testfile, content)
    read_content = m.read_file(testfile)
    assert content == read_content
