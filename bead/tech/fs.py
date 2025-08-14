import contextlib
import os
from pathlib import Path
import shutil
import stat
import tempfile


def ensure_directory(path: Path):
    if not os.path.exists(path):
        os.makedirs(path)

    assert os.path.isdir(path)


def write_file(path: Path, content: bytes | str):
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content)


def read_file(path: Path):
    return path.read_text()


@contextlib.contextmanager
def temp_dir(dir=None):
    if dir is None:
        dir = Path.cwd()
    ensure_directory(dir)

    temp_dir = tempfile.mkdtemp(dir=str(dir))
    try:
        yield Path(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def make_readonly(path: Path):
    '''
    WARNING: It does not work for Windows folders.

    Might fail (silently) on other systems as well.
    '''
    mode = os.stat(path)[stat.ST_MODE]
    os.chmod(path, mode & ~stat.S_IWRITE)


def make_writable(path: Path):
    mode = os.stat(path)[stat.ST_MODE]
    os.chmod(path, mode | stat.S_IWRITE)


def all_subpaths(dir: Path, followlinks=False):
    for root, _dirs, files in os.walk(dir, followlinks=followlinks):
        root = Path(root)
        yield root
        for file in files:
            yield root / file


def rmtree(root: Path, *args, **kwargs):
    for path in all_subpaths(root, followlinks=False):
        if not os.path.islink(path):
            make_writable(path)
    shutil.rmtree(root, *args, **kwargs)
