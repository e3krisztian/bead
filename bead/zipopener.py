"""
Opening a zip file can be very expensive, if the zip file contains many files.
E.g. opening a zip file with >100000 files can easily take 15s in Python.
This does not mean reading any file or even looping over the zip directory.

For this reason this module provides a small LRU cache of open (for reading) zip files.

Actually having this module made the tests (which use only small files)
run ~4% faster (5.14 -> 4.94 = 0.2s faster).
"""

import atexit
from typing import Dict
from typing import Tuple
from zipfile import BadZipFile
from zipfile import ZipFile

from tracelog import TRACELOG

__all__ = ('BadZipFile', 'open', 'close_all')

FileName = str
LogicalTime = int


class OpenZipLRUCache:
    def __init__(self, max_size: int = 10):
        self.max_size: int = max_size
        self.open_zip_files: Dict[FileName, ZipFile] = {}
        self.access_times: Dict[FileName, LogicalTime] = {}
        self.access_count: LogicalTime = 0

    def open(self, filename):
        if filename not in self.open_zip_files:
            if len(self.open_zip_files) == self.max_size:
                self.close(self.least_recently_used_filename)
            self.open_zip_files[filename] = ZipFile(filename)

        self.access(filename)
        return self.open_zip_files[filename]

    @property
    def least_recently_used_filename(self):
        def access_time(filename_access_time: Tuple[FileName, LogicalTime]):
            _, access_time = filename_access_time
            return access_time
        least_recently_used_filename, _ = sorted(self.access_times.items(), key=access_time)[0]
        TRACELOG(
            f'{least_recently_used_filename}: {self.access_times[least_recently_used_filename]}')
        return least_recently_used_filename

    def access(self, filename):
        TRACELOG(f'{filename}: {self.access_count}')
        self.access_times[filename] = self.access_count
        self.access_count += 1

    def close(self, filename):
        TRACELOG(f'{filename}')
        self.open_zip_files[filename].close()
        del self.open_zip_files[filename]
        del self.access_times[filename]

    def close_all(self):
        for filename in list(self.open_zip_files.keys()):
            self.close(filename)


_cache = OpenZipLRUCache()

open = _cache.open
close_all = _cache.close_all


def _cleanup():
    TRACELOG(vars(_cache))
    close_all()


atexit.register(_cleanup)
