from typing import Iterable
from typing import Iterator

from . import tech
from .bead import Archive
from .bead import Bead
from .box_query import QueryCondition
from .exceptions import InvalidArchive
from .ziparchive import ZipArchive

Path = tech.fs.Path


# Filesystem-specific condition checking for beads
_CHECKERS = {
    QueryCondition.BEAD_NAME: lambda name: lambda bead: bead.name == name,
    QueryCondition.KIND: lambda kind: lambda bead: bead.kind == kind,
    QueryCondition.CONTENT_ID: lambda content_id: lambda bead: bead.content_id == content_id,
    QueryCondition.AT_TIME: lambda timestamp: lambda bead: bead.freeze_time == timestamp,
    QueryCondition.NEWER_THAN: lambda timestamp: lambda bead: bead.freeze_time > timestamp,
    QueryCondition.OLDER_THAN: lambda timestamp: lambda bead: bead.freeze_time < timestamp,
    QueryCondition.AT_OR_NEWER: lambda timestamp: lambda bead: bead.freeze_time >= timestamp,
    QueryCondition.AT_OR_OLDER: lambda timestamp: lambda bead: bead.freeze_time <= timestamp,
}


def compile_conditions(conditions):
    '''
    Compile list of (check-type, check-param)-s into a match function.
    '''
    checkers = [_CHECKERS[check_type](check_param) for check_type, check_param in conditions]

    def match(bead):
        for check in checkers:
            if not check(bead):
                return False
        return True
    return match


class RawFilesystemResolver:
    """
    Filesystem-based bead resolver with lazy caching.
    """

    def __init__(self, box_directory: Path):
        self.box_directory = Path(box_directory)
        self._bead_cache = {}  # (name, content_id) -> Bead
        self._path_cache = {}  # (name, content_id) -> Path

    def _cache_bead_and_path(self, bead: Bead, path: Path) -> None:
        """Cache both bead and path for given bead."""
        key = (bead.name, bead.content_id)
        self._bead_cache[key] = bead
        self._path_cache[key] = path

    def _glob_bead_files(self, bead_name: str = '') -> Iterator[Path]:
        """Glob for bead files in box directory, optionally filtered by bead name."""
        if bead_name:
            pattern = bead_name + '_????????T????????????[-+]????.zip'
        else:
            pattern = '*.zip'
        return self.box_directory.glob(pattern)

    def get_beads(self, conditions, box_name: str) -> list[Bead]:
        """Retrieve beads matching conditions by scanning filesystem."""
        match = compile_conditions(conditions)

        bead_names = {
            value
            for tag, value in conditions
            if tag == QueryCondition.BEAD_NAME}
        if bead_names:
            if len(bead_names) > 1:
                return []
            paths = self._glob_bead_files(bead_names.pop())
        else:
            paths = self._glob_bead_files()

        beads = []
        for archive in self._archives_from(paths, box_name):
            if match(archive):
                bead = self._bead_from_archive(archive)
                beads.append(bead)
        return beads

    def _archives_from(self, paths: Iterable[Path], box_name: str) -> Iterator[Archive]:
        for path in paths:
            try:
                archive = ZipArchive(path, box_name)
                # Cache the archive as we process it
                bead = self._bead_from_archive(archive)
                self._cache_bead_and_path(bead, path)
            except InvalidArchive:
                # TODO: log/report problem
                pass
            else:
                yield archive

    def _bead_from_archive(self, archive: Archive) -> Bead:
        """Create a Bead instance from Archive metadata."""
        bead = Bead()
        bead.kind = archive.kind
        bead.name = archive.name
        bead.inputs = archive.inputs
        bead.content_id = archive.content_id
        bead.freeze_time_str = archive.freeze_time_str
        bead.box_name = archive.box_name
        return bead

    def get_file_path(self, name: str, content_id: str) -> Path:
        """Get file path for bead by name and content_id."""
        key = (name, content_id)

        # Check cache first
        if key in self._path_cache:
            return self._path_cache[key]

        # Search filesystem and cache result
        for path in self._glob_bead_files(name):
            try:
                archive = ZipArchive(path, box_name='')
                if archive.name == name and archive.content_id == content_id:
                    # Cache the successful lookup
                    bead = self._bead_from_archive(archive)
                    self._cache_bead_and_path(bead, path)
                    return path
            except InvalidArchive:
                continue

        raise LookupError(f"Bead not found: name='{name}', content_id='{content_id}'")

    def add_archive_file(self, archive_path: Path) -> None:
        """Add archive file to resolver cache."""
        try:
            archive = ZipArchive(archive_path, box_name='')
            bead = self._bead_from_archive(archive)
            # Cache the new bead and its path
            self._cache_bead_and_path(bead, archive_path)
        except InvalidArchive:
            # Skip invalid archives
            pass
