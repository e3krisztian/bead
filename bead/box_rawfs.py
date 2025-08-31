from typing import Iterable
from typing import Iterator

from . import tech
from .bead import Archive
from .bead import Bead
from .box import QueryCondition
from .box import compile_conditions
from .exceptions import InvalidArchive
from .ziparchive import ZipArchive

Path = tech.fs.Path


class RawFilesystemResolver:
    """
    Filesystem-based bead resolver with lazy caching.
    """
    
    def __init__(self, box_directory: Path):
        self.box_directory = Path(box_directory)
        self._bead_cache = {}  # (name, content_id) -> Bead
        self._path_cache = {}  # (name, content_id) -> Path
    
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
            glob = bead_names.pop() + '_????????T????????????[-+]????.zip'
        else:
            glob = '*'

        paths = self.box_directory.glob(glob)
        archives = self._archives_from(paths, box_name)
        beads = []
        for archive in archives:
            if match(archive):
                bead = self._bead_from_archive(archive)
                # Cache the bead we just created
                key = (bead.name, bead.content_id)
                self._bead_cache[key] = bead
                beads.append(bead)
        return beads

    def _archives_from(self, paths: Iterable[Path], box_name: str) -> Iterator[Archive]:
        for path in paths:
            try:
                archive = ZipArchive(path, box_name)
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
        glob = name + '_????????T????????????[-+]????.zip'
        for path in self.box_directory.glob(glob):
            try:
                archive = ZipArchive(path, box_name='')
                if archive.name == name and archive.content_id == content_id:
                    # Cache the successful lookup
                    self._path_cache[key] = path
                    return path
            except InvalidArchive:
                continue
        
        raise LookupError(f"Bead not found: name='{name}', content_id='{content_id}'")
    
    def add_archive_file(self, archive_path: Path) -> None:
        """Add archive file to resolver cache."""
        try:
            archive = ZipArchive(archive_path, box_name='')
            bead = self._bead_from_archive(archive)
            key = (bead.name, bead.content_id)
            
            # Cache the new bead and its path
            self._bead_cache[key] = bead
            self._path_cache[key] = archive_path
        except InvalidArchive:
            # Skip invalid archives
            pass
