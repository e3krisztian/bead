'''
Bead storage and retrieval system.

## Overview

A Box manages a directory of bead archives (.zip files), providing fast search and
retrieval through an SQLite index. Boxes are a convenience feature - beads can be
stored and used directly as files, but boxes enable efficient discovery and sharing.

## Architecture

The module defines three key types that work together:

- **Bead**: Lightweight metadata object (name, content_id, kind, freeze_time, inputs)
  returned from index queries. Fast to create and search.

- **Archive**: Full archive object with file extraction capabilities. Created on-demand
  from zip files via Box.resolve(). Heavier weight, only created when needed.

- **Box**: Storage manager that maintains a directory of archives and a SQLite index.
  Provides search API and resolution from Bead to Archive.

## Design Decisions

### Index as Source of Truth

Box treats the SQLite index as authoritative. If a bead is not in the index, it
doesn't exist from Box's perspective. The index must be kept in sync via:
- Automatic: `store()` adds new beads to index
- Manual: `sync()` discovers new/deleted files

### No Archive Caching

Box does not cache Archive objects. The index provides metadata (Bead instances),
and Archives are created on-demand via `resolve()` when file access is needed.
This keeps memory usage low and ensures consistency with filesystem.

### Search then Resolve Pattern

Typical workflow:
```python
# Search returns lightweight Beads
beads = box.search().by_kind("model").newer_than(timestamp).all()

# Resolve individual Beads to Archives only when needed
for bead in beads:
    archive = box.resolve(bead)
    archive.extract_dir('code', destination)
```

This two-phase approach optimizes for the common case where you search many beads
but only access a few.

## Use Cases

Boxes can be used to:
- **Share computations** (beads) when the box is on a shared drive (e.g. NFS, sshfs)
- **Store separate computation branches** (e.g. versions released to the public)
- **Hide sensitive computations** by splitting storage according to access level
  (naive access control, but can work)

## Key Classes

- **Box**: Main class for storage and retrieval
- **BeadSearch**: Abstract base class for fluent search API
- **BoxSearch**: Search within a single box
- **MultiBoxSearch**: Search across multiple boxes
'''

from abc import ABC
from abc import abstractmethod

from .bead import Archive
from .bead import Bead
from .box_index import BoxIndex
from .box_query import QueryCondition
from .exceptions import BoxError
from .exceptions import InvalidArchive
from .infra.fs import Path
from .infra.timestamp import time_from_timestamp
from .ziparchive import ZipArchive


ARCHIVE_COMMENT = '''
This file is a BEAD zip archive.

It is a normal zip file that stores a discrete computation of the form

    output = code(*inputs)

The archive contains

- inputs as part of metadata file: references (content_id) to other BEADs
- code   as files
- output as files
- extra metadata to support
  - linking different versions of the same computation
  - determining the newest version
  - reproducing multi-BEAD computation sequences built by a distributed team

There {is,will be,was} more info about BEADs at

- https://unknot.io
- https://github.com/ceumicrodata/bead
- https://github.com/e3krisztian/bead

----

'''


class BeadSearch(ABC):
    """
    Abstract base class for searching beads with a fluent builder pattern.
    """

    @abstractmethod
    def by_name(self, name):
        """Filter by bead name."""
        return self

    @abstractmethod
    def by_kind(self, kind):
        """Filter by bead kind."""
        return self

    @abstractmethod
    def by_content_id(self, content_id):
        """Filter by content ID."""
        return self

    @abstractmethod
    def at_time(self, timestamp):
        """Exact timestamp match."""
        return self

    @abstractmethod
    def newer_than(self, timestamp):
        """Timestamp > given value."""
        return self

    @abstractmethod
    def older_than(self, timestamp):
        """Timestamp < given value."""
        return self

    @abstractmethod
    def at_or_newer(self, timestamp):
        """Timestamp >= given value."""
        return self

    @abstractmethod
    def at_or_older(self, timestamp):
        """Timestamp <= given value."""
        return self

    @abstractmethod
    def unique(self):
        """Keep one instance by content_id."""
        return self

    @abstractmethod
    def first(self) -> Bead:
        """Return first bead found or raise LookupError if none found."""
        pass

    @abstractmethod
    def oldest(self) -> Bead:
        """Return oldest (by timestamp) matching bead or raise LookupError if none found."""
        pass

    @abstractmethod
    def newest(self) -> Bead:
        """Return newest (by timestamp) matching bead or raise LookupError if none found."""
        pass

    @abstractmethod
    def newer(self, n: int = 1) -> Bead:
        """Return nth newest bead (0=oldest, 1=2nd oldest, etc.) or raise LookupError if less items found."""
        pass

    @abstractmethod
    def older(self, n: int = 1) -> Bead:
        """Return nth oldest bead (0=newest, 1=2nd newest, etc.) or raise LookupError if less items found."""
        pass

    @abstractmethod
    def all(self) -> list[Bead]:
        """Return list of all matching beads."""
        pass


class BaseSearch(BeadSearch):
    """
    Base implementation of BeadSearch with common functionality.
    """

    def __init__(self):
        self.conditions = []
        self._unique_filter = False

    def by_name(self, name: str):
        if not name:
            raise ValueError("Name cannot be empty")
        self.conditions.append((QueryCondition.BEAD_NAME, name))
        return self

    def by_kind(self, kind: str):
        if not kind:
            raise ValueError("Kind cannot be empty")
        self.conditions.append((QueryCondition.KIND, kind))
        return self

    def by_content_id(self, content_id: str):
        if not content_id:
            raise ValueError("Content ID cannot be empty")
        self.conditions.append((QueryCondition.CONTENT_ID, content_id))
        return self

    def at_time(self, timestamp):
        if isinstance(timestamp, str):
            timestamp = time_from_timestamp(timestamp)
        self.conditions.append((QueryCondition.AT_TIME, timestamp))
        return self

    def newer_than(self, timestamp):
        if isinstance(timestamp, str):
            timestamp = time_from_timestamp(timestamp)
        self.conditions.append((QueryCondition.NEWER_THAN, timestamp))
        return self

    def older_than(self, timestamp):
        if isinstance(timestamp, str):
            timestamp = time_from_timestamp(timestamp)
        self.conditions.append((QueryCondition.OLDER_THAN, timestamp))
        return self

    def at_or_newer(self, timestamp):
        if isinstance(timestamp, str):
            timestamp = time_from_timestamp(timestamp)
        self.conditions.append((QueryCondition.AT_OR_NEWER, timestamp))
        return self

    def at_or_older(self, timestamp):
        if isinstance(timestamp, str):
            timestamp = time_from_timestamp(timestamp)
        self.conditions.append((QueryCondition.AT_OR_OLDER, timestamp))
        return self

    def unique(self):
        self._unique_filter = True
        return self

    def _apply_unique_filter(self, beads: list[Bead]) -> list[Bead]:
        if not self._unique_filter:
            return beads
        seen_content_ids = set()
        unique_beads = []
        for bead in beads:
            if bead.content_id not in seen_content_ids:
                seen_content_ids.add(bead.content_id)
                unique_beads.append(bead)
        return unique_beads

    def first(self) -> Bead:
        beads = self._get_beads()
        if not beads:
            raise LookupError("No beads found")
        return beads[0]

    def oldest(self) -> Bead:
        beads = self._get_beads()
        if not beads:
            raise LookupError("No beads found")
        return min(beads, key=lambda b: b.freeze_time)

    def newest(self) -> Bead:
        beads = self._get_beads()
        if not beads:
            raise LookupError("No beads found")
        return max(beads, key=lambda b: b.freeze_time)

    def newer(self, n: int = 1) -> Bead:
        beads = self._get_beads()
        if not beads:
            raise LookupError("No beads found")
        sorted_beads = sorted(beads, key=lambda b: b.freeze_time)
        if n >= len(sorted_beads):
            raise LookupError(f"Not enough beads found (requested index {n}, found {len(sorted_beads)})")
        return sorted_beads[n]

    def older(self, n: int = 1) -> Bead:
        beads = self._get_beads()
        if not beads:
            raise LookupError("No beads found")
        sorted_beads = sorted(beads, key=lambda b: b.freeze_time, reverse=True)
        if n >= len(sorted_beads):
            raise LookupError(f"Not enough beads found (requested index {n}, found {len(sorted_beads)})")
        return sorted_beads[n]

    def all(self) -> list[Bead]:
        return self._get_beads()

    @abstractmethod
    def _get_beads(self) -> list[Bead]:
        """Subclasses must implement this method to retrieve beads."""
        pass


class BoxSearch(BaseSearch):
    """
    Concrete implementation of BeadSearch for boxes.
    """

    def __init__(self, box):
        super().__init__()
        self.box = box

    def _get_beads(self) -> list[Bead]:
        beads = self.box.get_beads(self.conditions)
        return self._apply_unique_filter(beads)


class MultiBoxSearch(BaseSearch):
    """
    Search across multiple boxes.
    """

    def __init__(self, boxes):
        super().__init__()
        self.boxes = boxes

    def _get_beads(self) -> list[Bead]:
        all_beads = []
        for box in self.boxes:
            beads = box.get_beads(self.conditions)
            all_beads.extend(beads)

        return self._apply_unique_filter(all_beads)

    def first(self) -> Bead:
        for box in self.boxes:
            try:
                beads = box.get_beads(self.conditions)
                if beads:
                    return beads[0]
            except (InvalidArchive, IOError, OSError):
                continue
        raise LookupError("No beads found")


def search(boxes) -> BeadSearch:
    """
    Search across multiple boxes.
    """
    return MultiBoxSearch(boxes)


def resolve(boxes, bead: Bead) -> Archive:
    """
    Locate an extractable Archive for bead.

    Finds the appropriate box by matching bead.box_name and resolves the bead in that box.

    Args:
        boxes: List of Box instances to search
        bead: Bead instance to resolve

    Returns:
        Archive instance corresponding to the bead

    Raises:
        LookupError: If no box with matching name is found
        ValueError: If the bead cannot be resolved in the found box
    """
    for box in boxes:
        if box.name == bead.box_name:
            return box.resolve(bead)

    raise LookupError(f"Could not find box '{bead.box_name}' to resolve bead '{bead.name}'")


class Box:
    """
    Store Beads.
    """
    
    def __init__(self, name: str, location: Path, index_file_path: Path, enabled: bool = True):
        self.name = name
        self.location = location
        self.enabled = enabled
        self.index = BoxIndex(self.name, self.directory, Path(index_file_path))

    @property
    def directory(self):
        '''
        Location as a Path.

        Valid only for local boxes.
        '''
        return Path(self.location)

    def all_beads(self) -> list[Bead]:
        '''
        List of all beads in this Box.

        This is a list of Bead-s, not extractable Archives - intended for fast analytics.
        '''
        return self.get_beads([])

    def get_beads(self, conditions) -> list[Bead]:
        '''
        Retrieve matching beads.
        '''
        return self.index.get_beads(conditions)

    def resolve(self, bead: Bead) -> Archive:
        '''
        Resolve a Bead instance to its corresponding Archive.
        '''
        if bead.box_name != self.name:
            raise ValueError(f"Bead box_name '{bead.box_name}' does not match this box '{self.name}'")

        file_path = self.index.get_file_path(bead.name, bead.content_id)
        if not file_path.exists():
            raise LookupError(f"Archive file not found: {file_path}")
            
        archive = ZipArchive(file_path, self.name)
        self._validate_archive_matches_bead(archive, bead)
        return archive
    
    def _validate_archive_matches_bead(self, archive: Archive, bead: Bead):
        '''Validate that resolved Archive matches input Bead.'''
        if (archive.name != bead.name or
            archive.content_id != bead.content_id or
            archive.box_name != bead.box_name):
            raise ValueError(
                "Resolved Archive does not match Bead: "
                + f"Archive({archive.name}, {archive.content_id}, {archive.box_name}) != "
                + f"Bead({bead.name}, {bead.content_id}, {bead.box_name})")

    def store(self, workspace, freeze_time) -> Path:
        '''Store workspace as bead archive.'''
        if not self.directory.exists():
            raise BoxError(f'Box "{self.name}": directory {self.directory} does not exist')
        if not self.directory.is_dir():
            raise BoxError(f'Box "{self.name}": {self.directory} is not a directory')
        
        zipfilename = self.directory / f'{workspace.name}_{freeze_time}.zip'
        workspace.pack(zipfilename, freeze_time=freeze_time, comment=ARCHIVE_COMMENT)

        # Add to index
        self.index.add_file(zipfilename)
        
        return zipfilename

    def search(self) -> BeadSearch:
        """
        Return a BoxSearch instance for fluent search operations.
        """
        return BoxSearch(self)
