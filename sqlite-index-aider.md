# Requirements Document: SQLite-Based Box Index

Change `Box` and `BeadSearch` implementation to use and return `Bead` instances, not `Archive` instances as query results.
`Box` should provide a `.resolve(Bead) -> Archive` method, which can be used to map `BeadSearch` results (`Bead`s) to `Archive`s.
`Bead`'s (`.box_name`, `.name`, `.content_id`) property-tuple should be enough to properly resolve an `Archive`.
`Box` should maintain an internal map to back this resolution, and not parse up files again.
Later this internal map will be directly supported and replaced by an sqlite database based index.
`Box.resolve(bead)` should validate that the resolved `Archive` matches with the input `bead`.
This validation is a safety measure, that ensures, that when later we use an index, its content are in sync with the file system.

## Overview
Implement an SQLite-based index for bead storage and retrieval used from `Box`.

## Functional Requirements

### FR1: SQLite Index Module
- **FR1.1**: Create dedicated `bead/box_index.py` module
- **FR1.2**: No ORM dependency - use raw SQLite3
- **FR1.3**: Index is authoritative - anything not in index does not exist

### FR2: Index Operations
- **FR2.1**: `rebuild()` - Enumerate all files in box directory and rebuild index from scratch
- **FR2.2**: `sync()` - Discover new files to add to index
- **FR2.3**: `add_bead()` - Add single bead to index when stored
- **FR2.4**: `remove_bead()` - Remove bead from index when file deleted
- **FR2.5**: `compile_conditions()` - Build an SQL query and parameters from a list of `QueryCondition`-s
- **FR2.6**: `query()` - Run a query against the index, returning a list of (file_path, Bead) pairs using `compile_conditions` to translate the query
- **FR2.7**: Manual removal only - no automatic cleanup

### FR3: Failure Handling
- **FR3.1**: Handle missing index - auto-rebuild on first access
- **FR3.2**: Handle corrupted index - detect and rebuild
- **FR3.3**: Concurrent access protection for NFS/SSHFS environments
- **FR3.4**: Graceful degradation when index operations fail

### FR4: Search Implementation
- **FR4.1**: Provide `SQLiteBeadSearch` class implementing `BeadSearch` interface
- **FR4.2**: All existing search operations must work (by_name, by_kind, by_content_id, time filters, etc.)
- **FR4.3**: Performance improvement over current filesystem-based filtering

### FR5: Name Resolution
- **FR5.1**: Resolve beads by name ONLY when name comes from user input
- **FR5.2**: No translation between local names and "real" bead names
- **FR5.3**: Direct name → bead lookup via SQLite queries
- **FR5.4**: freeze_name is some immutable name - it is not used for name resolution
- **FR5.5**: name field is used for name resolution/matching, the name field is derived from the file name

### FR6: Input Tracking
- **FR6.1**: Store input specifications for each bead in the index
- **FR6.2**: Support querying beads by their input dependencies
- **FR6.3**: Enable dependency graph construction from index data
- **FR6.4**: Maintain referential integrity between beads and their inputs

## Data Model

Store the index with the name `.index.sqlite` under `Box.directory`.

### SQLite Schema

#### Beads Table
```sql
CREATE TABLE beads (
    bead_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    content_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    freeze_name TEXT NOT NULL,
    freeze_time_str TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_mtime REAL NOT NULL,
    UNIQUE(file_path),
    UNIQUE(name, content_id)
);

CREATE INDEX idx_beads_name ON beads(name);
CREATE INDEX idx_beads_content_id ON beads(content_id);
CREATE INDEX idx_beads_kind ON beads(kind);
CREATE INDEX idx_beads_freeze_time ON beads(freeze_time_str);
```

#### Inputs Table
```sql
CREATE TABLE inputs (
    input_id INTEGER PRIMARY KEY,
    bead_id INTEGER NOT NULL,
    input_name TEXT NOT NULL,
    input_kind TEXT NOT NULL,
    input_content_id TEXT NOT NULL,
    input_freeze_time_str TEXT NOT NULL,
    FOREIGN KEY (bead_id) REFERENCES beads(bead_id) ON DELETE CASCADE,
    UNIQUE(bead_id, input_name)
);

CREATE INDEX idx_inputs_bead_id ON inputs(bead_id);
CREATE INDEX idx_inputs_content_id ON inputs(input_content_id);
CREATE INDEX idx_inputs_kind ON inputs(input_kind);
CREATE INDEX idx_inputs_name ON inputs(input_name);
```

### Field Descriptions

#### Beads Table
- `bead_id`: Primary key, auto-increment
- `name`: Bead name derived from file name, used for name resolution
- `content_id`: Unique content identifier
- `kind`: Bead type/category
- `freeze_name`: Immutable name, not used for resolution
- `freeze_time_str`: ISO timestamp when bead was frozen
- `file_path`: Relative path to archive file from box directory
- `file_mtime`: File modification time for sync detection

#### Inputs Table
- `input_id`: Primary key, auto-increment
- `bead_id`: Foreign key to parent bead
- `input_name`: Name of the input as referenced by the bead
- `input_kind`: Kind of the input bead
- `input_content_id`: Content ID of the input bead
- `input_freeze_time_str`: Freeze time of the input bead

### Query Conditions

The existing `QueryCondition` enum from `bead/box.py` will be used unchanged:

```python
class QueryCondition(Enum):
    BEAD_NAME = auto()
    KIND = auto()
    CONTENT_ID = auto()
    AT_TIME = auto()
    NEWER_THAN = auto()
    OLDER_THAN = auto()
    AT_OR_NEWER = auto()
    AT_OR_OLDER = auto()
```

The `compile_conditions()` function in the index module will translate these enum values to appropriate SQL WHERE clauses and parameters.

## Success Criteria

- All existing functionality preserved
- Search performance improved by >50%
- Concurrent access works reliably on shared filesystems
- Input dependency queries execute efficiently
- Index rebuilds complete in reasonable time for large boxes
- Graceful handling of filesystem inconsistencies
