# Requirements Document: SQLite-Based Box Index

## Overview
Implement an SQLite-based index for bead storage and retrieval to replace filesystem-based operations in the `Box` class. The index will provide fast queries, concurrent access protection, and dependency tracking while maintaining full backward compatibility.

## Functional Requirements

### FR1: API Design (Bead vs Archive)
- **FR1.1**: `Box` and `BeadSearch` work with `Bead` instances, not `Archive` instances as query results
- **FR1.2**: `Box.resolve(Bead) -> Archive` method maps `BeadSearch` results to extractable `Archive`s
- **FR1.3**: `Bead`'s `(box_name, name, content_id)` tuple is sufficient to resolve an `Archive`
- **FR1.4**: `Box.resolve(bead)` validates that resolved `Archive` matches input `bead`
- **FR1.5**: Validation ensures index content stays in sync with filesystem

### FR2: SQLite Index Module
- **FR2.1**: Create dedicated `bead/box_index.py` module
- **FR2.2**: No ORM dependency - use raw SQLite3
- **FR2.3**: Index is authoritative - anything not in index does not exist from Box perspective
- **FR2.4**: Index stored as `.index.sqlite` under `Box.directory`

### FR3: Index Operations
- **FR3.1**: `rebuild()` - Enumerate all files in box directory and rebuild index from scratch
- **FR3.2**: `sync()` - Discover new files to add to index
- **FR3.3**: `add_bead()` - Add single bead to index when stored
- **FR3.4**: `remove_bead()` - Remove bead from index when file deleted (manual only)
- **FR3.5**: `compile_conditions()` - Build SQL query and parameters from `QueryCondition` list
- **FR3.6**: `query()` - Run query against index, returning list of `Bead`s
- **FR3.7**: `get_file_path()` - Resolve `(name, content_id)` to file path for `Box.resolve()`
- **FR3.8**: Manual removal only - no automatic cleanup

### FR4: Failure Handling and Robustness
- **FR4.1**: Handle missing index - auto-rebuild on first access
- **FR4.2**: Handle corrupted index - detect and rebuild
- **FR4.3**: Graceful degradation when index operations fail
- **FR4.4**: Graceful degradation to filesystem-based solution when index unusable
- **FR4.5**: Work with read-only index database (skip updates)
- **FR4.6**: Work with read-only filesystem (if index exists)
- **FR4.7**: Fall back to non-index solution when index file cannot be created
- **FR4.8**: Recognize index mismatch against actual `Archive` and abort with message

### FR5: Search and Query Implementation
- **FR5.1**: All existing search operations work (by_name, by_kind, by_content_id, time filters, etc.)
- **FR5.2**: Performance improvement over current filesystem-based filtering
- **FR5.3**: Support all existing `QueryCondition` enum values
- **FR5.4**: Index queries work with read-only database

### FR6: Name Resolution
- **FR6.1**: Resolve beads by name ONLY when name comes from user input
- **FR6.2**: No translation between local names and "real" bead names
- **FR6.3**: Direct name → bead lookup via SQLite queries
- **FR6.4**: `freeze_name` is immutable - not used for name resolution
- **FR6.5**: `name` field used for name resolution/matching, derived from file name

### FR7: Input Dependency Tracking
- **FR7.1**: Store input specifications for each bead in the index
- **FR7.2**: Support querying beads by their input dependencies
- **FR7.3**: Enable dependency graph construction from index data
- **FR7.4**: Maintain referential integrity between beads and their inputs

### FR8: Cross-Platform and Network Support
- **FR8.1**: Work on Windows, MacOS, Linux
- **FR8.2**: Work on network filesystems (NFS/SSHFS)
- **FR8.3**: Concurrent access protection for network environments
- **FR8.4**: Handle access from multiple computers over network

## Data Model

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

#### Inputs Table
- `input_id`: Primary key, auto-increment
- `bead_id`: Foreign key to parent bead
- `input_name`: Name of the input as referenced by the bead
- `input_kind`: Kind of the input bead
- `input_content_id`: Content ID of the input bead
- `input_freeze_time_str`: Freeze time of the input bead

### Query Conditions

The existing `QueryCondition` enum from `bead/box.py`:

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

## Success Criteria

- All existing functionality preserved
- Search performance improved by >50%
- Concurrent access works reliably on shared filesystems
- Input dependency queries execute efficiently
- Index rebuilds complete in reasonable time for large boxes
- Graceful handling of filesystem inconsistencies
- Cross-platform compatibility (Windows, MacOS, Linux)
- Network filesystem support (NFS/SSHFS)
- Read-only database and filesystem support
- Automatic fallback to filesystem-based operations when needed

## Implementation Plan

### Phase 1: API Transition (Bead vs Archive) ✅ COMPLETED
**Goal**: Change Box and BeadSearch to work with Bead instances instead of Archive instances

**Completed Work**:
1. ✅ **Box.get_beads() method**: 
   - Renamed from `get_archives()` to `get_beads()`
   - Returns `list[Bead]` instead of `list[Archive]`
   - Uses existing filesystem-based implementation
   - Creates Bead instances from Archive metadata via `_bead_from_archive()`

2. ✅ **Box.resolve(Bead) → Archive method**:
   - Takes Bead instance and returns corresponding Archive
   - Uses `(box_name, name, content_id)` tuple for resolution
   - Validates that resolved Archive matches input Bead
   - Implemented by re-parsing archive file with filesystem globbing

3. ✅ **Updated Search classes**:
   - `BaseSearch` and `BoxSearch` work with new `Box.get_beads()` signature
   - All search methods (first, newest, oldest, etc.) return Beads
   - Fluent API unchanged for backward compatibility
   - `MultiBoxSearch` adapted for Bead instances across multiple boxes

### Phase 2: SQLite Index Implementation 📋 PLANNED
**Goal**: Create the SQLite-based index infrastructure

**Planned Work**:
1. **Create bead/box_index.py module**:
   - `BoxIndex` class managing SQLite database lifecycle
   - Schema creation with beads and inputs tables
   - Database file location: `{Box.directory}/.index.sqlite`
   - Add filesystem access checks (read-only detection)

2. **Implement core index operations**:
   - `rebuild()`: Scan box directory, parse all archives, rebuild index from scratch
   - `sync()`: Scan directory and add any files not already in index
   - `add_bead(archive_path)`: Add single bead when Box.store() creates new archive
   - `remove_bead(archive_path)`: Remove bead from index when file deleted
   - Graceful handling of invalid archives during indexing

3. **Implement query functionality**:
   - `compile_conditions(conditions)`: Convert QueryCondition list to SQL WHERE clause
   - `query(conditions)`: Execute SQL query and return list of Bead instances
   - `get_file_path()`: Resolve `(name, content_id)` to file path
   - Handle all existing QueryCondition enum values
   - Index queries work with read-only database

4. **Add error handling and robustness**:
   - Auto-create index on first access if missing
   - Detect corrupted index and trigger rebuild
   - Implement graceful degradation to filesystem-based operations
   - Add comprehensive error handling for network filesystems
   - Performance testing and optimization

### Phase 3: Integration and Optimization 📋 PLANNED
**Goal**: Replace filesystem operations with SQLite queries

**Planned Work**:
1. **Integrate BoxIndex into Box class**:
   - Initialize BoxIndex in Box constructor with fallback detection
   - Call `sync()` on Box initialization (if index available)
   - Call `add_bead()` in Box.store() method (if index available)
   - Add index availability checking

2. **Replace Box.get_beads() implementation**:
   - Remove filesystem globbing and Archive parsing
   - Use BoxIndex.query() to get Bead instances directly
   - Maintain same method signature and behavior
   - Fall back to filesystem operations if index unavailable

3. **Optimize Box.resolve() method**:
   - Use index to look up file_path from `(name, content_id)`
   - Create Archive directly from file path
   - Validate Archive matches Bead (FR4.8 requirement)
   - Fall back to filesystem globbing if index unavailable

4. **Input dependency support**:
   - Store input specifications in inputs table during add_bead()
   - Implement queries for dependency graph construction
   - Add methods to query beads by their input dependencies

### Phase 4: Testing and Validation 📋 PLANNED
**Goal**: Ensure reliability and performance

**Planned Work**:
1. **Comprehensive testing**:
   - All existing Box and BeadSearch functionality works unchanged
   - Performance benchmarks show >50% improvement
   - Concurrent access testing on shared filesystems
   - Index corruption and recovery scenarios
   - Read-only filesystem and database scenarios

2. **Cross-platform validation**:
   - Windows, MacOS, Linux compatibility
   - Network filesystem testing (NFS/SSHFS)
   - Concurrent access from multiple computers

3. **Migration and deployment**:
   - Existing boxes work immediately (auto-rebuild index)
   - No data migration required
   - Backward compatibility maintained
   - Performance monitoring and optimization

## Key Design Principles

- **No caching**: Index provides file paths, Archives created on-demand
- **Simple resolution**: `(name, content_id)` → file_path → Archive
- **Validation**: Every resolve() validates index vs filesystem consistency
- **Existing API preserved**: All current Box and BeadSearch methods work unchanged
- **Performance**: SQLite queries replace filesystem operations and Python filtering
- **Graceful degradation**: Always fall back to filesystem operations when index unavailable
- **Cross-platform**: Work reliably on all supported platforms and network filesystems
