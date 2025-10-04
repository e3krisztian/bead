# ruff: noqa

"""Infrastructure utilities for the bead system.

This package contains low-level utilities that abstract platform and system
concerns, providing safe and convenient APIs for common infrastructure needs.

Modules:
    fs: Filesystem operations (read/write files, directories, permissions)
    sqlite: SQLite database connection handling with automatic resource cleanup
    timestamp: Time and date parsing, formatting, and timezone handling
    securehash: Content hashing for data integrity
    persistence: JSON serialization and deserialization
    identifier: UUID generation for unique identifiers

All modules in this package are designed to be infrastructure-level abstractions
that hide platform-specific details and provide consistent, safe interfaces.
"""

from . import fs
from . import identifier
from . import persistence
from . import securehash
from . import timestamp
