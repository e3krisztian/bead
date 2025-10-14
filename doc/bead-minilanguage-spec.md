# Bead Reference Mini-Language Specification

## Overview

A concise, shell-friendly mini-language for specifying beads in command-line operations. The language supports referencing beads by name, constraining searches to specific boxes, and selecting versions by time or relative position.

## Grammar

```
BEAD_SPEC ::= FILE_PATH | QUALIFIED_NAME

FILE_PATH ::= (any path with separators or .zip extension)

QUALIFIED_NAME ::= [ BOX ":" ] NAME [ "@" TIME_EXPR ]

NAME ::= bead_name                       # autocomplete from all boxes
       | ""                              # empty = use context (input name)

BOX ::= box_name                         # restrict to specific box
      | (omitted)                        # omitted = all boxes

TIME_EXPR ::= PARTIAL_ISO_TIMESTAMP      # Partial ISO 8601 with separators
            | "latest"                   # newest (default when omitted)
            | RELATIVE

PARTIAL_ISO_TIMESTAMP ::= YYYY[-MM[-DD[THH[:MM[:SS[.NNNNNN[TIMEZONE]]]]]]]
    # Must use separators (- for date, : for time)
    # Can be arbitrarily truncated from right
    # Timezone: +HHMM, +HH:MM, -HHMM, -HH:MM, Z

RELATIVE ::= DIRECTION{1,4}              # Repeat 1-4 times for count
           | DIRECTION NUMBER            # Explicit number for larger counts

DIRECTION ::= "-" | "+" | "^"
```

## Valid Forms

| Form | Interpretation |
|------|----------------|
| `name` | name, all boxes, latest |
| `box:name` | name, specific box, latest |
| `name@time` | name, all boxes, time constrained |
| `box:name@time` | name, specific box, time constrained |
| `box:` | input name, specific box, latest |
| `@time` | input name, all boxes, time constrained |
| `box:@time` | input name, specific box, time constrained |

## Examples

### Basic Forms

```bash
# Name only - search all boxes for newest
bead edit hotel-dataset
bead input add foo model

# Box + name - specific box, newest
bead edit home:hotel-dataset
bead edit prod:model
bead input update foo backup:model       # uses INPUT_NAME if name empty
```

### Time Constraints - Partial ISO Timestamps

Timestamps are interpreted as "at or before end of specified period":

```bash
# Year precision
bead edit dataset@2023                   # at or before end of 2023

# Month precision
bead edit dataset@2024-06                # at or before end of June 2024

# Day precision
bead edit model@2024-03-15               # at or before end of March 15, 2024

# Hour precision
bead edit data@2024-03-15T14             # at or before end of 14:00 hour

# Minute precision
bead edit data@2024-03-15T14:30          # at or before 14:30:59.999999

# Second precision
bead edit data@2024-03-15T14:30:45       # at or before that exact second

# Microsecond precision
bead edit data@2024-03-15T14:30:45.123456

# With timezone
bead edit data@2024-03-15T14:30:45+0200
bead edit data@2024-03-15T14:30:45+00:00
bead edit data@2024-03-15T14:30:45Z
```

### Relative Versions

#### Shorthand (1-4 repetitions)

```bash
# Previous versions (-)
bead input update foo @-                 # 1 version back
bead input update foo @--                # 2 versions back
bead input update foo @---               # 3 versions back
bead input update foo @----              # 4 versions back

# Next versions (+)
bead input update foo @+                 # 1 version forward
bead input update foo @++                # 2 versions forward
bead input update foo @+++               # 3 versions forward
bead input update foo @++++              # 4 versions forward

# Git-style alias (^)
bead input update foo @^                 # 1 version back
bead input update foo @^^                # 2 versions back
bead input update foo @^^^               # 3 versions back
bead input update foo @^^^^              # 4 versions back
```

#### Explicit Numbers (for larger counts)

```bash
bead input update foo @-5                # 5 versions back
bead input update foo @-10               # 10 versions back
bead input update foo @+7                # 7 versions forward
bead input update foo @^12               # 12 versions back (git alias)
```

### Combined Constraints

```bash
# Box + name + time
bead edit home:hotel-dataset@2024-06     # from home box, June 2024 or earlier
bead edit prod:model@latest              # from prod box, explicit latest

# Context name + box + time
bead input update foo backup:@2024-03-15 # foo from backup, March 15 or earlier
bead input update foo prod:@---          # foo from prod, 3 versions back

# Context name + all boxes + time
bead input update foo @2024              # foo from any box, 2024 or earlier
bead input update foo @-                 # foo from any box, previous version
```

### Context-Based Forms

For `input add` and `input update` commands, the INPUT_NAME parameter provides the default name:

```bash
bead input add bar prod:                 # adds "bar" from prod box
bead input update foo @-                 # updates "foo", prev version, any box
bead input update foo home:@--           # updates "foo" from home, 2 versions back
```

### File Paths (Unchanged)

Direct file references bypass the mini-language:

```bash
bead edit /path/to/archive.zip
bead input add foo ./local/bead.zip
bead edit ../beads/hotel-dataset_20240315.zip
```

## Parsing Algorithm

```python
def parse_bead_spec(spec: str) -> BeadSpec:
    # Check for file path
    if '/' in spec or '\\' in spec or spec.endswith('.zip'):
        return FilePathSpec(spec)

    # Check for box:name separator
    if ':' in spec:
        box, rest = spec.split(':', 1)

        # Check for time separator
        if '@' in rest:
            name, time = rest.split('@', 1)
            return BeadSpec(name=name, box=box, time=time)
        else:
            # box:name (no time)
            return BeadSpec(name=rest, box=box, time="")

    # No box separator
    elif '@' in spec:
        # name@time (no box)
        name, time = spec.split('@', 1)
        return BeadSpec(name=name, box="", time=time)

    else:
        # Just name
        return BeadSpec(name=spec, box="", time="")

def parse_relative(s: str) -> int:
    """Parse relative version movement.

    Returns negative for backwards, positive for forwards.
    """
    direction = s[0]

    # Count repeated characters
    if all(c == direction for c in s):
        count = len(s)
    else:
        # Explicit number
        count = int(s[1:])

    # Convert ^ to -
    if direction == '^':
        direction = '-'

    return -count if direction == '-' else count
```

## Context Rules

### Empty Name
- **Valid for**: `input add` and `input update` commands
- **Uses**: INPUT_NAME parameter from command
- **Examples**: `box:`, `@time`, `box:@time`

### Empty/Omitted Box
- **Valid for**: All commands
- **Means**: Search all boxes
- **Examples**: `name`, `name@time`, `@time`

### Empty Time
- **Defaults to**: `latest` (newest version)

## Autocomplete Behavior

### Token without separators (e.g., `hotel-d`)
- Complete BOTH box names and bead names:
  - Box names with `:` suffix: `home:`, `prod:`
  - Bead names: `hotel-dataset`, `hotel-bookings`
- User can continue typing to narrow, or select box to scope to that box

### Token with `:` but before `@` (e.g., `home:hotel-d`)
- Complete bead names from specified box (or all boxes if box is empty)
- Query box(es): `SELECT DISTINCT name FROM beads WHERE name LIKE 'prefix%'`

### Token with `@` (e.g., `name@` or `box:name@`)
- Complete time expressions:
  - `latest`
  - `-`, `--`, `---`, `----`
  - `+`, `++`, `+++`, `++++`
  - `^`, `^^`, `^^^`, `^^^^`
  - Current year (e.g., `2024`)
  - Timestamp patterns based on existing beads in scope

## Interpretation Semantics

### Partial Timestamps
When a timestamp is truncated, it represents "at or before the end of that period":

- `2024` → at or before `2024-12-31T23:59:59.999999`
- `2024-03` → at or before end of March 2024
- `2024-03-15` → at or before end of that day (23:59:59.999999)
- `2024-03-15T14` → at or before end of that hour (14:59:59.999999)
- `2024-03-15T14:30:45` → at or before that exact second (14:30:45.999999)

### Relative Versions
Relative movements are applied to the current/loaded version of an input:

- `-` or `^` → Previous version (older timestamp)
- `+` → Next version (newer timestamp)
- Movement is along the version timeline for the same bead (by name/kind)

### Box Constraints
- **Specified box**: Only search in that box
- **Omitted box**: Search all enabled boxes
- Boxes are searched in order of definition in environment

## Design Rationale

### Name-First, Box Optional
The most common operation is selecting a bead by name from any box, so `name` alone works without any separator. When box scope is needed, the natural `box:name` syntax makes the relationship clear.

### `:` for Location, `@` for Time
Using different separators for different concepts improves readability:
- `:` groups box and name (location/identity)
- `@` specifies when (time constraint)

This mirrors familiar patterns like `user@host` and `namespace:resource`.

### Shorthand Relative Syntax
Repeating `-`, `+`, or `^` for small counts (1-4) provides visual feedback and is quick to type. Explicit numbers are available for larger movements.

### Git-Style `^` Alias
Supporting `^` as an alias for `-` accommodates muscle memory from git users, where `^` indicates "parent" or "previous".

### Separator Requirement in Timestamps
Requiring separators (`-` and `:`) in ISO timestamps avoids ambiguity and simplifies parsing without needing complex date parsing logic.

### Partial Timestamp Interpretation
Interpreting partial timestamps as "end of period" aligns with intuitive expectation: "give me the newest from 2024" means "from all of 2024, not just January 1st".

### Autocomplete Discovery
When no separator is present, completing both box names (with `:`) and bead names simultaneously allows users to discover the box-scoping feature naturally while still making name-only completion fast and direct.
