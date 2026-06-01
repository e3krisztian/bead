# Changelog

## Unreleased (since v0.9)

### `bead branch`

Assign a fresh kind to the current workspace, starting a new incompatible bead series under the same name. Consumers trying to update from the old series get a kind mismatch error, preventing accidental upgrades to incompatible data. Previously only achievable by renaming the workspace directory.

### `bead init`

Initialize the current directory as a bead workspace. Counterpart to `bead new`, which creates a new subdirectory instead.



### Bead reference mini-language

Commands that take a bead argument now accept a structured spec: `[[box:]name][@time]`.

- **Box prefix**: `home:hotel-dataset` restricts search to a named box
- **Time suffix**: selects a specific version
  - Absolute: `@2024`, `@2024-06`, `@2024-06-15`, `@2024-06-15T14:30:00`
  - Partial timestamps match to end of unit: `@2024-06` matches the latest bead up to end of June
  - Relative offset: `@-` (previous), `@--` (two back), `@-5`, `@+` (next), `@+5`; `^` / `^^` are aliases for `-` / `--`
  - Default: latest version if `@` is omitted
- **File path**: a path containing `/`, `\`, or ending in `.zip` is treated as a direct archive reference
- **Context-aware**: in `input update` and `input load`, name and box can be omitted and are inferred from the input

### Shell tab-completion

New command `bead completion [bash|zsh|fish]` prints shell setup instructions (shell is auto-detected if omitted). After sourcing, tab-completion is available for all commands:

- Completes box names, bead names, and timestamps drawn from actual box contents
- Handles `@` and `:` separators that shells normally split on
- Input name completions include source bead, date, and load status

### Input update safety

`bead input update` gained strict matching and explicit override flags:

- By default, updates match by both name and kind; a kind or name change is an error
- `--no-kind`: match by name only (allows kind changes)
- `--no-name`: match by kind only (allows name changes)
- `--allow-downgrade`: permit updating to an older version
- `--force`: bypass name, kind, and downgrade checks
- When a candidate is found but kinds differ, the error message says so and suggests `--no-kind`
- Explicit time specs (e.g. `@2024`) implicitly allow downgrade because the intent is clear

### `input map` command

`bead input map <INPUT-NAME> [BEAD-SPEC]` changes which bead an input is mapped to without re-adding it. The mapping is stored in the box index and used when generating the dependency graph, so the graph correctly reflects renames.

### `bead input load` name-mismatch warning

If the bead is found by content ID but stored under a different name than expected, `load` warns rather than silently loading or failing.

### `bead status` improvements

- Shows each input's load status and source bead
- Flags inputs that have an update available (`**UPDATE AVAILABLE**`)
- Reports if a bead is found under a different name than recorded
- `-v`/`--verbose` shows kind and content ID

### Box enable/disable

`bead box enable <name>` / `bead box disable <name>` — a disabled box is excluded from all searches without being forgotten.

### Box index overhaul

The SQLite box index is now the primary search path:

- Schema versioning (currently v4) for safe upgrades
- Freeze times stored as UTC Unix microseconds for correct cross-timezone ordering
- Indexes on `freeze_time_unix`, `(kind, name)`, and `content_id` for fast queries
- Progress reporting with `tqdm` during indexing
- Corrupted indexes are detected at open time and prompt `bead box reindex`
- `bead box index` / `bead box reindex` auto-select the single box if only one is configured
- Indexes are refreshed at the start of search commands so new beads are visible without manual re-indexing

### `bead graph` (renamed from `bead web`)

The `web` command is now `graph`. It works as a pipeline of sub-commands:

```
bead graph [load <file>] [/ sources .. sinks /] [color] [heads] [save|dot|png|svg|view <file>]
```

- `load` skips box scanning and uses a previously saved graph file
- `/ sources .. sinks /` filters to the subgraph reachable between named beads
- `color` assigns freshness colours: green (current), orange (outdated), grey (superseded), red (phantom)
- `heads` keeps only the latest version per cluster plus older beads still referenced by outdated computations
- `save` / `dot` / `png` / `svg` / `view` output the graph in various formats

### XDG / platformdirs compliance

Config is now stored in platform-appropriate directories:

- Linux: `~/.config/bead/boxes.json`
- macOS: `~/Library/Application Support/bead/`
- Windows: `%APPDATA%\bead\`

On first run after upgrading, the old config is automatically migrated.

### Error reporting

- Duplicate `ERROR:` prefix removed from error messages
- Invalid time expressions produce a user-friendly message instead of a traceback
- Kind mismatch during update names the conflicting kinds and suggests the fix
- Box index errors include the affected file path and actionable recovery steps
- Optional debug logging via `BEAD_LOG_DIR` environment variable
