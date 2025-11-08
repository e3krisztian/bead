"""
Autocomplete support for bead specification mini-language.

Provides context-aware tab-completion for bead specs with syntax: [[box:]name][@time]

Completion behavior:
  - No separators (e.g., "hotel-d"):
      Complete both box names (with : suffix) and bead names
  - After : but before @ (e.g., "home:hotel-d"):
      Complete bead names from specified box
  - After @ (e.g., "hotel-dataset@"):
      Complete time expressions (latest) and actual timestamps from matching beads

Strategy:
  Bash splits on COMP_WORDBREAKS, which includes ':' and '@'.
  To handle this, we:
  1. Parse COMP_LINE and COMP_POINT from the environment to reconstruct the full spec
  2. Complete based on the full spec
  3. Return the full parts (bash recombines them)

  Examples:
    - "b" -> ["box:"]  (full box name with separator)
    - "box:na" -> ["name"] (full bead name)
    - "box:name@la" -> ["latest"] (full time expression)
"""

import os
from dataclasses import dataclass

from bead.box_query import QueryCondition
from bead.workspace import Workspace

from .bead_spec import BeadSpec
from .environment import get_environment


SHELL_COMMAND_SEPARATORS = ' |><&;()'


@dataclass
class ShellContext:
    """Context information for shell completion.

    Encapsulates shell-specific parsing and environment variable handling.
    """
    shell_type: str  # 'bash', 'zsh', or 'fish'
    full_spec: str   # Reconstructed bead spec (e.g., "box:name@la")
    prefix: str      # Original prefix from argcomplete

    @classmethod
    def from_environment(cls, prefix):
        """Create ShellContext by parsing environment variables.

        Args:
            prefix: The prefix parameter from argcomplete

        Returns:
            ShellContext with shell type and reconstructed spec
        """
        # Detect which shell we're running under
        shell_type = os.environ.get('_ARGCOMPLETE_SHELL', 'bash')

        # For ZSH, prefix is already the complete spec (no splitting on : and @)
        if shell_type == 'zsh':
            return cls(shell_type=shell_type, full_spec=prefix, prefix=prefix)

        # For BASH and others, reconstruct from COMP_LINE since prefix is split
        comp_line = os.environ.get('COMP_LINE', '')
        comp_point = os.environ.get('COMP_POINT', '')

        if not comp_line or not comp_point:
            # Can't reconstruct, use prefix as-is
            return cls(shell_type=shell_type, full_spec=prefix, prefix=prefix)

        try:
            comp_point = int(comp_point)
        except (ValueError, TypeError):
            return cls(shell_type=shell_type, full_spec=prefix, prefix=prefix)

        if comp_point < 0 or comp_point > len(comp_line):
            # Can't reconstruct, use prefix as-is
            return cls(shell_type=shell_type, full_spec=prefix, prefix=prefix)

        # Get the part of COMP_LINE up to the cursor
        before_cursor = comp_line[:comp_point]

        # Find the start of the bead spec - look backwards for shell separators
        spec_start = len(before_cursor)
        for i in range(len(before_cursor) - 1, -1, -1):
            if before_cursor[i] in SHELL_COMMAND_SEPARATORS:
                spec_start = i + 1
                break
            if i == 0:
                spec_start = 0
                break

        # Extract the part being typed - this includes : and @ since they're part of the bead spec
        full_spec = before_cursor[spec_start:]

        return cls(shell_type=shell_type, full_spec=full_spec, prefix=prefix)


@dataclass
class SpecContext:
    """Parsed components of a bead spec.

    Pre-computes spec parts to avoid repeated string parsing in handlers.
    """
    full_spec: str
    box_part: str | None    # Part before ':' (None if no colon)
    name_part: str | None   # Part between ':' and '@' (or after ':' if no '@')
    time_part: str | None   # Part after '@' (None if no @)
    has_colon: bool
    has_at: bool

    @classmethod
    def parse(cls, full_spec):
        """Parse a bead spec into its components.

        Args:
            full_spec: Full spec string (e.g., "box:name@la")

        Returns:
            SpecContext with parsed components
        """
        has_colon = ':' in full_spec
        has_at = '@' in full_spec

        box_part = None
        name_part = None
        time_part = None

        if has_at:
            # Split on @ first
            spec_before_at, time_part = full_spec.rsplit('@', 1)
            if has_colon:
                # box:name@time
                box_part, name_part = spec_before_at.rsplit(':', 1)
            else:
                # name@time
                name_part = spec_before_at
        elif has_colon:
            # box:name (no time)
            box_part, name_part = full_spec.rsplit(':', 1)
        else:
            # Just name (no box, no time)
            name_part = full_spec

        return cls(
            full_spec=full_spec,
            box_part=box_part,
            name_part=name_part,
            time_part=time_part,
            has_colon=has_colon,
            has_at=has_at
        )

    def get_name_spec(self):
        """Get box:name or just name (without @ part).

        Returns the spec prefix before the time component, suitable for
        constructing completions like 'box:name@timestamp'.

        Returns:
            String like 'box:name' or 'name', excluding the time part
        """
        if self.box_part:
            return f"{self.box_part}:{self.name_part or ''}"
        return self.name_part or ''


def _debug(msg):
    """Write debug message to log file if BEAD_COMPLETION_DEBUG is set."""
    debug_log = os.environ.get('BEAD_COMPLETION_DEBUG')
    if debug_log:
        with open(debug_log, 'a') as f:
            f.write(msg + '\n')


def _default_format_bead_name(box, bead):
    """Default formatter that returns just the bead name."""
    return bead.name


def _get_boxes_for_spec(spec, env):
    """Get boxes to search based on spec.box.

    Args:
        spec: Parsed BeadSpec
        env: Environment with boxes

    Returns:
        List of Box objects to search
    """
    if spec.box:
        return [env.get_box(spec.box)]
    return env.get_boxes()


def after_arg(required_attr, inner_completer, parser=None):
    """
    Wrapper that conditionally activates completers based on parser state.

    Solves the problem where argcomplete shows completions for all optional
    positional arguments simultaneously (e.g., `cmd [arg1 [arg2]]`), even when
    they should complete in order. By suppressing arg2's completer until arg1
    is provided, this ensures proper sequential completion.

    Only calls inner_completer if the required argument has been explicitly
    provided (not the default value).

    Args:
        required_attr: Name of the argument attribute to check
        inner_completer: The completer function to wrap
        parser: Optional parser to get default value from

    Returns:
        Wrapped completer function
    """
    def wrapper(prefix, parsed_args, **kwargs):
        val = getattr(parsed_args, required_attr, None)
        default = parser.get_default(required_attr) if parser else None
        if val in (None, '') or val == default:
            return []
        return inner_completer(prefix, parsed_args, **kwargs)
    return wrapper


def _find_beads_by_name_prefix(boxes, name_prefix, conditions=None, format_fn=None):
    """
    Find beads from boxes by name prefix and format results.

    Args:
        boxes: Iterable of Box objects to query
        name_prefix: String prefix to filter bead names
        conditions: Optional list of (QueryCondition, value) tuples for filtering
        format_fn: Optional function(box, bead) -> str to format results.
                   Defaults to returning bead.name

    Returns:
        List of formatted candidate strings (deduplicated by bead name)
    """
    if conditions is None:
        conditions = []
    if format_fn is None:
        format_fn = _default_format_bead_name

    candidates = []
    seen_names = set()

    for box in boxes:
        try:
            beads = box.index.get_beads(conditions)
            for bead in beads:
                if bead.name.startswith(name_prefix) and bead.name not in seen_names:
                    candidates.append(format_fn(box, bead))
                    seen_names.add(bead.name)
        except Exception:
            # Skip boxes that fail to query
            continue

    return candidates


def complete_bead_spec(prefix, parsed_args, **kwargs):
    """
    Autocomplete function for bead specifications.

    Bash splits at ':' and '@' (default COMP_WORDBREAKS).
    We reconstruct the full bead spec from COMP_LINE and COMP_POINT,
    then return full parts for bash to recombine.

    Args:
        prefix: The partial word being completed (from argcomplete after splitting)
        parsed_args: Already parsed arguments (from argparse)
        **kwargs: Additional argcomplete context

    Returns:
        List of completion candidates (incremental parts to add)
    """
    # Create shell context with reconstructed spec
    shell_ctx = ShellContext.from_environment(prefix)
    _debug(f'[ENTRY] prefix={repr(prefix)}')
    _debug(f'[ENTRY] shell_type={shell_ctx.shell_type}')
    _debug(f'[ENTRY] full_spec={repr(shell_ctx.full_spec)}')
    _debug(f'[ENTRY] parsed_args={parsed_args}')

    comp_line = os.environ.get('COMP_LINE', '')
    comp_point = os.environ.get('COMP_POINT', '')
    _debug(f'[ENTRY] COMP_LINE={repr(comp_line)}, COMP_POINT={repr(comp_point)}')
    if comp_point:
        _debug(f'[ENTRY] COMP_LINE length={len(comp_line)}, chars after cursor: {repr(comp_line[int(comp_point):])}')
    _debug(f'[ENTRY] Has # in COMP_LINE: {"#" in comp_line}, Has @ in COMP_LINE: {"@" in comp_line}')

    full_spec = shell_ctx.full_spec

    # Try to create environment for querying boxes/beads
    try:
        env = get_environment()
    except Exception:
        # If we can't get environment, return empty completions
        return []

    # File path completion - delegate to shell
    if '/' in full_spec or '\\' in full_spec:
        return []  # Let shell handle file completion

    # Parse the full spec to understand what we're completing
    try:
        spec = BeadSpec.parse(full_spec)
    except Exception:
        # If parsing fails, return empty
        return []

    # Determine what to complete based on separators in full spec
    if '@' in full_spec:
        # Complete timestamp expressions
        result = _complete_timestamp_expression(full_spec, spec, env, prefix)
    elif ':' in full_spec:
        # Complete bead names after box:
        result = _complete_bead_after_box(full_spec, spec, env, prefix)
    else:
        # Complete both box names and bead names
        result = _complete_box_or_bead(full_spec, env, prefix)

    _debug(f'[RESULT] candidates={result}')

    return result


def _complete_box_or_bead(full_spec, env, prefix):
    """Complete both box names (with :) and bead names.

    Args:
        full_spec: The full bead spec being completed (e.g., "box" or "nam")
        env: Environment with boxes and beads
        prefix: The incremental part from argcomplete (what was just typed)

    Returns:
        List of full completions for the current part
    """
    candidates = []

    # Add box names with : suffix
    try:
        for box in env.get_boxes():
            if box.name.startswith(full_spec):
                # Return the full box name with colon separator
                candidates.append(box.name + ':')
    except Exception:
        pass

    # Add bead names from all boxes
    try:
        boxes = env.get_boxes()
        candidates.extend(_find_beads_by_name_prefix(boxes, full_spec))
    except Exception:
        pass

    return sorted(candidates)


def _complete_beads_from_spec_box(spec, name_prefix, env):
    """Complete beads using the box name from parsed spec.

    Returns:
        List of candidates, or None if strategy failed
    """
    if not spec.box:
        return None

    try:
        _debug(f'[spec_box_strategy] Using spec.box={repr(spec.box)}')
        box = env.get_box(spec.box)
        _debug(f'[spec_box_strategy] Found box {spec.box}')
        return _find_beads_by_name_prefix(
            [box], name_prefix,
            format_fn=lambda b, bead: spec.box + ':' + bead.name
        )
    except Exception as e:
        _debug(f'[spec_box_strategy] Failed: {e}')
        return None


def _complete_beads_from_parsed_box(box_name, name_prefix, env):
    """Complete beads using box name parsed from full_spec.

    Returns:
        List of candidates, or None if strategy failed
    """
    try:
        _debug(f'[parsed_box_strategy] Trying box_name={repr(box_name)}')
        box = env.get_box(box_name)
        _debug(f'[parsed_box_strategy] Found box {box_name}')
        return _find_beads_by_name_prefix(
            [box], name_prefix,
            format_fn=lambda b, bead: box_name + ':' + bead.name
        )
    except Exception as e:
        _debug(f'[parsed_box_strategy] Failed: {e}')
        return None


def _complete_beads_from_all_boxes(name_prefix, env):
    """Complete beads by querying all boxes (fallback strategy).

    Returns:
        List of candidates, or empty list if strategy failed
    """
    try:
        _debug('[all_boxes_strategy] Querying all boxes')
        boxes = env.get_boxes()
        return _find_beads_by_name_prefix(
            boxes, name_prefix,
            format_fn=lambda b, bead: b.name + ':' + bead.name
        )
    except Exception as e:
        _debug(f'[all_boxes_strategy] Failed: {e}')
        return []


def _complete_bead_after_box(full_spec, spec, env, prefix):
    """Complete bead names from specified box.

    Uses three strategies in order:
    1. Try with spec.box (if explicitly specified)
    2. Try with box_name parsed from full_spec
    3. Fall back to querying all boxes

    Args:
        full_spec: The full bead spec being completed (e.g., "box:nam")
        spec: Parsed BeadSpec
        env: Environment with boxes and beads
        prefix: The incremental part from argcomplete

    Returns:
        List of full bead name completions
    """
    _debug(f'[_complete_bead_after_box] spec.box={repr(spec.box)}, spec.name={repr(spec.name)}')

    # Parse spec components
    spec_ctx = SpecContext.parse(full_spec)
    if not spec_ctx.has_colon:
        _debug("[_complete_bead_after_box] No ':' in full_spec, returning []")
        return []

    _debug(f'[_complete_bead_after_box] box_part={repr(spec_ctx.box_part)}, name_part={repr(spec_ctx.name_part)}')

    # Try strategies in order
    candidates = (_complete_beads_from_spec_box(spec, spec_ctx.name_part, env) or
                  _complete_beads_from_parsed_box(spec_ctx.box_part, spec_ctx.name_part, env) or
                  _complete_beads_from_all_boxes(spec_ctx.name_part, env))

    _debug(f'[_complete_bead_after_box] returning {len(candidates)} candidates: {sorted(candidates)}')
    return sorted(candidates)


def _collect_timestamp_completions(spec, env, time_prefix, spec_ctx):
    """Collect timestamp completions from matching beads.

    Args:
        spec: Parsed BeadSpec
        env: Environment with boxes
        time_prefix: Time prefix to filter patterns
        spec_ctx: Parsed SpecContext for formatting

    Returns:
        List of formatted timestamp completions (e.g., 'box:name@2024-06-15')
    """
    candidates = []

    try:
        # Determine which boxes to search
        boxes = _get_boxes_for_spec(spec, env)

        # Build query conditions for matching beads
        conditions = []
        if spec.name:
            conditions.append((QueryCondition.BEAD_NAME, spec.name))

        # Get beads and extract unique timestamp patterns
        seen_timestamps = set()
        for box in boxes:
            try:
                beads = box.index.get_beads(conditions)
                for bead in beads:
                    # Use bead's freeze_time property (datetime object)
                    dt = bead.freeze_time
                    # Extract progressively detailed timestamp patterns
                    patterns = _extract_timestamp_patterns(dt)
                    for pattern in patterns:
                        if pattern.startswith(time_prefix) and pattern not in seen_timestamps:
                            # Return full spec with timestamp pattern
                            prefix_part = spec_ctx.get_name_spec()
                            candidates.append(prefix_part + '@' + pattern)
                            seen_timestamps.add(pattern)
            except Exception:
                continue
    except Exception:
        pass

    return candidates


def _complete_timestamp_expression(full_spec, spec, env, prefix):
    """Complete timestamp expressions with actual timestamps from matching beads.

    Args:
        full_spec: The full spec with @ (e.g., 'box:name@la')
        spec: Parsed BeadSpec
        env: Environment with boxes and beads
        prefix: The incremental part from argcomplete

    Returns:
        List of full time expression completions
    """
    candidates = []

    # Parse spec components
    spec_ctx = SpecContext.parse(full_spec)
    if not spec_ctx.has_at:
        return []

    time_prefix = spec_ctx.time_part or ''

    # Fixed completions (keep list format for future additions)
    fixed_completions = [
        'latest',
    ]

    for completion in fixed_completions:
        if completion.startswith(time_prefix):
            # Return full name_part@completion
            prefix_part = spec_ctx.get_name_spec()
            candidates.append(prefix_part + '@' + completion)

    # Collect actual timestamps from matching beads
    candidates.extend(_collect_timestamp_completions(spec, env, time_prefix, spec_ctx))

    return sorted(candidates)


def _extract_timestamp_patterns(dt):
    """
    Extract useful timestamp completion patterns from a datetime object.

    Uses progressively longer format strings to create ISO-like patterns.
    For example, datetime(2024, 6, 15, 14, 30, 45) yields:
    - "2024"
    - "2024-06"
    - "2024-06-15"
    """
    patterns = []

    try:
        # Year
        patterns.append(dt.strftime('%Y'))

        # Year-Month
        patterns.append(dt.strftime('%Y-%m'))

        # Year-Month-Day
        patterns.append(dt.strftime('%Y-%m-%d'))
    except Exception:
        pass

    return patterns


def complete_input_name(prefix, parsed_args, **kwargs):
    """
    Autocomplete function for input names.

    Returns input names with source bead, timestamp, and load status.

    Args:
        prefix: The partial input name being completed
        parsed_args: Already parsed arguments (not used - workspace from cwd)
        **kwargs: Additional argcomplete context

    Returns:
        Dict mapping input names to "source@date (status)" descriptions
    """
    try:
        workspace = Workspace.for_current_working_directory()
        if not workspace.is_valid:
            return {}

        # Build dict with input names and descriptions
        result = {}
        for input_spec in workspace.inputs:
            if input_spec.name.startswith(prefix):
                # Get source bead name
                source_name = workspace.get_source_name(input_spec.name)

                # Get date from freeze_time_iso (first 10 chars: YYYY-MM-DD)
                date_str = input_spec.freeze_time_iso[:10]

                # Get load status
                status = "loaded" if workspace.is_loaded(input_spec.name) else "not loaded"

                # Format: "source@date (status)"
                result[input_spec.name] = f"{source_name}@{date_str} ({status})"

        return result
    except Exception:
        # Gracefully handle any errors
        return {}


def complete_box_name(prefix, parsed_args, **kwargs):
    """
    Autocomplete function for box names.

    Returns enabled box names with their directory paths as descriptions.

    Args:
        prefix: The partial box name being completed
        parsed_args: Already parsed arguments
        **kwargs: Additional argcomplete context

    Returns:
        Dict mapping box names to shortened directory paths (with ~)
    """
    try:
        env = get_environment()

        # Get enabled boxes
        boxes = env.get_boxes()

        # Build dict with box names and shortened paths
        result = {}
        for box in boxes:
            if box.name.startswith(prefix):
                # Shorten path by replacing home directory with ~
                path_str = str(box.directory)
                home = os.path.expanduser('~')
                if path_str.startswith(home):
                    path_str = '~' + path_str[len(home):]
                result[box.name] = path_str

        return result
    except Exception:
        # Gracefully handle any errors
        return {}
