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

from bead import meta
from bead.box_query import QueryCondition
from .bead_spec import BeadSpec
from .environment import get_environment


SHELL_COMMAND_SEPARATORS = ' |><&;()'


def _debug(msg):
    """Write debug message to log file if BEAD_COMPLETION_DEBUG is set."""
    debug_log = os.environ.get('BEAD_COMPLETION_DEBUG')
    if debug_log:
        with open(debug_log, 'a') as f:
            f.write(msg + '\n')


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
    # Log entry
    _debug(f"[ENTRY] prefix={repr(prefix)}")
    _debug(f"[ENTRY] parsed_args={parsed_args}")
    comp_line = os.environ.get('COMP_LINE', '')
    comp_point = os.environ.get('COMP_POINT', '')
    _debug(f"[ENTRY] COMP_LINE={repr(comp_line)}, COMP_POINT={repr(comp_point)}")
    _debug(f"[ENTRY] COMP_LINE length={len(comp_line)}, chars after cursor: {repr(comp_line[int(comp_point):] if comp_point else '')}")
    _debug(f"[ENTRY] Has # in COMP_LINE: {'#' in comp_line}, Has @ in COMP_LINE: {'@' in comp_line}")

    # Reconstruct the full bead spec from COMP_LINE and COMP_POINT FIRST
    # This is crucial - we need the full context before creating Environment
    full_spec = _reconstruct_spec_from_comp_line(prefix)

    _debug(f"[SPEC] full_spec={repr(full_spec)}")

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
        # Complete time expressions
        result = _complete_time_expr(full_spec, spec, env, prefix)
    elif ':' in full_spec:
        # Complete bead names after box:
        result = _complete_bead_after_box(full_spec, spec, env, prefix)
    else:
        # Complete both box names and bead names
        result = _complete_box_or_bead(full_spec, env, prefix)

    _debug(f"[RESULT] candidates={result}")

    return result


def _reconstruct_spec_from_comp_line(prefix):
    """
    Reconstruct the full bead spec from COMP_LINE and COMP_POINT environment variables.

    Bash splits on COMP_WORDBREAKS (:, @, etc.), so we need to piece back together
    what the user was typing before the wordbreak characters got in the way.

    ZSH does NOT split on these characters, so prefix is already complete.
    For ZSH, we just return the prefix as-is.

    Args:
        prefix: The current partial word (from argcomplete)

    Returns:
        Full bead spec string (e.g., "box:name@la")
    """
    # Detect which shell we're running under
    target_shell = os.environ.get('_ARGCOMPLETE_SHELL', 'bash')

    # For ZSH, prefix is already the complete spec (no splitting on : and @)
    if target_shell == 'zsh':
        return prefix

    # For BASH and others, reconstruct from COMP_LINE since prefix is split
    comp_line = os.environ.get('COMP_LINE', '')
    comp_point = os.environ.get('COMP_POINT', '')

    if not comp_line or not comp_point:
        # Can't reconstruct, return just the prefix
        return prefix

    try:
        comp_point = int(comp_point)
    except (ValueError, TypeError):
        return prefix

    if comp_point < 0 or comp_point > len(comp_line):
        # Can't reconstruct, return just the prefix
        return prefix

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

    return full_spec


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
        seen_names = set()
        for box in boxes:
            try:
                # Query distinct bead names
                beads = box.index.get_beads([])
                for bead in beads:
                    if bead.name.startswith(full_spec) and bead.name not in seen_names:
                        # Return the full bead name
                        candidates.append(bead.name)
                        seen_names.add(bead.name)
            except Exception:
                continue
    except Exception:
        pass

    return sorted(candidates)


def _complete_bead_after_box(full_spec, spec, env, prefix):
    """Complete bead names from specified box.

    Args:
        full_spec: The full bead spec being completed (e.g., "box:nam")
        spec: Parsed BeadSpec
        env: Environment with boxes and beads
        prefix: The incremental part from argcomplete

    Returns:
        List of full bead name completions
    """
    _debug(f"[_complete_bead_after_box] spec.box={repr(spec.box)}, spec.name={repr(spec.name)}")

    candidates = []

    # Extract the part after the colon
    if ':' not in full_spec:
        _debug("[_complete_bead_after_box] No ':' in full_spec, returning []")
        return []

    box_name, name_prefix = full_spec.rsplit(':', 1)
    _debug(f"[_complete_bead_after_box] box_name={repr(box_name)}, name_prefix={repr(name_prefix)}")

    try:
        # Get beads from the specified box
        if spec.box:
            # Box name was explicitly specified
            _debug(f"[_complete_bead_after_box] Using spec.box={repr(spec.box)}")
            box = env.get_box(spec.box)
            beads = box.index.get_beads([])
            _debug(f"[_complete_bead_after_box] Found {len(beads)} beads in {spec.box}")
            seen_names = set()
            for bead in beads:
                if bead.name.startswith(name_prefix) and bead.name not in seen_names:
                    # Return full box:name (argcomplete will trim the box: part based on wordbreak)
                    candidates.append(spec.box + ':' + bead.name)
                    seen_names.add(bead.name)
        else:
            # Box name part of full_spec (e.g., "box" from "box:")
            # Try to find beads from that box
            _debug(f"[_complete_bead_after_box] spec.box is empty, trying box_name={repr(box_name)}")
            try:
                box = env.get_box(box_name)
                beads = box.index.get_beads([])
                _debug(f"[_complete_bead_after_box] Found {len(beads)} beads in {box_name}")
                seen_names = set()
                for bead in beads:
                    if bead.name.startswith(name_prefix) and bead.name not in seen_names:
                        # Return full box:name (argcomplete will trim the box: part based on wordbreak)
                        candidates.append(box_name + ':' + bead.name)
                        seen_names.add(bead.name)
            except Exception as e:
                # If we can't find the specific box, try all boxes
                _debug(f"[_complete_bead_after_box] Failed to get box {repr(box_name)}: {e}, trying all boxes")
                boxes = env.get_boxes()
                seen_names = set()
                for box in boxes:
                    try:
                        beads = box.index.get_beads([])
                        for bead in beads:
                            if bead.name.startswith(name_prefix) and bead.name not in seen_names:
                                # Return full box:name (argcomplete will trim the box: part based on wordbreak)
                                candidates.append(box.name + ':' + bead.name)
                                seen_names.add(bead.name)
                    except Exception:
                        continue
    except Exception as e:
        _debug(f"[_complete_bead_after_box] Exception: {e}")

    _debug(f"[_complete_bead_after_box] returning {len(candidates)} candidates: {sorted(candidates)}")
    return sorted(candidates)


def _complete_time_expr(full_spec, spec, env, prefix):
    """Complete time expressions with actual timestamps from matching beads.

    Args:
        full_spec: The full spec with @ (e.g., 'box:name@la')
        spec: Parsed BeadSpec
        env: Environment with boxes and beads
        prefix: The incremental part from argcomplete

    Returns:
        List of full time expression completions
    """
    candidates = []

    # Extract the part after @
    if '@' not in full_spec:
        return []

    name_part, time_prefix = full_spec.rsplit('@', 1)

    # Fixed completions (keep list format for future additions)
    fixed_completions = [
        'latest',
    ]

    for completion in fixed_completions:
        if completion.startswith(time_prefix):
            # Return full name_part@completion
            candidates.append(name_part + '@' + completion)

    # Get actual timestamps from matching beads
    try:
        # Determine which boxes to search
        if spec.box:
            boxes = [env.get_box(spec.box)]
        else:
            boxes = env.get_boxes()

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
                            # Return full name_part@pattern
                            candidates.append(name_part + '@' + pattern)
                            seen_timestamps.add(pattern)
            except Exception:
                continue
    except Exception:
        pass

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

    Returns input names from the current workspace that match the given prefix.

    Args:
        prefix: The partial input name being completed
        parsed_args: Already parsed arguments (containing workspace)
        **kwargs: Additional argcomplete context

    Returns:
        List of input names matching the prefix
    """
    try:
        # Get workspace from parsed_args
        workspace = getattr(parsed_args, 'workspace', None)

        if not workspace:
            return []

        # Check if workspace is valid
        if not getattr(workspace, 'is_valid', False):
            return []

        # Get input names from workspace
        input_names = list(workspace.meta.get(meta.INPUTS, {}).keys())

        # Filter by prefix
        return sorted([name for name in input_names if name.startswith(prefix)])
    except Exception:
        # Gracefully handle any errors
        return []


def complete_box_name(prefix, parsed_args, **kwargs):
    """
    Autocomplete function for box names.

    Returns enabled box names from the environment that match the given prefix.

    Args:
        prefix: The partial box name being completed
        parsed_args: Already parsed arguments
        **kwargs: Additional argcomplete context

    Returns:
        List of box names matching the prefix
    """
    try:
        env = get_environment()

        # Get enabled boxes
        boxes = env.get_boxes()

        # Get box names and filter by prefix
        box_names = [box.name for box in boxes if box.name.startswith(prefix)]

        return sorted(box_names)
    except Exception:
        # Gracefully handle any errors
        return []
