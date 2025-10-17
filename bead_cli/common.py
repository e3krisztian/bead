import sys
from enum import Enum
from typing import Iterable, NoReturn

from tqdm import tqdm

from bead import box as bead_box
from bead import log
from bead.bead import Archive
from bead.box_index import BoxIndexError, IndexingProgress
from bead.exceptions import InvalidArchive
from bead.infra.timestamp import parse_iso8601
from bead.infra.timestamp import time_from_user
from bead.infra.timestamp import detect_time_precision
from bead.infra.timestamp import add_one_unit
from bead.infra.timestamp import TimePrecision
from bead.meta import InputSpec
from bead.workspace import Workspace
from bead.ziparchive import ZipArchive

from . import arg_help
from . import arg_metavar
from .bead_spec import BeadSpec, parse_relative_offset, is_relative_offset


class MatchStrategy(Enum):
    """Strategy for matching beads during resolution.

    Historical evolution:
    - Pre-2019: KIND_ONLY matching
    - 2019-2025: NAME_ONLY matching
    - 2025+: NAME_AND_KIND (strict) matching by default

    IMPORTANT: No automatic fallbacks between strategies - any relaxation
    must be explicit user choice to preserve upgrade coordinate integrity.
    """
    NAME_AND_KIND = ("name_and_kind", "name and kind")  # Default: strict matching
    NAME_ONLY = ("name_only", "name only")              # --no-kind: ignore kind differences
    KIND_ONLY = ("kind_only", "kind only")              # --no-name: ignore name differences

    def __init__(self, value, display_name):
        self._value_ = value
        self.display_name = display_name

TIME_LATEST = parse_iso8601('9999-12-31')

ERROR_EXIT = 1


def die(msg) -> NoReturn:
    log.error(msg)
    sys.stderr.write('ERROR: ')
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    sys.exit(ERROR_EXIT)


def warning(msg):
    log.warning(msg)
    sys.stderr.write('WARNING: ')
    sys.stderr.write(msg)
    sys.stderr.write('\n')


def info(msg):
    log.info(msg)
    sys.stderr.write(msg)
    sys.stderr.write('\n')


def OPTIONAL_WORKSPACE(parser):
    '''
    Define `workspace` as option, defaulting to current directory
    '''
    parser.arg(
        '--workspace', '-w', metavar=arg_metavar.WORKSPACE,
        type=Workspace, default=Workspace.for_current_working_directory(),
        help=arg_help.WORKSPACE)


def assert_valid_workspace(workspace):
    if not workspace.is_valid:
        die(f'{workspace.directory} is not a valid workspace')


class DefaultArgSentinel:
    '''
    I am a sentinel for default values.

    I.e. If you see me, it means that you got the default value.

    I also provide human sensible description for the default value.
    '''

    def __init__(self, description: str):
        self.description = description

    def __repr__(self):
        return self.description


def arg_bead_spec(nargs, default):
    '''
    Declare bead_spec argument - either a name or a file or something special
    '''
    def declare(parser):
        parser.arg(
            'bead_spec', metavar=arg_metavar.BEAD, help=arg_help.BEAD,
            nargs=nargs, type=str, default=default)
    return declare


def BEAD_SPEC_defaulting_to(name):
    return arg_bead_spec(nargs='?', default=name)


BEAD_SPEC = arg_bead_spec(nargs=None, default=None)


def _get_bead_name(
    spec: BeadSpec,
    context_input: InputSpec | None,
    use_name: bool
) -> str | None:
    """
    Determine the bead name to search for.

    Priority: spec name > context name > None

    Args:
        spec: Parsed bead specification
        context_input: Input spec for context name
        use_name: Whether to use context name if spec doesn't provide one

    Returns:
        Bead name or None if no name available
    """
    if spec.name:
        return spec.name
    if use_name and context_input:
        return context_input.name
    return None


def _create_bead_search(
    env,
    spec: BeadSpec,
    context_input: InputSpec | None,
    match_strategy: MatchStrategy = MatchStrategy.NAME_AND_KIND
):
    """
    Create a bead search with optional name and kind constraints.

    Selects boxes based on spec and environment, then applies optional
    filters for name (from spec or context) and kind (from context)
    according to the match strategy.

    Args:
        env: Environment with box definitions
        spec: Parsed bead specification
        context_input: Input spec for context kind/name
        match_strategy: Strategy for matching name and/or kind

    Returns:
        BeadSearch query object ready for selection
    """
    # Filter boxes
    if spec.box:
        boxes = [env.get_box(spec.box)]
    else:
        boxes = env.get_boxes()

    query = bead_box.search(boxes)

    # Add name constraint if strategy allows it
    use_name = (match_strategy != MatchStrategy.KIND_ONLY)
    name = _get_bead_name(spec, context_input, use_name)
    if name:
        query = query.by_name(name)

    # Add kind constraint if strategy allows it
    use_kind = (match_strategy != MatchStrategy.NAME_ONLY)
    if use_kind and context_input:
        query = query.by_kind(context_input.kind)

    return query


def _select_bead_by_time(
    query,
    spec: BeadSpec,
    context_input: InputSpec | None
):
    """
    Select a bead from the query based on time constraints.

    Handles three cases:
    - Relative offsets (e.g., -, +, -)
    - Absolute timestamps with precision-based end-of-unit logic
    - No time specified (defaults to latest)

    Args:
        query: BeadSearch query object
        spec: Parsed bead specification
        context_input: Input spec for freeze_time with relative offsets

    Returns:
        Selected Bead object

    Raises:
        ValueError: If relative offset requires context but none provided
        LookupError: If no bead found matching constraints
    """
    if spec.time:
        time_expr = spec.time

        if is_relative_offset(time_expr):
            # Relative offset requires context
            if not context_input:
                raise ValueError(f"Relative offset '{time_expr}' requires context input")

            offset = parse_relative_offset(time_expr)

            if offset < 0:
                # Moving backwards in time (older)
                query = query.older_than(context_input.freeze_time)
                # Get the nth older bead (offset is negative, so negate it)
                bead = query.older(-offset - 1)
            else:
                # Moving forwards in time (newer)
                query = query.newer_than(context_input.freeze_time)
                # Get the nth newer bead
                bead = query.newer(offset - 1)
        else:
            # Absolute time from spec (parse string)
            if time_expr == "latest":
                time_constraint = TIME_LATEST
                bead = query.at_or_older(time_constraint).newest()
            else:
                try:
                    time_constraint = time_from_user(time_expr)
                except ValueError:
                    die(f"Invalid time expression: '{time_expr}'. "
                        f"Expected ISO8601 timestamp (e.g., 2024-06-15) or timedelta (e.g., 1d, 2w).")

                # Detect precision and adjust to end of unit for inclusive matching
                try:
                    precision = detect_time_precision(time_expr)
                except ValueError:
                    die(f"Invalid time expression: '{time_expr}'. "
                        f"Expected ISO8601 timestamp (e.g., 2024-06-15) or timedelta (e.g., 1d, 2w).")

                if precision != TimePrecision.MICROSECOND:
                    # For partial specifications (year, month, day, etc.), use end-of-unit logic
                    # This makes @2025-09-19 match all beads on that day, not just those
                    # created after midnight UTC
                    end_of_unit = add_one_unit(time_constraint, precision)
                    bead = query.older_than(end_of_unit).newest()
                else:
                    # Fully specified timestamp - use as-is
                    bead = query.at_or_older(time_constraint).newest()
    else:
        # No time specified - use latest
        bead = query.at_or_older(TIME_LATEST).newest()

    return bead


def resolve_bead(
    env,
    bead_spec: str
) -> Archive:
    """
    Resolve a bead specification to an Archive (simple lookup).

    Finds a bead by name or file path without context or matching constraints.
    Use this for direct bead lookups like 'bead edit' or 'input add'.

    Args:
        env: Environment with box definitions
        bead_spec: Bead specification string (e.g., "name", "box:name@2024", "/path/to/bead.zip")

    Returns:
        Archive object

    Raises:
        LookupError: If bead cannot be found
        ValueError: If specification is invalid or uses relative offsets without context
    """
    spec = BeadSpec.parse(bead_spec)

    if spec.is_file_path:
        return ZipArchive(spec.file_path)

    if spec.time and is_relative_offset(spec.time):
        die(f"Relative time offset '{spec.time}' requires an input context. "
            f"Use 'bead input update' with a context.")

    # Simple search with just name constraint
    if spec.box:
        boxes = [env.get_box(spec.box)]
    else:
        boxes = env.get_boxes()

    query = bead_box.search(boxes)

    if spec.name:
        query = query.by_name(spec.name)

    # Time selection (absolute time only, no context needed)
    bead = _select_bead_by_time(query, spec, context_input=None)

    return bead_box.resolve(boxes, bead)


def find_bead_for_update(
    env,
    bead_spec: str,
    current_input: InputSpec,
    match_strategy: MatchStrategy = MatchStrategy.NAME_AND_KIND
) -> Archive:
    """
    Resolve a bead for updating an input (with full context support).

    Finds a bead with context and matching strategy support. Use this when
    updating an input, where we need to fall back to context for name/kind
    and support relative time offsets.

    Args:
        env: Environment with box definitions
        bead_spec: Bead specification string (e.g., "name", "@-", "box:name@2024")
        current_input: Current input spec for context name/kind and relative offsets
        match_strategy: Strategy for matching name and/or kind

    Returns:
        Archive object

    Raises:
        LookupError: If bead cannot be found
        ValueError: If specification is invalid
    """
    spec = BeadSpec.parse(bead_spec)

    if spec.is_file_path:
        return ZipArchive(spec.file_path)

    search = _create_bead_search(env, spec, current_input, match_strategy)
    bead = _select_bead_by_time(search, spec, current_input)

    if spec.box:
        boxes = [env.get_box(spec.box)]
    else:
        boxes = env.get_boxes()

    return bead_box.resolve(boxes, bead)


def verify_with_feedback(archive: Archive):
    print(f'Verifying archive {archive.location} ...', end='', flush=True)
    try:
        archive.validate()
        print(' OK', flush=True)
    except InvalidArchive:
        print(' DAMAGED!', flush=True)
        raise


def report_progress(description: str, progress_generator: Iterable[IndexingProgress], silent_success=False) -> bool:
    '''
    Consume a progress generator, report status using tqdm, and return success.
    '''
    errors = []
    # Initialize with a total of 0; it will be updated on the first iteration.
    with tqdm(total=0, desc=f'  {description}', unit=' files', leave=False) as pbar:
        try:
            for progress in progress_generator:
                if pbar.total != progress.total:
                    pbar.total = progress.total
                    # Refresh to show the total immediately
                    pbar.refresh()

                pbar.update(1)
                if progress.latest_error:
                    errors.append(progress.latest_error)
                    # tqdm.write is the safe way to print messages without breaking the bar
                    tqdm.write(f"  ✗ Error indexing {progress.path}")
        except BoxIndexError as e:
            # Catch fatal errors from the generator itself (e.g., DB connection)
            tqdm.write(f"  ✗ FATAL: {e}")
            # Ensure the progress bar is cleared on fatal error
            pbar.close()
            return False

    if errors:
        print(f'  ✗ Completed indexing with {len(errors)} error(s).')
        return False

    if not silent_success:
        print('  ✓ Done')
    return True


def refresh_box_index(box):
    '''Create or update index for a single box (refresh = create if needed + sync).'''
    try:
        return report_progress(f'Refreshing box "{box.name}"', box.index.sync(), silent_success=True)
    except Exception as e:
        die(f'  ✗ Failed: {e}')


def refresh_all_box_indexes(env):
    '''Refresh indexes for all enabled boxes to ensure they're up-to-date.'''
    boxes = env.get_boxes()
    if not boxes:
        return
        
    for box in boxes:
        refresh_box_index(box)
