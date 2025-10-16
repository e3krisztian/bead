import sys
from typing import Iterable, NoReturn

from tqdm import tqdm

from bead import box as bead_box
from bead import log
from bead.bead import Archive
from bead.box_index import BoxIndexError, IndexingProgress
from bead.exceptions import InvalidArchive
from bead.infra.timestamp import parse_iso8601
from bead.infra.timestamp import time_from_user
from bead.meta import InputSpec
from bead.workspace import Workspace
from bead.ziparchive import ZipArchive

from . import arg_help
from . import arg_metavar
from .bead_spec import BeadSpec, parse_bead_spec, parse_relative_offset, is_relative_offset

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


def resolve_bead(
    env,
    bead_spec: str | BeadSpec,
    context_input: InputSpec | None = None,
    use_kind: bool = True,
    use_name: bool = True
) -> Archive:
    """
    Resolve a bead specification to an Archive.

    Args:
        env: Environment with box definitions
        bead_spec: Bead specification (string or BeadSpec object)
        context_input: Input spec for context name/kind and relative offsets
        use_kind: Whether to filter by kind when context_input is provided
        use_name: Whether to filter by name when context_input is provided

    Returns:
        Archive object

    Raises:
        LookupError: If bead cannot be found
        ValueError: If specification is invalid
    """
    # Parse spec if it's a string
    if isinstance(bead_spec, str):
        spec = parse_bead_spec(bead_spec)
    else:
        spec = bead_spec

    # Handle file paths
    if spec.is_file_path:
        return ZipArchive(spec.file_path)

    # Determine bead name
    name = None
    if spec.name:
        name = spec.name
    elif use_name and context_input:
        name = context_input.name
    elif not use_kind:
        # NAME_ONLY strategy requires a name
        raise ValueError("Bead name not specified and no context provided")

    # Filter boxes
    if spec.box:
        boxes = [env.get_box(spec.box)]
    else:
        boxes = env.get_boxes()

    # Build search query
    query = bead_box.search(boxes)

    # Add name constraint if we have a name
    if name:
        query = query.by_name(name)

    # Add kind constraint if requested
    if use_kind and context_input:
        query = query.by_kind(context_input.kind)

    # Apply time constraint from spec
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
            else:
                try:
                    time_constraint = time_from_user(time_expr)
                except ValueError:
                    die(f"Invalid time expression: '{time_expr}'. "
                        f"Expected ISO8601 timestamp (e.g., 2024-06-15) or timedelta (e.g., 1d, 2w).")

            bead = query.at_or_older(time_constraint).newest()
    else:
        # No time specified - use latest
        bead = query.at_or_older(TIME_LATEST).newest()

    # Resolve to archive
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
