import os
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
from bead.workspace import Workspace
from bead.ziparchive import ZipArchive

from . import arg_help
from . import arg_metavar

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


def BEAD_TIME(parser):
    parser.arg('-t', '--time', dest='bead_time', type=time_from_user, default=TIME_LATEST)


def BEAD_OFFSET(parser):
    parser.arg('-N', '--next', dest='bead_offset', action='store_const', const=1, default=0)
    parser.arg('-P', '--prev', '--previous', dest='bead_offset', action='store_const', const=-1)


def arg_bead_ref_base(nargs, default):
    '''
    Declare bead_ref_base argument - either a name or a file or something special
    '''
    def declare(parser):
        parser.arg(
            'bead_ref_base', metavar=arg_metavar.BEAD_REF, help=arg_help.BEAD_REF,
            nargs=nargs, type=str, default=default)
    return declare


def BEAD_REF_BASE_defaulting_to(name):
    return arg_bead_ref_base(nargs='?', default=name)


BEAD_REF_BASE = arg_bead_ref_base(nargs=None, default=None)


def resolve_bead(env, bead_ref_base, time) -> Archive:
    # prefer exact file name over box search
    if os.path.isfile(bead_ref_base):
        return ZipArchive(bead_ref_base)

    # not a file - try box search
    boxes = env.get_boxes()
    bead = bead_box.search(boxes).by_name(bead_ref_base).at_or_older(time).newest()
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
