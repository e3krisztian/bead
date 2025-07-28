import os
import sys
from typing import NoReturn

from bead.exceptions import InvalidArchive
from bead.workspace import Workspace
from bead import spec as bead_spec
from bead.ziparchive import ZipArchive
from bead.bead import Archive
from bead import box as bead_box
from bead.tech.fs import Path
from bead.tech.timestamp import time_from_user, parse_iso8601
from . import arg_help
from . import arg_metavar
from .environment import Environment


TIME_LATEST = parse_iso8601('9999-12-31')

ERROR_EXIT = 1


def die(msg) -> NoReturn:
    sys.stderr.write('ERROR: ')
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    sys.exit(ERROR_EXIT)


def warning(msg):
    sys.stderr.write('WARNING: ')
    sys.stderr.write(msg)
    sys.stderr.write('\n')


def info(msg):
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


class get_env:
    '''
    Make an Environment when called.

    It will also create a missing config directory and provides a meaningful
    text when used as default for an argparse argument.
    '''

    def __init__(self, config_dir):
        self.config_dir = Path(config_dir)

    def __call__(self):
        config_dir = self.config_dir
        try:
            os.makedirs(config_dir)
        except OSError:
            if not os.path.isdir(config_dir):
                raise
        return Environment.from_dir(config_dir)

    def __repr__(self):
        return f'Environment at {self.config_dir}'


def OPTIONAL_ENV(parser):
    '''
    Define `env` as option, defaulting to environment config in user's home directory
    '''
    config_dir = parser.defaults['config_dir']
    parser.arg(
        '--env', '--environment', metavar=arg_metavar.ENV,
        dest='get_env',
        type=get_env, default=get_env(config_dir),
        help=arg_help.ENV)


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


def resolve_bead(env, bead_ref_base, time):
    # prefer exact file name over box search
    if os.path.isfile(bead_ref_base):
        return ZipArchive(bead_ref_base)

    # not a file - try box search
    return bead_box.search_boxes(env.get_boxes()).by_name(bead_ref_base).at_or_older(time).newest()


def verify_with_feedback(archive: Archive):
    print(f'Verifying archive {archive.archive_filename} ...', end='', flush=True)
    try:
        archive.validate()
        print(' OK', flush=True)
    except InvalidArchive:
        print(' DAMAGED!', flush=True)
        raise
