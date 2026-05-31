# PYTHON_ARGCOMPLETE_OK
from collections.abc import Sequence
import importlib.metadata
import logging
import os
import subprocess
import sys
import textwrap
import traceback


from bead.exceptions import BoxIndexError
from bead.infra.fs import Path
from bead.infra.timestamp import timestamp

from . import box
from . import completion
from . import input
from . import workspace
from .cmdparse import Command
from .cmdparse import Parser
from .environment import Environment, WorkspaceNotFoundError
from .migration import migrate_config_if_needed
import platformdirs
from .graph import commands as graph_commands


def output_of(shell_cmd: str):
    return subprocess.check_output(shell_cmd, shell=True).decode('utf-8').strip()


def get_version_info():
    try:
        version = importlib.metadata.version('bead')
    except importlib.metadata.PackageNotFoundError:
        version = 'unknown'

    return textwrap.dedent(
        f'''
        Python:
        ------
        {sys.version}

        Bead:
        ----
        {version}
        '''
    )


class CmdVersion(Command):
    '''
    Show program version info
    '''

    def run(self, args, env: 'Environment'):
        print(get_version_info())


def make_argument_parser(defaults):
    parser = Parser.new(defaults)
    (parser
        .commands(
            ('new', workspace.CmdNew, 'Create a new subdirectory and initialize it as a bead workspace.'),
            ('init', workspace.CmdInit, "Initialize the current directory as a bead workspace. (Like 'bead new' but in-place.)"),
            ('branch', workspace.CmdBranch, 'Assign a new kind, starting a new incompatible bead series.'),
            ('edit', workspace.CmdEdit, 'Create workspace from specified bead.'),
            ('discard', workspace.CmdDiscard, 'Delete workspace.'),
            ('save', workspace.CmdSave, 'Save workspace in a box.'),
            ('status', workspace.CmdStatus, 'Show workspace information.'),
            ('graph', graph_commands.CmdGraph, 'Visualize dependency graph and connections between beads.'),
            ('nuke', workspace.CmdDiscard, 'Delete workspace. (same as discard)'),
            ('completion', completion.CmdCompletion, 'Output shell completion setup code.'),
            ('version', CmdVersion, 'Show program version.'),
        ))

    (parser
        .group('input', 'Manage data loaded from other beads')
        .commands(
            ('add', input.CmdInputAdd, 'Define dependency and load its data.'),
            ('delete', input.CmdDelete, 'Forget all about an input.'),
            ('rm', input.CmdDelete, 'Forget all about an input. (alias for delete)'),
            ('map', input.CmdMap, 'Change the name of the bead from which the input is loaded/updated.'),
            ('update', input.CmdUpdate, 'Update input[s] to newest version or defined bead.'),
            ('load', input.CmdLoad, 'Load data from already defined dependency.'),
            ('unload', input.CmdUnload, 'Unload input data.'),
        ))

    box_parser = parser.group('box', 'Manage bead boxes')
    box_parser.commands(
        ('add', box.CmdBoxAdd, 'Define a box.'),
        ('list', box.CmdList, 'Show known boxes.'),
        ('forget', box.CmdForget, 'Forget a known box.'),
        ('enable', box.CmdEnable, 'Enable a box.'),
        ('disable', box.CmdDisable, 'Disable a box.'),
        ('index', box.CmdIndex, 'Create or update box index for faster searches.'),
        ('reindex', box.CmdReindex, 'Rebuild box index from scratch.'),
    )

    return parser


def setup_logging(debug: bool, log_dir: Path):
    """
    Configure logging to write only to file, never to console.

    This allows error handling functions (die, warning, info) to control
    all console output directly, while allowing log.* calls to write to
    debug logs without duplication.

    Args:
        debug: Enable debug logging to file
        log_dir: Directory for log files
    """
    # Remove any default handlers to prevent console output
    logging.root.handlers = []

    if debug:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / 'bead-debug.log'

        # Add only file handler - never log to console
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logging.root.addHandler(file_handler)
        logging.root.setLevel(logging.DEBUG)
    else:
        # Add NullHandler to prevent last-resort stderr output
        # User-facing messages go through die/warning/info functions
        logging.root.addHandler(logging.NullHandler())
        logging.root.setLevel(logging.WARNING)


def cleanup_old_error_files(error_dir: Path, keep_count: int = 20, max_age_days: int = 90):
    """
    Clean up old error files to prevent accumulation.

    Keeps the most recent keep_count files, and deletes any older than max_age_days.
    """
    import time

    if not error_dir.exists():
        return

    cutoff_time = time.time() - (max_age_days * 86400)
    error_files = sorted(error_dir.glob('error_*.txt'), key=lambda p: p.stat().st_mtime)

    # Delete files older than max_age_days
    for error_file in error_files:
        if error_file.stat().st_mtime < cutoff_time:
            error_file.unlink()

    # Re-scan after age-based cleanup
    error_files = sorted(error_dir.glob('error_*.txt'), key=lambda p: p.stat().st_mtime)

    # Keep only the most recent keep_count files
    if len(error_files) > keep_count:
        for old_file in error_files[:-keep_count]:
            old_file.unlink()


def run(config_dir: Path, state_dir: Path, argv: Sequence[str]):
    parser_defaults = dict(config_dir=config_dir)
    parser = make_argument_parser(parser_defaults)

    # Handle completion requests (bash, zsh, fish all use _ARGCOMPLETE env var)
    # This must be called after parser creation but before parse_args()
    parser.autocomplete()

    env = Environment(config_dir, state_dir)
    try:
        return parser.dispatch(argv, env)
    except WorkspaceNotFoundError as e:
        print(f'ERROR: {e.directory} is not a valid workspace', file=sys.stderr)
        return 1
    except BoxIndexError as e:
        print_box_index_error(e)
        return 1


FAILURE_TEMPLATE = """\
{exception}
An unexpected error occurred. Details saved to:
  {error_report}

Please report this issue at:
  {repo}/issues/new
(Attach the error file above)
"""


def print_box_index_error(e):
    """Print BoxIndexError with structured formatting to stderr."""
    # Format error with structured information
    error_msg = f"ERROR: {e}"
    if e.box_name:
        error_msg = f"ERROR (Box '{e.box_name}'): {e}"
    print(error_msg, file=sys.stderr)

    # Show index path if available
    if e.index_path:
        print(f"Index: {e.index_path}", file=sys.stderr)

    if e.advice:
        print(f"ADVICE: {e.formatted_advice}", file=sys.stderr)
    else:
        # A sensible default for unexpected index errors
        default_advice = "This might be resolved by running 'bead box reindex"
        if e.box_name:
            default_advice += f" {e.box_name}"
        default_advice += "'."
        print(f"ADVICE: {default_advice}", file=sys.stderr)


def is_existing_cwd():
    """Is the current working directory a valid directory?

    On POSIX systems `bead discard` causes a strange situation,
    where the calling process' working directory becomes non-existing.
    It means, that all future file operations there will fail.
    """
    try:
        return Path('.').resolve().is_dir()
    except FileNotFoundError:
        return False


def main(run=run):
    if not is_existing_cwd():
        print(
            'ERROR: Current working directory is non-functional.\n'
            + 'Is it a "discard"-ed workspace? Use "cd .." to fix it.',
            file=sys.stderr)
        sys.exit(2)

    # Setup directories
    config_dir = Path(platformdirs.user_config_dir('bead'))
    state_dir = Path(platformdirs.user_state_dir('bead'))
    log_dir = Path(os.environ.get('BEAD_LOG_DIR', platformdirs.user_log_dir('bead')))

    migrate_config_if_needed(config_dir)

    config_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    # Setup logging
    debug = os.environ.get('BEAD_DEBUG') == '1'
    setup_logging(debug, log_dir)

    # Clean up old error files
    error_dir = log_dir / 'errors'
    cleanup_old_error_files(error_dir)

    try:
        retval = run(config_dir, state_dir, sys.argv[1:])
    except KeyboardInterrupt:
        print("Interrupted :(", file=sys.stderr)
        retval = -1
    except SystemExit:
        raise
    except BaseException:
        # all remaining erroros are catched - including RunTimeErrors
        sys_argv = f'{sys.argv!r}'
        exception = traceback.format_exc()
        short_exception = traceback.format_exc(limit=1)

        # Write error report to log directory
        error_dir.mkdir(parents=True, exist_ok=True)
        error_report = error_dir / f'error_{timestamp()}.txt'
        with open(error_report, 'w') as f:
            f.write(f'sys_argv = {sys_argv}\n')
            f.write(f'cwd = {os.getcwd()}\n')
            f.write(f'{exception}\n')
            f.write(f'{get_version_info()}\n')
        print(
            FAILURE_TEMPLATE.format(
                exception=short_exception,
                error_report=error_report,
                repo='https://github.com/e3krisztian/bead',
            ),
            file=sys.stderr
        )
        retval = -1
    sys.exit(retval)


if __name__ == '__main__':
    main()
