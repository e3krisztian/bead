import os.path
from typing import TYPE_CHECKING

from bead.box import resolve
from bead.box import search
from bead.exceptions import InvalidArchive
from bead.workspace import Workspace

from . import arg_help
from . import arg_metavar
from .cmdparse import Command
from .common import BEAD_OFFSET
from .common import BEAD_TIME
from .common import OPTIONAL_WORKSPACE
from .common import TIME_LATEST
from .common import BEAD_REF_BASE_defaulting_to
from .common import DefaultArgSentinel
from .common import assert_valid_workspace
from .common import die
from .common import resolve_bead
from .common import verify_with_feedback
from .common import warning

if TYPE_CHECKING:
    from .environment import Environment

# input_nick
ALL_INPUTS = DefaultArgSentinel('all inputs')


def OPTIONAL_INPUT_NICK(parser):
    '''
    Declare `input_nick` as optional parameter
    '''
    parser.arg(
        'input_nick', type=str, nargs='?', default=ALL_INPUTS,
        metavar=arg_metavar.INPUT_NICK, help=arg_help.INPUT_NICK)


def INPUT_NICK(parser):
    '''
    Declare `input_nick` as mandatory parameter
    '''
    parser.arg(
        'input_nick',
        metavar=arg_metavar.INPUT_NICK, help=arg_help.INPUT_NICK)


# bead_ref
SAME_BEAD_NEWEST_VERSION = DefaultArgSentinel('same bead, newest version')
USE_INPUT_NICK = DefaultArgSentinel(f'use {arg_metavar.INPUT_NICK}')


class CmdAdd(Command):
    '''
    Make data from another bead available in the input directory.
    '''

    def declare(self, arg):
        arg(INPUT_NICK)
        arg(BEAD_REF_BASE_defaulting_to(USE_INPUT_NICK))
        arg(BEAD_TIME)
        arg(OPTIONAL_WORKSPACE)

    def run(self, args, env: 'Environment'):
        input_nick = args.input_nick
        bead_ref_base = args.bead_ref_base
        workspace = get_workspace(args)

        if os.path.dirname(input_nick):
            die(f'Invalid input name: {input_nick}')

        if bead_ref_base is USE_INPUT_NICK:
            bead_ref_base = input_nick

        try:
            bead = resolve_bead(env, bead_ref_base, args.bead_time)
        except LookupError:
            die(f'Not a known bead name: {bead_ref_base}')

        _check_load_with_feedback(workspace, args.input_nick, bead)


class CmdDelete(Command):
    '''
    Forget all about an input.
    '''

    def declare(self, arg):
        arg(INPUT_NICK)
        arg(OPTIONAL_WORKSPACE)

    def run(self, args, env: 'Environment'):
        input_nick = args.input_nick
        workspace = get_workspace(args)
        if workspace.has_input(input_nick):
            workspace.delete_input(input_nick)
            print(f'Input {input_nick} is deleted.')
        else:
            die(f'Input {input_nick} does not exist')


class CmdUpdate(Command):
    '''
    Update input[s] to newest version or defined bead.
    '''

    def declare(self, arg):
        arg(OPTIONAL_INPUT_NICK)
        arg(BEAD_REF_BASE_defaulting_to(SAME_BEAD_NEWEST_VERSION))
        arg(BEAD_TIME)
        arg(BEAD_OFFSET)
        arg(OPTIONAL_WORKSPACE)

    def run(self, args, env: 'Environment'):
        if args.input_nick is ALL_INPUTS:
            self.update_all_inputs(args, env)
        else:
            self.update_one_input(args, env)

    def update_all_inputs(self, args, env):
        if args.bead_ref_base is not SAME_BEAD_NEWEST_VERSION:
            die('Too many arguments')
        if args.bead_offset:
            die("--next, --prev can not be specified when updating all inputs")
        workspace = get_workspace(args)
        for input in workspace.inputs:
            try:
                bead = search(env.get_boxes()).by_kind(input.kind).at_or_older(args.bead_time).newest()
                # Resolve bead to archive for _update_input
                archive = resolve(env.get_boxes(), bead)
            except LookupError:
                if workspace.is_loaded(input.name):
                    print(
                        f'Skipping update of "{input.name}":'
                        + f' no other candidate found ({input.freeze_time})')
                else:
                    warning(f'Could not find bead for "{input.name}"')
            else:
                _update_input(workspace, input, archive)
        print('All inputs are up to date.')

    def update_one_input(self, args, env):
        input_nick = args.input_nick
        bead_ref_base = args.bead_ref_base
        workspace = get_workspace(args)
        input = workspace.get_input(input_nick)
        if input is None:
            die(f'Workspace does not have input "{input_nick}"'
                ' - did you want to add it as a new one?')
        if bead_ref_base is SAME_BEAD_NEWEST_VERSION:
            if args.bead_offset and args.bead_time is not TIME_LATEST:
                die('You can give either --prev/--next or --time, not both')

            boxes = env.get_boxes()
            try:
                if args.bead_offset:
                    # handle --prev --next - use kind instead of bead name
                    query = search(boxes).by_kind(input.kind)
                    if args.bead_offset == 1:
                        bead = query.newer_than(input.freeze_time).oldest()  # next = oldest of newer beads
                    else:
                        bead = query.older_than(input.freeze_time).newest()  # prev = newest of older beads
                else:
                    # --time - use kind instead of bead name
                    bead = search(boxes).by_kind(input.kind).at_or_older(args.bead_time).newest()
                # Resolve bead to archive
                archive = resolve(boxes, bead)
            except LookupError:
                die(f'Could not find bead for "{input.name}"')
        else:
            # path or new bead by name - same as input add, edit
            if args.bead_offset:
                die('--prev/--next is not supported when an input is replaced with another bead')
            archive = resolve_bead(env, bead_ref_base, args.bead_time)
        if archive:
            _update_input(workspace, input, archive)
        else:
            die('Can not find matching bead')


def _update_input(workspace, input, archive):
    if workspace.is_loaded(input.name) and input.content_id == archive.content_id:
        assert input.kind == archive.kind
        assert input.freeze_time == archive.freeze_time
        print(
            f'Skipping update of {input.name}:'
            + f' it is already at requested version ({input.freeze_time})')
    else:
        if input.kind != archive.kind:
            warning(f'Updating input "{input.name}" with a bead of different kind')
        _check_load_with_feedback(workspace, input.name, archive)


class CmdLoad(Command):
    '''
    Put defined input data in place.
    '''

    def declare(self, arg):
        arg(OPTIONAL_INPUT_NICK)
        arg(OPTIONAL_WORKSPACE)

    def run(self, args, env: 'Environment'):
        input_nick = args.input_nick
        workspace = get_workspace(args)
        if input_nick is ALL_INPUTS:
            inputs = workspace.inputs
            if inputs:
                for input in inputs:
                    _load(env, workspace, input)
            else:
                warning('No inputs defined to load.')
        else:
            if not workspace.has_input(input_nick):
                die(f'No input with name {input_nick}')
            _load(env, workspace, workspace.get_input(input_nick))


def _load(env, workspace, input):
    assert input is not None
    if not workspace.is_loaded(input.name):
        content_id = input.content_id
        archive = None
        for box in env.get_boxes():
            # Only try to find by exact content_id match
            try:
                bead = box.search().by_content_id(content_id).first()
                archive = box.resolve(bead)
                break
            except LookupError:
                continue
        if archive is None:
            warning(f'Could not find bead for input "{input.name}" - not loaded!')
            return
        _check_load_with_feedback(workspace, input.name, archive)
    else:
        print(f'"{input.name}" is already loaded - skipping')


def _check_load_with_feedback(workspace: Workspace, input_nick, archive):
    try:
        verify_with_feedback(archive)
    except InvalidArchive:
        warning(f'Bead for {input_nick} is found but damaged - not loading.')
    else:
        if workspace.is_loaded(input_nick):
            print(f'Removing current data from {input_nick}')
            workspace.unload(input_nick)
        print(f'Loading new data to {input_nick} ...', end='', flush=True)
        workspace.load(input_nick, archive)
        print(' Done')


class CmdUnload(Command):
    '''
    Remove input data.
    '''

    def declare(self, arg):
        arg(OPTIONAL_INPUT_NICK)
        arg(OPTIONAL_WORKSPACE)

    def run(self, args, env: 'Environment'):
        input_nick = args.input_nick
        workspace = get_workspace(args)
        if input_nick is ALL_INPUTS:
            for input in workspace.inputs:
                _unload(workspace, input.name)
        else:
            _unload(workspace, input_nick)


def _unload(workspace, input_nick):
    if workspace.is_loaded(input_nick):
        print('Unloading', input_nick, '...', end='', flush=True)
        workspace.unload(input_nick)
        print(' Done', flush=True)
    else:
        print(input_nick, 'was not loaded - skipping')


def get_workspace(args) -> Workspace:
    assert_valid_workspace(args.workspace)
    return args.workspace
