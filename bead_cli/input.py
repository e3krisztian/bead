import os.path
from enum import Enum
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
from .common import refresh_all_box_indexes
from .common import resolve_bead
from .common import verify_with_feedback
from .common import warning

if TYPE_CHECKING:
    from .environment import Environment


class MatchStrategy(Enum):
    """Strategy for matching beads during input update.

    Historical evolution:
    - Pre-2019: KIND_ONLY matching
    - 2019-2025: NAME_ONLY matching
    - 2025+: NAME_AND_KIND (strict) matching by default

    IMPORTANT: No automatic fallbacks between strategies - any relaxation
    must be explicit user choice to preserve upgrade coordinate integrity.
    """
    NAME_AND_KIND = "name_and_kind"  # Default: strict matching
    NAME_ONLY = "name_only"          # --no-kind: ignore kind differences
    KIND_ONLY = "kind_only"          # --no-name: ignore name differences


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

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        try:
            bead = resolve_bead(env, bead_ref_base, args.bead_time)
        except LookupError:
            die(f'Not a known bead name: {bead_ref_base}')

        _check_load_with_feedback(workspace, args.input_nick, bead)
        workspace.set_input_bead_name(args.input_nick, bead.name)


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


class CmdMap(Command):
    '''
    Change the name of the bead from which the input is loaded/updated.
    '''

    def declare(self, arg):
        arg(INPUT_NICK)
        arg(BEAD_REF_BASE_defaulting_to(USE_INPUT_NICK))
        arg(OPTIONAL_WORKSPACE)

    def run(self, args, env: 'Environment'):
        input_nick = args.input_nick
        bead_ref_base = args.bead_ref_base
        workspace = get_workspace(args)

        if input_nick not in [input_spec.name for input_spec in workspace.inputs]:
            die(f'Unknown input name: {input_nick}')

        if bead_ref_base is USE_INPUT_NICK:
            bead_ref_base = input_nick

        workspace.set_input_bead_name(input_nick, bead_ref_base)
        print(f'Input "{input_nick}" mapped to bead "{bead_ref_base}"')


class CmdUpdate(Command):
    '''
    Update input[s] to newest version or defined bead.

    By default, matches by both input name and kind for precise updates.
    Use --no-kind or --no-name to relax matching constraints.
    '''

    def declare(self, arg):
        arg(OPTIONAL_INPUT_NICK)
        arg(BEAD_REF_BASE_defaulting_to(SAME_BEAD_NEWEST_VERSION))
        arg(BEAD_TIME)
        arg(BEAD_OFFSET)
        arg(OPTIONAL_WORKSPACE)
        # Matching options (mutually exclusive)
        # NOTE: Option names --no-kind/--no-name chosen for better UX over --ignore-kind/--ignore-name
        # as they more clearly communicate what constraint is being relaxed
        def add_matching_options(parser):
            matching_group = parser.argparser.add_mutually_exclusive_group()
            matching_group.add_argument(
                '--no-kind', action='store_const', const=MatchStrategy.NAME_ONLY,
                dest='match_strategy', help='Ignore bead kind when matching (match by name only)')
            matching_group.add_argument(
                '--no-name', action='store_const', const=MatchStrategy.KIND_ONLY,
                dest='match_strategy', help='Ignore bead name when matching (match by kind only)')
            # Set default strategy
            parser.argparser.set_defaults(match_strategy=MatchStrategy.NAME_AND_KIND)
        arg(add_matching_options)

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

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        workspace = get_workspace(args)
        for input in workspace.inputs:
            try:
                archive = self._find_archive_for_update(
                    env.get_boxes(), input, args.bead_time, offset=None,
                    match_strategy=args.match_strategy, workspace=workspace
                )
            except LookupError:
                if workspace.is_loaded(input.name):
                    print(
                        f'Skipping update of "{input.name}":'
                        + f' no other candidate found ({input.freeze_time})')
                else:
                    self._warn_no_match_found(input, args)
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

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        if bead_ref_base is SAME_BEAD_NEWEST_VERSION:
            # Update from existing input
            if args.bead_offset and args.bead_time is not TIME_LATEST:
                die('You can give either --prev/--next or --time, not both')

            try:
                archive = self._find_archive_for_update(
                    env.get_boxes(), input, args.bead_time, args.bead_offset,
                    match_strategy=args.match_strategy, workspace=workspace
                )
            except LookupError:
                self._die_no_match_found(input, args)
        else:
            # Explicit bead reference (path or new bead by name)
            if args.bead_offset:
                die('--prev/--next is not supported when an input is replaced with another bead')
            try:
                archive = resolve_bead(env, bead_ref_base, args.bead_time)
            except LookupError:
                die(f'Not a known bead name: {bead_ref_base}')

        _update_input(workspace, input, archive)
        # Update mapping when user specifies explicit bead (not when using existing mapping)
        if bead_ref_base is not SAME_BEAD_NEWEST_VERSION:
            workspace.set_input_bead_name(input_nick, archive.name)

    def _find_archive_for_update(self, boxes, input, time, offset, match_strategy, workspace=None):
        """Find and resolve archive for input update based on matching strategy.

        Args:
            boxes: List of boxes to search
            input: InputSpec from workspace
            time: Timestamp constraint (for newest search)
            offset: Version offset (1 for --next, -1 for --prev, 0/None for newest)
            match_strategy: MatchStrategy enum value
            workspace: Workspace (optional, for input name mapping)

        Returns:
            Archive object ready for loading

        Raises:
            LookupError: When no matching bead is found
        """
        query = search(boxes)

        # Use mapped bead name if available, otherwise use input name
        bead_name = input.name
        if workspace:
            bead_name = workspace.get_input_bead_name(input.name)

        if match_strategy == MatchStrategy.NAME_ONLY:
            query = query.by_name(bead_name)
        elif match_strategy == MatchStrategy.KIND_ONLY:
            query = query.by_kind(input.kind)
        elif match_strategy == MatchStrategy.NAME_AND_KIND:
            query = query.by_name(bead_name).by_kind(input.kind)
        else:
            raise ValueError(f"Unknown match strategy: {match_strategy}")

        # Apply time/offset constraint and get bead
        if offset == 1:
            # --next: oldest of newer beads
            bead = query.newer_than(input.freeze_time).oldest()
        elif offset == -1:
            # --prev: newest of older beads
            bead = query.older_than(input.freeze_time).newest()
        else:
            # newest at or before time
            bead = query.at_or_older(time).newest()

        # Resolve bead to archive
        return resolve(boxes, bead)

    def _get_match_description(self, args):
        """Get human-readable description of current matching mode."""
        if args.match_strategy == MatchStrategy.NAME_ONLY:
            return "name only"
        elif args.match_strategy == MatchStrategy.KIND_ONLY:
            return "kind only"
        elif args.match_strategy == MatchStrategy.NAME_AND_KIND:
            return "name and kind"
        raise ValueError

    def _warn_no_match_found(self, input, args):
        """Provide helpful warning when no match found in update_all_inputs."""
        match_desc = self._get_match_description(args)
        msg = f'Could not find bead for "{input.name}" (matching by {match_desc})'
        if args.match_strategy == MatchStrategy.NAME_AND_KIND:
            msg += '. Try --no-kind or --no-name to relax matching'
        warning(msg)

    def _die_no_match_found(self, input, args):
        """Provide helpful error when no match found in update_one_input."""
        match_desc = self._get_match_description(args)
        msg = f'Could not find bead for "{input.name}" (matching by {match_desc})'
        if args.match_strategy == MatchStrategy.NAME_AND_KIND:
            msg += '. Try --no-kind or --no-name to relax matching'
        die(msg)


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
        
        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)
        
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
