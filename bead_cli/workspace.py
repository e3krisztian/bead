import os
from typing import TYPE_CHECKING

from bead import layouts
from bead.exceptions import BoxError
from bead.exceptions import InvalidArchive
from bead.infra.fs import Path
from bead.infra.fs import ensure_directory
from bead.infra.fs import rmtree
from bead.infra.identifier import uuid
from bead.infra.timestamp import timestamp
from bead.workspace import Workspace

from .args import BEAD_SPEC
from .args import BOX_NAME
from .args import DefaultArgSentinel
from .args import WORKSPACE
from .cmdparse import Command
from .common import die
from .common import info
from .common import MatchStrategy
from .common import refresh_all_box_indexes
from .common import resolve_bead
from .common import verify_with_feedback
from .common import warning

if TYPE_CHECKING:
    from .environment import Environment


def assert_may_be_valid_name(name):
    '''
    Refuse bead names that are non cross platform file-system compatible
    '''
    valid_syntax = (
        name
        and os.path.sep not in name
        and '/' not in name
        and '\\' not in name
        and ':' not in name
    )
    if not valid_syntax:
        die(f'Invalid name "{name}"')


class CmdNew(Command):
    '''
    Create and initialize new workspace directory for a new bead.
    '''

    def declare(self, arg):
        arg(WORKSPACE.required)

    def run(self, args, env: 'Environment'):
        workspace: Workspace = args.workspace
        assert_may_be_valid_name(workspace.name)
        if os.path.exists(workspace.directory):
            die(f'Directory {workspace.name} already exists.')

        kind = uuid()
        workspace.create(kind)
        print(f'Created workspace "{workspace.name}"')


class CmdBranch(Command):
    '''
    Assign a new kind to the workspace, starting a new incompatible bead series.
    '''

    def run(self, args, env: 'Environment'):
        workspace = env.get_workspace()
        workspace.set_kind(uuid())
        print('Branched.')


USE_THE_ONLY_BOX = DefaultArgSentinel(
    'if there is exactly one box,' +
    ' store there, otherwise it MUST be specified')


class CmdSave(Command):
    '''
    Save workspace in a box.
    '''

    def declare(self, arg):
        arg(BOX_NAME.with_default(USE_THE_ONLY_BOX))

    def run(self, args, env: 'Environment'):
        box_name = args.box_name
        workspace = env.get_workspace()
        # XXX: (usability) save - support saving directly to a directory outside of workspace
        if box_name is USE_THE_ONLY_BOX:
            boxes = env.get_boxes()
            if not boxes:
                warning('No boxes have been defined')
                beadbox = Path(os.path.expanduser('~/BeadBox'))
                info(f'Creating and using a new one with name `home` and location {beadbox}')
                ensure_directory(beadbox)
                env.add_box('home', beadbox)
                env.save()
                # continue with newly created box
                boxes = env.get_boxes()
                assert len(boxes) == 1
            if len(boxes) > 1:
                die(
                    'BOX parameter is not optional!\n' +
                    '(more than one boxes exists)')
            box = boxes[0]
        else:
            try:
                box = env.get_box(box_name)
            except LookupError:
                die(f'Unknown box: {box_name}')
        try:
            location = box.store(workspace, timestamp())
        except BoxError as e:
            die(f'Error saving: {e}')
        print(f'Successfully stored bead at {location}.')


DERIVE_FROM_BEAD_NAME = DefaultArgSentinel('derive one from bead name')


class CmdEdit(Command):
    '''
    Unpack a bead as a source tree.

    Bead directory layout is created, but only the source files are
    extracted.
    '''

    def declare(self, arg):
        arg(BEAD_SPEC.required)
        arg(WORKSPACE.with_default(DERIVE_FROM_BEAD_NAME))
        arg('--review', dest='review',
            default=False, action='store_true',
            help='Include output data for review (normally not needed for editing).')

    def run(self, args, env: 'Environment'):
        review = args.review

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        try:
            bead = resolve_bead(env, args.bead_spec)
        except LookupError:
            die('Bead not found!')
        try:
            verify_with_feedback(bead)
        except InvalidArchive:
            die('Bead is damaged')
        if args.workspace is DERIVE_FROM_BEAD_NAME:
            workspace = Workspace(bead.name)
        else:
            workspace = args.workspace

        if os.path.exists(workspace.directory):
            die(f'Workspace "{workspace.name}" directory already exists'
                ' - do you have an old checkout?')
        bead.unpack_to(workspace)
        assert workspace.is_valid

        if review:
            output_directory = workspace.directory / layouts.Workspace.OUTPUT
            bead.unpack_data_to(output_directory)

        print(f'Extracted source into {workspace.directory}')
        # XXX: try to load smaller inputs?
        if workspace.inputs:
            print('Input data not loaded, update if needed and load manually')


def print_inputs(env, workspace, verbose, match_strategy):
    if not workspace.is_valid:
        die(f'{workspace.directory} is not a valid workspace')
    inputs = sorted(workspace.inputs)

    if inputs:
        boxes = env.get_boxes()

        print('Inputs:')
        has_not_loaded = False
        is_first_input = True
        for input in inputs:
            if not is_first_input:
                print('')
            is_first_input = False

            is_loaded = workspace.is_loaded(input.name)
            has_not_loaded = has_not_loaded or not is_loaded

            print(f'input/{input.name}')

            real_name = workspace.get_source_name(input.name)
            found_beads = find_beads_by_content_id(boxes, input.content_id)

            print_input_status(is_loaded, found_beads, verbose)
            print_input_location(real_name, input, found_beads, verbose, boxes, match_strategy)

        print('')
        if has_not_loaded:
            print('Some inputs are currently not loaded.')
            print('You can "load" or "update" them manually.')
    else:
        print('No inputs defined')


def find_beads_by_content_id(boxes, content_id):
    """Search all boxes for beads matching the content_id."""
    found = []
    for box in boxes:
        try:
            bead = box.search().by_content_id(content_id).first()
            found.append((box.name, bead))
        except LookupError:
            continue
    return found


def print_input_status(is_loaded, found_beads, verbose):
    """Print the Status line if needed."""
    if verbose or not is_loaded:
        if is_loaded:
            print('\tStatus:      loaded')
        else:
            print('\tStatus:      **NOT LOADED**')


def print_input_location(real_name, input, found_beads, verbose, boxes, match_strategy):
    """Print location information (Bead/From/Box/Available as lines)."""
    if not found_beads:
        print_missing_bead(real_name, input, verbose)
        return

    update_available = check_update_available(real_name, input, boxes, match_strategy)
    update_suffix = ' **UPDATE AVAILABLE**' if update_available else ''

    matching_boxes = []
    different_name_beads = []

    for box_name, bead in found_beads:
        if bead.name == real_name:
            matching_boxes.append(box_name)
        else:
            different_name_beads.append((box_name, bead))

    if verbose:
        print(f'\tKind:        {input.kind}')
        print(f'\tContent id:  {input.content_id}')

    if len(matching_boxes) == 1 and not different_name_beads:
        print(f'\tFrom:        {matching_boxes[0]}:{real_name}@{input.freeze_time_iso}{update_suffix}')
    else:
        print(f'\tBead:        {real_name}@{input.freeze_time_iso}{update_suffix}')
        if matching_boxes:
            print(f'\tBox:         {", ".join(matching_boxes)}')
        if different_name_beads:
            specs = [f'{box_name}:{bead.name}' for box_name, bead in different_name_beads]
            print(f'\tAvailable as: {", ".join(specs)}')

    if verbose and update_available:
        print_update_details(real_name, input, boxes, match_strategy)


def print_missing_bead(real_name, input, verbose):
    """Print information for missing bead."""
    if verbose:
        print(f'\tKind:        {input.kind}')
        print(f'\tContent id:  {input.content_id}')
    print(f'\tBead:        {real_name}@{input.freeze_time_iso}')
    print('\tBox:         **NO CANDIDATES**')
    print('\t   Maybe it has been renamed? or is it in an unreachable box?')


def check_update_available(real_name, input, boxes, match_strategy):
    """Check if a newer version is available for update."""
    use_name = (match_strategy != MatchStrategy.KIND_ONLY)
    use_kind = (match_strategy != MatchStrategy.NAME_ONLY)

    for box in boxes:
        try:
            search = box.search()
            if use_name:
                search = search.by_name(real_name)
            if use_kind:
                search = search.by_kind(input.kind)

            search.newer_than(input.freeze_time).newest()
            return True
        except LookupError:
            continue

    return False


def print_update_details(real_name, input, boxes, match_strategy):
    """Print detailed update information in verbose mode."""
    use_name = (match_strategy != MatchStrategy.KIND_ONLY)
    use_kind = (match_strategy != MatchStrategy.NAME_ONLY)

    for box in boxes:
        try:
            search = box.search()
            if use_name:
                search = search.by_name(real_name)
            if use_kind:
                search = search.by_kind(input.kind)

            newest = search.newer_than(input.freeze_time).newest()
            print(f'\tUpdate:      {box.name}:{newest.name}@{newest.freeze_time_iso}')
            return
        except LookupError:
            continue


class CmdStatus(Command):
    '''
    Show workspace status - name of bead, inputs and their unpack status.
    '''

    def declare(self, arg):
        arg('-v', '--verbose', default=False, action='store_true',
            help='show more detailed information')
        arg(self._add_matching_options)

    @staticmethod
    def _add_matching_options(parser):
        """Add --no-kind/--no-name matching options for update checking."""
        matching_group = parser.argparser.add_mutually_exclusive_group()
        matching_group.add_argument(
            '--no-kind', action='store_const', const=MatchStrategy.NAME_ONLY,
            dest='match_strategy', help='Check for updates ignoring kind (match by name only)')
        matching_group.add_argument(
            '--no-name', action='store_const', const=MatchStrategy.KIND_ONLY,
            dest='match_strategy', help='Check for updates ignoring name (match by kind only)')
        parser.argparser.set_defaults(match_strategy=MatchStrategy.NAME_AND_KIND)

    def run(self, args, env: 'Environment'):
        workspace = env.get_unchecked_workspace()
        verbose = args.verbose
        match_strategy = args.match_strategy
        kind_needed = verbose
        if workspace.is_valid:
            print(f'Bead Name: {workspace.name}')
            if kind_needed:
                print(f'Bead kind: {workspace.kind}')
            print()
            print_inputs(env, workspace, verbose, match_strategy)
        else:
            warning(f'Invalid workspace ({workspace.directory})')


class CmdDiscard(Command):
    '''
    Delete the current workspace directory - like rm -rf "$PWD", only more aggressive.
    '''

    def declare(self, arg):
        arg(WORKSPACE.with_default(Workspace.for_current_working_directory()))
        arg('-f', '--force', default=False, action='store_true',
            help=('Do not check that the directory is a valid workspace.'
                  ' Removes partially removed (damaged/invalid) workspaces,'
                  ' and (DANGER ZONE!) non-workspace directories as well!'))

    def run(self, args, env: 'Environment'):
        workspace = args.workspace
        if not args.force:
            if not workspace.is_valid:
                die(f'{workspace.directory} is not a valid workspace')
        directory = workspace.directory
        # on non-posix systems (Windows) it might happen, that we can not remove
        # the directory we are in -> ignore errors
        rmtree(directory, ignore_errors=os.name != 'posix')
        print(f'Deleted workspace {directory}')
