import os.path
from typing import TYPE_CHECKING, Literal, NoReturn, overload

from bead.bead import Archive
from bead.box import resolve
from bead.box import search
from bead.exceptions import InvalidArchive
from bead.meta import InputSpec
from bead.workspace import Workspace

from .args import BEAD_SPEC
from .args import DefaultArgSentinel
from .args import INPUT_NAME
from .args import WORKSPACE
from .cmdparse import Command
from .common import MatchStrategy
from .common import assert_valid_workspace
from .common import die
from .common import find_bead_for_update
from .common import refresh_all_box_indexes
from .common import resolve_bead
from .common import verify_with_feedback
from .common import warning

if TYPE_CHECKING:
    from .environment import Environment


# Command-specific sentinels for input commands
ALL_INPUTS = DefaultArgSentinel('all inputs')
SAME_BEAD_NEWEST_VERSION = DefaultArgSentinel('same bead, newest version')
USE_INPUT_NAME = DefaultArgSentinel('use INPUT-NAME')


class CmdInputAdd(Command):
    '''
    Make data from another bead available in the input directory.
    '''

    def declare(self, arg):
        arg(INPUT_NAME.required)
        arg(BEAD_SPEC.with_default(USE_INPUT_NAME))
        arg(WORKSPACE.optional)

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        bead_spec = args.bead_spec
        workspace = get_workspace(args)

        if os.path.dirname(input_name):
            die(f'Invalid input name: {input_name}')

        if bead_spec is USE_INPUT_NAME:
            bead_spec = input_name

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        try:
            bead = resolve_bead(env, bead_spec)
        except LookupError:
            die(f'Not a known bead name: {bead_spec}')

        _check_load_with_feedback(workspace, args.input_name, bead)
        workspace.set_source_name(args.input_name, bead.name)


class CmdDelete(Command):
    '''
    Forget all about an input.
    '''

    def declare(self, arg):
        arg(INPUT_NAME.required)
        arg(WORKSPACE.optional)

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        workspace = get_workspace(args)
        if workspace.has_input(input_name):
            workspace.delete_input(input_name)
            print(f'Input {input_name} is deleted.')
        else:
            die(f'Input {input_name} does not exist')


class CmdMap(Command):
    '''
    Change the name of the bead from which the input is loaded/updated.
    '''

    def declare(self, arg):
        arg(INPUT_NAME.required)
        arg(BEAD_SPEC.with_default(USE_INPUT_NAME))
        arg(WORKSPACE.optional)

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        bead_spec = args.bead_spec
        workspace = get_workspace(args)

        if input_name not in [input_spec.name for input_spec in workspace.inputs]:
            die(f'Unknown input name: {input_name}')

        if bead_spec is USE_INPUT_NAME:
            bead_spec = input_name

        workspace.set_source_name(input_name, bead_spec)
        print(f'Input "{input_name}" mapped to bead "{bead_spec}"')


class CmdUpdate(Command):
    '''
    Update input[s] to newest version or defined bead.

    By default, matches by both input name and kind for precise updates.
    Use --no-kind or --no-name to relax matching constraints.
    '''

    def declare(self, arg):
        arg(INPUT_NAME.with_default(ALL_INPUTS))
        arg(BEAD_SPEC.after(INPUT_NAME, default=SAME_BEAD_NEWEST_VERSION))
        arg(WORKSPACE.optional)
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
        # Safety options
        def add_safety_options(parser):
            parser.argparser.add_argument(
                '--force', action='store_true',
                help='Bypass all safety checks (name, kind, time)')
            parser.argparser.add_argument(
                '--allow-downgrade', action='store_true',
                help='Allow updating to older version')
        arg(add_safety_options)

    def run(self, args, env: 'Environment'):
        if args.input_name is ALL_INPUTS:
            self.update_all_inputs(args, env)
        else:
            self.update_one_input(args, env)

    def update_all_inputs(self, args, env):
        if args.bead_spec is not SAME_BEAD_NEWEST_VERSION:
            die('Too many arguments')

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        workspace = get_workspace(args)
        for input in workspace.inputs:
            # Use mapped bead name for updates (unless --no-name is used)
            use_name = (args.match_strategy != MatchStrategy.KIND_ONLY)
            if use_name:
                bead_spec_to_resolve = workspace.get_source_name(input.name)
            else:
                # KIND_ONLY: don't use any name, just search by kind
                bead_spec_to_resolve = ''
            try:
                archive = find_bead_for_update(
                    env, bead_spec_to_resolve, current_input=input,
                    match_strategy=args.match_strategy
                )
            except LookupError:
                if workspace.is_loaded(input.name):
                    print(
                        f'Skipping update of "{input.name}":'
                        + f' no other candidate found ({input.freeze_time})')
                else:
                    self._no_match_found(input, args, fatal=False)
            else:
                _update_input(workspace, input, archive)
        print('All inputs are up to date.')

    def update_one_input(self, args, env):
        input_name = args.input_name
        bead_spec = args.bead_spec
        workspace = get_workspace(args)
        try:
            input = workspace.get_input(input_name)
        except LookupError:
            die(f'Workspace does not have input "{input_name}"'
                ' - did you want to add it as a new one?')

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        # Determine whether user explicitly specified a bead name (not using default/context)
        explicit_bead_name_given = (bead_spec is not SAME_BEAD_NEWEST_VERSION)

        # Resolve archive using new mini-language
        if bead_spec is SAME_BEAD_NEWEST_VERSION:
            # Use mapped bead name for default updates (unless --no-name is used)
            use_name = (args.match_strategy != MatchStrategy.KIND_ONLY)
            if use_name:
                bead_spec_to_resolve = workspace.get_source_name(input_name)
            else:
                # KIND_ONLY: don't use any name, just search by kind
                bead_spec_to_resolve = ''
            try:
                archive = find_bead_for_update(
                    env, bead_spec_to_resolve, current_input=input,
                    match_strategy=args.match_strategy
                )
            except LookupError:
                self._no_match_found(input, args, fatal=True)
        else:
            # User provided explicit bead spec
            # For relative offsets without explicit name (e.g., @-, @+), prepend mapped name
            from .bead_spec import BeadSpec
            spec = BeadSpec.parse(bead_spec)
            if spec.time and not spec.name:
                # Relative offset without name - use mapped name
                mapped_name = workspace.get_source_name(input_name)
                bead_spec = f'{mapped_name}@{spec.time}'

            try:
                archive = find_bead_for_update(
                    env, bead_spec, current_input=input,
                    match_strategy=args.match_strategy
                )
            except LookupError:
                # Check if this is a kind mismatch by trying without kind constraint
                from .bead_spec import BeadSpec
                spec = BeadSpec.parse(bead_spec)
                if spec.name and args.match_strategy == MatchStrategy.NAME_AND_KIND:
                    try:
                        find_bead_for_update(
                            env, bead_spec, current_input=input,
                            match_strategy=MatchStrategy.NAME_ONLY
                        )
                        # If we got here, it found a bead without kind constraint
                        die(f"Kind (lineage) mismatch: only a different kind of {spec.name} was found. "
                            f"Use --no-kind to allow this update.")
                    except LookupError:
                        # Bead truly doesn't exist, report original error
                        pass
                die(f'Not a known bead name: {bead_spec}')

        # Verify constraints before updating
        # Determine if spec has time constraint (allows downgrade)
        from .bead_spec import BeadSpec
        has_time_constraint = False
        if bead_spec is not SAME_BEAD_NEWEST_VERSION and not os.path.isfile(bead_spec):
            spec = BeadSpec.parse(bead_spec)
            has_time_constraint = bool(spec.time)

        self._verify_archive_constraints(
            input, archive, args, workspace, explicit_bead_name_given, has_time_constraint
        )

        _update_input(workspace, input, archive)
        # Update mapping when user specifies explicit bead (not when using existing mapping)
        if bead_spec is not SAME_BEAD_NEWEST_VERSION:
            workspace.set_source_name(input_name, archive.name)

    @overload
    def _no_match_found(self, input, args, fatal: Literal[True]) -> NoReturn: ...

    @overload
    def _no_match_found(self, input, args, fatal: Literal[False] = False) -> None: ...

    def _no_match_found(self, input, args, fatal=False):
        """Report when no matching bead found during update."""
        match_desc = args.match_strategy.display_name
        msg = f'Could not find bead for "{input.name}" (matching by {match_desc})'
        if args.match_strategy == MatchStrategy.NAME_AND_KIND:
            msg += '. Try --no-kind or --no-name to relax matching'
        (die if fatal else warning)(msg)

    def _verify_archive_constraints(
        self, input: InputSpec, archive: Archive, args, workspace: Workspace,
        explicit_bead_name: bool = False, has_time_constraint: bool = False
    ) -> None:
        """Verify archive meets safety constraints (name, kind, time)."""
        self._verify_name_constraint(input, archive, args, workspace, explicit_bead_name)
        self._verify_kind_constraint(input, archive, args)
        self._verify_time_constraint(input, archive, args, has_time_constraint)

    def _verify_name_constraint(
        self, input: InputSpec, archive: Archive, args, workspace: Workspace, explicit_bead_name: bool
    ) -> None:
        """Verify archive name matches expected bead name.

        Skipped when user explicitly provides a bead name (intentional change).
        """
        if explicit_bead_name:
            return

        mapped_name = workspace.get_source_name(input.name)
        if archive.name != mapped_name:
            # Allow if --no-name (KIND_ONLY strategy) or --force
            no_name_relaxed = (args.match_strategy == MatchStrategy.KIND_ONLY)
            if not (no_name_relaxed or args.force):
                die(f'Name change detected: {mapped_name} → {archive.name}. '
                    f'Use --no-name or --force to allow.')

    def _verify_kind_constraint(self, input: InputSpec, archive: Archive, args) -> None:
        """Verify archive kind matches input kind."""
        if archive.kind != input.kind:
            # Allow if --no-kind (NAME_ONLY strategy) or --force
            no_kind_relaxed = (args.match_strategy == MatchStrategy.NAME_ONLY)
            if not (no_kind_relaxed or args.force):
                die(f'Kind mismatch: expected {input.kind}, got {archive.kind}. '
                    f'Use --no-kind or --force to allow.')

    def _verify_time_constraint(
        self, input: InputSpec, archive: Archive, args, has_time_constraint: bool = False
    ) -> None:
        """Verify archive is not a downgrade (unless explicitly allowed)."""
        allows_downgrade = (
            args.allow_downgrade or
            args.force or
            has_time_constraint  # Time specified in bead spec (e.g., @2024, @-, @+)
        )
        if archive.freeze_time < input.freeze_time:
            if not allows_downgrade:
                die(f'Downgrade detected: {input.freeze_time} → {archive.freeze_time}. '
                    f'Use --allow-downgrade or --force to allow.')


def _update_input(workspace: Workspace, input: InputSpec, archive: Archive) -> None:
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
        arg(INPUT_NAME.with_default(ALL_INPUTS))
        arg(WORKSPACE.optional)

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        workspace = get_workspace(args)

        # Refresh indexes to ensure we have the latest beads
        refresh_all_box_indexes(env)

        if input_name is ALL_INPUTS:
            inputs = workspace.inputs
            if inputs:
                for input in inputs:
                    _load(env, workspace, input)
            else:
                warning('No inputs defined to load.')
        else:
            try:
                input = workspace.get_input(input_name)
            except LookupError:
                die(f'No input with name {input_name}')
            _load(env, workspace, input)


def _load(env: 'Environment', workspace: Workspace, input: InputSpec) -> None:
    assert input is not None
    if not workspace.is_loaded(input.name):
        content_id = input.content_id
        expected_bead_name = workspace.get_source_name(input.name)
        boxes = env.get_boxes()

        # First attempt: search by both name and content_id
        try:
            bead = search(boxes).by_name(expected_bead_name).by_content_id(content_id).first()
            archive = resolve(boxes, bead)
        except LookupError:
            # Second attempt: search by content_id only
            try:
                bead = search(boxes).by_content_id(content_id).first()
                archive = resolve(boxes, bead)
                # Warn about name mismatch
                if bead.name != expected_bead_name:
                    warning(
                        f'Input "{input.name}" expected bead "{expected_bead_name}" '
                        f'but found under name "{bead.name}"'
                    )
            except LookupError:
                warning(f'Could not find bead for input "{input.name}" - not loaded!')
                return

        _check_load_with_feedback(workspace, input.name, archive)
    else:
        print(f'"{input.name}" is already loaded - skipping')


def _check_load_with_feedback(workspace: Workspace, input_name: str, archive: Archive) -> None:
    try:
        verify_with_feedback(archive)
    except InvalidArchive:
        warning(f'Bead for {input_name} is found but damaged - not loading.')
    else:
        if workspace.is_loaded(input_name):
            print(f'Removing current data from {input_name}')
            workspace.unload(input_name)
        print(f'Loading new data to {input_name} ...', end='', flush=True)
        workspace.load(input_name, archive)
        print(' Done')


class CmdUnload(Command):
    '''
    Remove input data.
    '''

    def declare(self, arg):
        arg(INPUT_NAME.with_default(ALL_INPUTS))
        arg(WORKSPACE.optional)

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        workspace = get_workspace(args)
        if input_name is ALL_INPUTS:
            for input in workspace.inputs:
                _unload(workspace, input.name)
        else:
            _unload(workspace, input_name)


def _unload(workspace, input_name):
    if workspace.is_loaded(input_name):
        print('Unloading', input_name, '...', end='', flush=True)
        workspace.unload(input_name)
        print(' Done', flush=True)
    else:
        print(input_name, 'was not loaded - skipping')



def get_workspace(args) -> Workspace:
    assert_valid_workspace(args.workspace)
    return args.workspace
