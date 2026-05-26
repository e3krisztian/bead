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
from .bead_spec import BeadSpec
from .args import INPUT_NAME
from .cmdparse import Command
from .common import MatchStrategy
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

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        bead_spec = args.bead_spec
        workspace = env.get_workspace()

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

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        workspace = env.get_workspace()
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

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        bead_spec = args.bead_spec
        workspace = env.get_workspace()

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
        arg(self._add_matching_options)
        self._add_safety_options(arg)

    @staticmethod
    def _add_matching_options(parser):
        """Add --no-kind/--no-name mutually exclusive matching options.

        NOTE: Option names --no-kind/--no-name chosen for better UX over --ignore-kind/--ignore-name
        as they more clearly communicate what constraint is being relaxed.
        """
        matching_group = parser.argparser.add_mutually_exclusive_group()
        matching_group.add_argument(
            '--no-kind', action='store_const', const=MatchStrategy.NAME_ONLY,
            dest='match_strategy', help='Ignore bead kind when matching (match by name only)')
        matching_group.add_argument(
            '--no-name', action='store_const', const=MatchStrategy.KIND_ONLY,
            dest='match_strategy', help='Ignore bead name when matching (match by kind only)')
        parser.argparser.set_defaults(match_strategy=MatchStrategy.NAME_AND_KIND)

    @staticmethod
    def _add_safety_options(arg):
        """Add --force and --allow-downgrade safety options."""
        arg('--force', action='store_true',
            help='Bypass all safety checks (name, kind, time)')
        arg('--allow-downgrade', action='store_true',
            help='Allow updating to older version')

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

        workspace = env.get_workspace()
        for input in workspace.inputs:
            bead_spec_to_resolve = self._get_default_bead_spec(input.name, workspace, args)
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
        workspace = env.get_workspace()
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
        has_time_constraint = False
        if bead_spec is SAME_BEAD_NEWEST_VERSION:
            bead_spec_to_resolve = self._get_default_bead_spec(input_name, workspace, args)
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
            bead_spec, spec = self._resolve_bead_spec_for_input(bead_spec, input_name, workspace)
            archive = self._find_with_diagnostics(env, bead_spec, input, args, spec)
            # Determine if spec has time constraint (allows downgrade)
            if not os.path.isfile(args.bead_spec):
                has_time_constraint = bool(spec.time)

        # Verify constraints before updating

        self._verify_archive_constraints(
            input, archive, args, workspace, explicit_bead_name_given, has_time_constraint
        )

        _update_input(workspace, input, archive)
        # Update mapping when user specifies explicit bead (not when using existing mapping)
        if bead_spec is not SAME_BEAD_NEWEST_VERSION:
            workspace.set_source_name(input_name, archive.name)

    def _get_default_bead_spec(self, input_name, workspace, args):
        """Get bead spec to use when user doesn't provide one (SAME_BEAD_NEWEST_VERSION).

        Uses mapped bead name unless --no-name is used.
        """
        use_name = (args.match_strategy != MatchStrategy.KIND_ONLY)
        if use_name:
            return workspace.get_source_name(input_name)
        else:
            # KIND_ONLY: don't use any name, just search by kind
            return ''

    @staticmethod
    def _resolve_bead_spec_for_input(bead_spec, input_name, workspace):
        """Resolve bead spec in context of input.

        For relative offsets without explicit name (e.g., @-, @+), prepends the mapped bead name.
        Returns tuple: (resolved_spec_string, parsed_BeadSpec_object)
        """
        spec = BeadSpec.parse(bead_spec)
        if spec.time and not spec.name:
            # Relative offset without name - use mapped name
            mapped_name = workspace.get_source_name(input_name)
            resolved_spec = f'{mapped_name}@{spec.time}'
            return resolved_spec, spec
        return bead_spec, spec

    def _find_with_diagnostics(self, env, bead_spec, input, args, spec):
        """Find bead with helpful error messages on failure.

        Args:
            spec: Already parsed BeadSpec object (to avoid re-parsing)
        """
        try:
            return find_bead_for_update(
                env, bead_spec, current_input=input,
                match_strategy=args.match_strategy
            )
        except LookupError:
            resolved_spec = BeadSpec.parse(bead_spec)
            effective_name = resolved_spec.name or spec.name
            # Diagnose the failure
            if effective_name and args.match_strategy == MatchStrategy.NAME_AND_KIND:
                try:
                    if self._is_kind_mismatch(env, bead_spec, input, args):
                        die(f"Kind (lineage) mismatch: only a different kind of {effective_name} was found. "
                            f"Use --no-kind to allow this update.")
                except LookupError:
                    pass  # Bead truly doesn't exist, will report below
            if resolved_spec.time and effective_name and self._bead_name_exists(env, effective_name, input, args):
                die(f'No version found for "{effective_name}" matching @{resolved_spec.time}')
            die(f'Not a known bead name: {bead_spec}')

    def _verify_archive_constraints(
        self, input: InputSpec, archive: Archive, args, workspace: Workspace,
        explicit_bead_name: bool = False, has_time_constraint: bool = False
    ) -> None:
        """Verify archive meets safety constraints (name, kind, time)."""
        self._verify_name_constraint(input, archive, args, workspace, explicit_bead_name)
        self._verify_kind_constraint(input, archive, args)
        self._verify_time_constraint(input, archive, args, has_time_constraint)

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

    def _is_kind_mismatch(self, env, bead_spec, input, args):
        """Check if bead exists with same name but different kind.

        Returns True only if we find a bead that matches by name but has a different kind.
        Raises LookupError if no bead found at all.
        """
        archive = find_bead_for_update(
            env, bead_spec, current_input=input,
            match_strategy=MatchStrategy.NAME_ONLY
        )
        # Found a bead - check if it has different kind
        return archive.kind != input.kind

    def _bead_name_exists(self, env, name, input, args):
        try:
            find_bead_for_update(env, name, current_input=input, match_strategy=MatchStrategy.NAME_ONLY)
            return True
        except LookupError:
            return False

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

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        workspace = env.get_workspace()

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

    def run(self, args, env: 'Environment'):
        input_name = args.input_name
        workspace = env.get_workspace()
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
