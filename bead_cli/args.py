"""
Argument declarers and related constants for bead CLI.

This module consolidates all argument-related definitions:
- Argument declarer classes (BEAD_SPEC, INPUT_NAME, WORKSPACE, BOX_NAME)
- DefaultArgSentinel class for creating argument defaults
- Help text and metavar constants (inline in declarers)
"""
from bead.workspace import Workspace

from .autocomplete import complete_bead_spec, complete_input_name, complete_box_name


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


def _arg_bead_spec(parser, arg_name, nargs, default):
    '''
    Internal helper for declaring bead_spec argument.
    '''
    action = parser.arg(
        arg_name, metavar='BEAD', help='''which bead to use - supports mini-language syntax:

Syntax: [[box:]name][@time] | file_path

Basic forms:
  - bead name: "hotel-dataset"
  - box + name: "archive:hotel-dataset"
  - file path: "/path/to/bead.zip"

Time constraints:
  - partial timestamp: "hotel-dataset@2024-06-15"
  - year/month: "hotel-dataset@2024" or "hotel-dataset@2024-06"
  - with box: "archive:hotel-dataset@2024-06-15"

Relative versions:
  - previous: "hotel-dataset@-" (or @--, @---, @----)
  - next: "hotel-dataset@+" (or @++, @+++, @++++)
  - explicit count: "hotel-dataset@-5" or "hotel-dataset@+10"

Context-based (for input commands):
  - box only: "archive:" (uses input name)
  - time only: "@2024-06-15" (uses input name)
  - combined: "archive:@-" (uses input name, from archive box, previous version)
''',
        nargs=nargs, type=str, default=default)
    action.completer = complete_bead_spec
    return action


class BEAD_SPEC:
    """Argument declarer for bead specifications.

    Provides variations for different argument parsing scenarios:
    - required: Mandatory bead spec (no default)
    - with_default(value): Optional bead spec with default value
    - after(required_arg, default): Bead spec that completes after another arg
    """

    ARG_NAME = 'bead_spec'

    @classmethod
    def required(cls, parser):
        """Declare required bead_spec argument."""
        return _arg_bead_spec(parser, cls.ARG_NAME, nargs=None, default=None)

    @classmethod
    def with_default(cls, name):
        """Create bead_spec declarer with default value."""
        def declare(parser):
            return _arg_bead_spec(parser, cls.ARG_NAME, nargs='?', default=name)
        return declare

    @classmethod
    def after(cls, required_arg, default):
        """Create bead_spec declarer that completes after another argument."""
        from .autocomplete import after_arg
        attr_name = required_arg.ARG_NAME
        def declare(parser):
            action = parser.arg(cls.ARG_NAME, type=str, nargs='?', default=default)
            action.completer = after_arg(attr_name, complete_bead_spec, parser.argparser)
            return action
        return declare


class INPUT_NAME:
    """Argument declarer for input names.

    Provides variations for different argument parsing scenarios:
    - required: Mandatory input name (no default)
    - with_default(value): Optional input name with default value
    """

    ARG_NAME = 'input_name'

    @classmethod
    def required(cls, parser):
        """Declare required input_name argument."""
        action = parser.arg(
            cls.ARG_NAME,
            metavar='INPUT-NAME',
            help='name of input, its workspace relative location is "input/%(metavar)s"')
        action.completer = complete_input_name
        return action

    @classmethod
    def with_default(cls, default):
        """Create input_name declarer with default value."""
        def declare(parser):
            action = parser.arg(
                cls.ARG_NAME, type=str, nargs='?', default=default,
                metavar='INPUT-NAME',
                help='name of input, its workspace relative location is "input/%(metavar)s"')
            action.completer = complete_input_name
            return action
        return declare


class WORKSPACE:
    """Argument declarer for workspace paths.

    Provides variations for different argument parsing scenarios:
    - required: Mandatory workspace directory (no default)
    - with_default(value): Optional positional with custom default value
    """

    ARG_NAME = 'workspace'

    @classmethod
    def required(cls, parser):
        """Declare required workspace argument."""
        return parser.arg(
            cls.ARG_NAME,
            metavar='DIRECTORY',
            type=Workspace,
            help='bead and directory to create')

    @classmethod
    def with_default(cls, default_workspace):
        """Create workspace declarer with custom default value."""
        def declare(parser):
            parser.arg(
                cls.ARG_NAME, nargs='?', type=Workspace,
                default=default_workspace,
                metavar='DIRECTORY', help='workspace directory')
        return declare


class BOX_NAME:
    """Argument declarer for box names.

    Provides variations for different argument parsing scenarios:
    - required: Mandatory box name (no default)
    - with_default(value): Optional box name with default value
    """

    ARG_NAME = 'box_name'

    @classmethod
    def required(cls, parser):
        """Declare required box_name argument."""
        action = parser.arg(
            cls.ARG_NAME,
            metavar='BOX-NAME',
            help='Name of box to store bead')
        action.completer = complete_box_name
        return action

    @classmethod
    def with_default(cls, default):
        """Create box_name declarer with default value."""
        def declare(parser):
            action = parser.arg(
                cls.ARG_NAME, type=str, nargs='?', default=default,
                metavar='BOX-NAME',
                help='Name of box to store bead')
            action.completer = complete_box_name
            return action
        return declare
