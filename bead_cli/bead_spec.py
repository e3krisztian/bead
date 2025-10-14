"""
Bead reference mini-language parser.

Parses bead specifications in the format: [[box:]name][@time]

Examples:
    hotel-dataset           # name only
    home:hotel-dataset      # box + name
    hotel-dataset@2024-06-15   # name + time
    home:hotel-dataset@2024    # all three
    home:                   # box only (context name)
    @2024                   # time only (context name)
    home:@-                 # box + time (context name)
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class BeadSpec:
    """
    Parsed bead specification.

    All fields are strings (never None). Empty string indicates omitted/context value.

    Attributes:
        name: Bead name, or "" to use context (INPUT_NAME)
        box: Box name, or "" for all boxes
        time: Time expression, or "" for latest
        is_file_path: True if this is a direct file reference
        file_path: File path if is_file_path=True, otherwise ""
    """
    name: str
    box: str
    time: str
    is_file_path: bool
    file_path: str


def parse_bead_spec(spec: str) -> BeadSpec:
    """
    Parse a bead specification string.

    Grammar:
        BEAD_SPEC ::= FILE_PATH | QUALIFIED_NAME
        QUALIFIED_NAME ::= [ BOX ":" ] NAME [ "@" TIME_EXPR ]

    Args:
        spec: Bead specification string

    Returns:
        BeadSpec with parsed components

    Examples:
        >>> parse_bead_spec("hotel-dataset")
        BeadSpec(name="hotel-dataset", box="", time="", is_file_path=False, file_path="")

        >>> parse_bead_spec("home:hotel-dataset")
        BeadSpec(name="hotel-dataset", box="home", time="", is_file_path=False, file_path="")

        >>> parse_bead_spec("hotel-dataset@2024")
        BeadSpec(name="hotel-dataset", box="", time="2024", is_file_path=False, file_path="")

        >>> parse_bead_spec("/path/to/bead.zip")
        BeadSpec(name="", box="", time="", is_file_path=True, file_path="/path/to/bead.zip")
    """
    # Check for file path
    if '/' in spec or '\\' in spec or spec.endswith('.zip'):
        return BeadSpec(
            name="",
            box="",
            time="",
            is_file_path=True,
            file_path=spec
        )

    # Parse qualified name: [[box:]name][@time]
    # Strategy: Find '@' first (time separator), then ':' in the prefix (box separator)
    # This avoids confusion with ':' in timestamps (e.g., 2024-06-15T14:30:45)

    if '@' in spec:
        # Has time separator
        # Split at first '@' to separate name/box part from time part
        prefix, time = spec.split('@', 1)

        if ':' in prefix:
            # box:name@time or box:@time
            box, name = prefix.split(':', 1)
            return BeadSpec(
                name=name,
                box=box,
                time=time,
                is_file_path=False,
                file_path=""
            )
        else:
            # name@time or @time (empty name)
            return BeadSpec(
                name=prefix,
                box="",
                time=time,
                is_file_path=False,
                file_path=""
            )

    elif ':' in spec:
        # Has box separator but no time
        # box:name or box:
        box, name = spec.split(':', 1)
        return BeadSpec(
            name=name,
            box=box,
            time="",
            is_file_path=False,
            file_path=""
        )

    else:
        # Just name
        return BeadSpec(
            name=spec,
            box="",
            time="",
            is_file_path=False,
            file_path=""
        )


def parse_relative_offset(expr: str) -> int:
    """
    Parse a relative version offset expression.

    Supports:
        - Shorthand: -, --, ---, ---- (1-4 repetitions)
        - Explicit: -5, -10, etc.
        - Plus: +, ++, +++, +5, etc.
        - Git alias: ^, ^^, ^^^, ^5, etc. (equivalent to minus)

    Args:
        expr: Relative offset expression

    Returns:
        Negative integer for backwards, positive for forwards

    Examples:
        >>> parse_relative_offset("-")
        -1
        >>> parse_relative_offset("---")
        -3
        >>> parse_relative_offset("-5")
        -5
        >>> parse_relative_offset("+")
        1
        >>> parse_relative_offset("++")
        2
        >>> parse_relative_offset("^")
        -1
        >>> parse_relative_offset("^^")
        -2

    Raises:
        ValueError: If expression is invalid
    """
    if not expr:
        raise ValueError("Empty relative offset expression")

    direction = expr[0]
    if direction not in ('-', '+', '^'):
        raise ValueError(f"Invalid relative offset: must start with -, +, or ^: {expr}")

    # Check if all characters are the same (shorthand form)
    if all(c == direction for c in expr):
        count = len(expr)
    else:
        # Explicit number form
        try:
            count = int(expr[1:])
        except ValueError:
            raise ValueError(f"Invalid relative offset: {expr}")

    # Convert ^ to - (git alias)
    if direction == '^':
        direction = '-'

    # Return signed count
    return -count if direction == '-' else count


def is_relative_offset(expr: str) -> bool:
    """
    Check if a time expression is a relative offset.

    Args:
        expr: Time expression string

    Returns:
        True if expression starts with -, +, or ^
    """
    return bool(expr) and expr[0] in ('-', '+', '^')
