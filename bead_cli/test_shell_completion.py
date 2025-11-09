"""
Integration tests for shell tab completion using pexpect.

WHY THESE TESTS EXIST
======================

These tests verify real shell integration that unit tests cannot catch. During initial
development, unit tests (test_autocomplete.py) passed while actual completion failed in
bash and zsh, and never worked in fish. The integration stack is complex:
  PTY ↔ Shell ↔ argcomplete ↔ completion code

Without these tests, there is no way to verify that:
- Bash and zsh actually invoke our completion code correctly
- Special characters (:, @) in COMP_WORDBREAKS are handled properly
- Incremental completion works for complex bead specs (box:name@time)
- The completion "feels right" in real terminal environments

These tests were essential tools during development to debug and iterate on the
integration, not just verification after the fact. Manual testing across environments
(Linux bash, Linux zsh, macOS bash, macOS zsh) is not feasible.

WHAT THESE TESTS VERIFY
========================

Bead spec completion is complex due to special separator characters:
- box:name@time format with : and @ as delimiters
- Both : and @ are in bash's COMP_WORDBREAKS by default
- Requires incremental completion at each separator
- Different shells handle wordbreaks differently

Test coverage includes:
- Basic command completion (bead inp → bead input)
- Box name completion (box:)
- Bead name completion (box:name)
- Time expression completion (@latest, @YYYY-MM-DD)
- Cursor positioning in middle of specs
- Multiple candidates with shared prefixes
- Each test verifies distinct bead spec parsing and completion behavior

ACKNOWLEDGED TRADE-OFFS
========================

These tests are:
✓ Necessary - no alternative for real shell integration verification
✓ Comprehensive - each tests distinct bead spec behavior with special separators
✗ Slow - ~35 seconds (600x slower than unit tests, but acceptable for coverage)
✗ Fragile - platform differences, timing issues, complex buffer management
✗ High maintenance - constant breakage and fixes, complex workarounds needed

The pain is unavoidable for ensuring the complex bead spec completion works correctly
in production shells. Unit tests provide fast feedback; these tests provide confidence.

TECHNICAL APPROACH
==================

Uses raw pexpect for complete control over shell interaction and consistent
buffer management. Tests real shell completion behavior by driving interactive
shells (bash, zsh).

BASH VERSION REQUIREMENTS
==========================

Full test coverage requires bash 4.0+ for mid-word completion support. The
skip-completed-text readline feature was introduced in bash 4.0 and is required
for cursor-in-middle completion tests.

macOS system bash is version 3.2 (from 2007, GPLv2) which lacks this feature.
Tests automatically:
- Prefer homebrew bash (5.x) on macOS when available
- Skip cursor-in-middle tests on bash 3.2 with clear message
- Full coverage on Linux (typically bash 4.x or 5.x)
"""

import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import pexpect
import pytest

from bead.ziparchive import ZipArchive

# Type alias for pexpect match result
MatchType = re.Match[str]

# Skip entire test module on Windows - pexpect requires Unix PTY support
pytestmark = pytest.mark.skipif(
    sys.platform == 'win32',
    reason="Shell completion tests require Unix shells and pexpect (not available on Windows)"
)

# Magic control characters
TAB = '\t'
BELL = '\x07'  # Completion feedback tone
BACKSPACE = '\x08'  # Character deletion
CTRL_A = '\x01'  # Move to start of line
CTRL_E = '\x05'  # Move to end of line
CTRL_F = '\x06'  # Move cursor forward (right) one character
CTRL_B = '\x02'  # Move cursor backward (left) one character
ESC = '\x1b'  # Escape key
NEWLINE = '\n'


# Check if a shell is available on the system
def _shell_available(shell_name):
    """Check if a shell is available on the system.

    Args:
        shell_name: Name of shell (e.g., 'bash', 'zsh', 'fish')

    Returns:
        bool: True if shell is available
    """
    try:
        subprocess.run(
            [shell_name, "--version"],
            capture_output=True,
            timeout=2,
            check=True
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _find_best_bash():
    """Find the best bash executable to use.

    On macOS, prefer homebrew bash (5.x) over system bash (3.2) for better
    completion support including mid-word completion with skip-completed-text.

    Returns:
        str: Path to bash executable, or 'bash' to use PATH
    """
    if sys.platform != 'darwin':
        return 'bash'  # Use PATH on non-macOS

    # On macOS, try homebrew locations first (bash 5.x)
    for path in ['/opt/homebrew/bin/bash', '/usr/local/bin/bash']:
        if os.path.exists(path):
            return path

    # Fall back to system bash (3.2)
    return 'bash'


def _get_bash_version(bash_path='bash'):
    """Get bash major version number.

    Args:
        bash_path: Path to bash executable

    Returns:
        int or None: Major version number, or None if detection fails
    """
    try:
        result = subprocess.run(
            [bash_path, '--version'],
            capture_output=True,
            text=True,
            timeout=2
        )
        # Parse version from first line: "GNU bash, version 5.2.37(1)-release"
        match = re.search(r'version (\d+)\.', result.stdout)
        if match:
            return int(match.group(1))
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


@dataclass
class ShellConfig:
    """Configuration for shell-specific completion testing behavior.

    Contains all shell-specific settings including completion behavior,
    initialization arguments, and prompt configuration.
    """
    init_args: str
    """Arguments to pass when starting the shell"""

    prompt_pattern: str
    """Regex pattern for detecting the shell's default prompt"""

    prompt_change: str
    """Command to change prompt for pexpect (e.g., 'PS1="..."')"""

    extra_init: str
    """Additional initialization commands (e.g., 'set +o history')"""

    env_overrides: dict = field(default_factory=dict)
    """Environment variables to override for this shell (e.g., {'TERM': 'dumb'})"""

    @property
    def tab(self) -> str:
        """Single tab character."""
        return TAB

    @property
    def ctrl_a(self) -> str:
        """Control-A (move to beginning of line) character."""
        return CTRL_A


# Shell-specific configurations
SHELL_CONFIGS = {
    'bash': ShellConfig(
        init_args='--norc',
        prompt_pattern=r'\$',
        prompt_change='PS1="[PEXPECT_PROMPT>"',
        extra_init='set +o history',  # Disable history to avoid noise in completion output
        env_overrides={'TERM': 'dumb'},
    ),
    'zsh': ShellConfig(
        init_args='--no-rcs',
        prompt_pattern=r'%',
        prompt_change='PROMPT="[PEXPECT_PROMPT>"',
        # Initialize zsh completion and set options for consistent behavior across versions:
        # - autoload -U compinit: Load completion system
        # - compinit -u: Initialize completion without security checks (test environment)
        # - BASH_AUTO_LIST: Display completions bash-style for consistency
        # - NO_AUTO_MENU: Don't auto-cycle through completions (prevents unwanted 2nd completion)
        # - LIST_AMBIGUOUS: List completions when ambiguous
        extra_init='autoload -U compinit && compinit -u && setopt BASH_AUTO_LIST NO_AUTO_MENU LIST_AMBIGUOUS',
        env_overrides={'TERM': 'dumb'},
    ),
    'fish': ShellConfig(
        init_args='--no-config --private',
        prompt_pattern=r'> ',  # fish's simple prompt with TERM=dumb
        prompt_change='function fish_prompt; echo "[PEXPECT_PROMPT]>"; end',
        extra_init='',  # fish doesn't need compinit-like setup
        env_overrides={'TERM': 'dumb'},
    ),
}


@pytest.fixture(scope="module", autouse=True)
def home_env(tmp_path_factory):
    """Set up temporary bead environment with test box and bead.

    Creates a HOME directory with a test box named 'box' and a test bead
    named 'name' saved in that box. Returns environment dict for use with
    subprocess and pexpect.
    """
    # Create temporary HOME directory
    temp_home = tmp_path_factory.mktemp("bead_home")

    # Create environment with temp HOME
    env = os.environ.copy()
    env['HOME'] = str(temp_home)

    # Create debug log file for completion diagnostics
    debug_log = temp_home / "completion_debug.log"
    env['BEAD_COMPLETION_DEBUG'] = str(debug_log)

    # Create a test workspace directory
    workspace_dir = temp_home / "test_workspace"
    workspace_dir.mkdir()

    # Create initial bead: bead new name
    subprocess.run(
        ["bead", "new", "name"],
        cwd=str(workspace_dir),
        env=env,
        check=True,
        timeout=10
    )

    # Enter the bead workspace
    name_workspace = workspace_dir / "name"

    # Create a box directory
    box_dir = temp_home / "box"
    box_dir.mkdir()

    # Add the box to bead
    subprocess.run(
        ["bead", "box", "add", "box", str(box_dir)],
        env=env,
        check=True,
        timeout=10
    )

    # Save the bead to the 'box' box
    subprocess.run(
        ["bead", "save"],
        cwd=str(name_workspace),
        env=env,
        check=True,
        timeout=10
    )

    # Index the box so autocomplete can find beads
    subprocess.run(
        ["bead", "box", "index", "box"],
        env=env,
        check=True,
        timeout=10
    )

    # Verify bead was saved to box by checking for name_*.zip file
    bead_files = list(box_dir.glob("name_*.zip"))
    if not bead_files:
        raise RuntimeError(f"No bead file found in box directory {box_dir}")

    return env


@pytest.fixture
def completion_debug_log(home_env):
    """Return the Path to the completion debug log file.

    Cleans up the log file after the test.
    """
    debug_log = Path(home_env['BEAD_COMPLETION_DEBUG'])
    try:
        yield debug_log
    finally:
        # Clean up debug log after test
        if debug_log.exists():
            debug_log.unlink()


@pytest.fixture
def bead_freeze_time(home_env):
    """Return the freeze_time of the test bead as a datetime object.

    Reads the bead archive metadata to get the exact timestamp when the bead was created,
    avoiding any race condition with the system clock.
    """
    # Find the bead file in the test box
    box_dir = Path(home_env['HOME']) / 'box'
    bead_files = list(box_dir.glob('name_*.zip'))
    if not bead_files:
        raise RuntimeError(f"No bead file found in box directory {box_dir}")

    # Open the bead archive and read its freeze_time
    bead_file = bead_files[0]
    archive = ZipArchive(str(bead_file))
    return archive.freeze_time


class ShellTester:
    """Integration test harness for shell completion testing with pexpect.

    Supports multiple shells (bash, zsh). Uses low-level pexpect API for all
    shell interactions to ensure consistent buffer management and avoid
    synchronization issues.
    """

    def __init__(self, env, shell_name, debug=False):
        """Initialize tester.

        Args:
            env: Environment dict (from home_env fixture)
            shell_name: Name of shell ('bash', 'zsh')
            debug: If True, print debug info
        """
        self.env = env
        self.shell_name = shell_name
        self.config = SHELL_CONFIGS[shell_name]
        if not self.config:
            raise ValueError(f"Unknown shell: {shell_name}")
        self.debug = debug
        self.shell: pexpect.pty_spawn.spawn | None = None
        self.bash_version: int | None = None  # Will be set during setup for bash

    def setup(self):
        """Start shell and configure completion."""
        # Apply env_overrides from config
        env = self.env.copy()
        env.update(self.config.env_overrides)

        # For bash on macOS, prefer homebrew bash (5.x) over system bash (3.2)
        if self.shell_name == 'bash':
            bash_path = _find_best_bash()
            shell_cmd = f"{bash_path} {self.config.init_args}"
            # Detect bash version
            self.bash_version = _get_bash_version(bash_path)
        else:
            shell_cmd = f"{self.shell_name} {self.config.init_args}"

        # Use pure pexpect instead of pexpect.replwrap for complete control over
        # echo settings and buffer management. replwrap couldn't reliably disable
        # echo (causing command text in output) and couldn't handle zsh's async
        # control codes sent after prompts. Manual setup with setecho(False) and
        # explicit prompt configuration provides the control needed for reliable testing.
        # Start shell in temp HOME directory to isolate test artifacts
        self.shell = pexpect.spawn(shell_cmd, timeout=5, encoding='utf-8', env=env, cwd=env['HOME'], echo=False)

        # Disable echo if it's enabled (like replwrap does)
        if self.shell.echo:
            self.shell.setecho(False)
            self.shell.waitnoecho()

        # Wait for initial prompt
        self.shell.expect(self.config.prompt_pattern, timeout=5.0)

        # Change prompt
        prompt_cmd = self.config.prompt_change
        self.shell.send(prompt_cmd + '\n')
        # Wait for new prompt to appear
        new_prompt = prompt_cmd.split('"')[1]
        self.shell.expect_exact(new_prompt, timeout=5.0)

        # Run extra init commands
        if self.config.extra_init:
            self.shell.send(self.config.extra_init + '\n')
            self.shell.expect_exact(new_prompt, timeout=5.0)

        # Clear buffers after initialization
        self.shell.buffer = ''
        self.shell.before = ''
        self.shell.after = ''

        # Load bead completion
        if self.shell_name == 'fish':
            # For fish, pipe completion script directly to source
            self.send_command(f'bead completion {self.shell_name} | source')
        else:
            self.send_command(f'eval "$(bead completion {self.shell_name})"')

        # Setup bead workspace
        self.send_command('bead new workspace')
        self.send_command('cd workspace')

    def cleanup(self):
        """Clean up shell session."""
        if self.shell:
            try:
                self.send_command('bead nuke')
                self.shell.send("exit\n")
                self.shell.expect(pexpect.EOF, timeout=2)
            except Exception:
                pass

    def send_command(self, cmd):
        """Send command and wait for prompt using low-level pexpect API.

        Uses direct child.send() to ensure consistent buffer state throughout
        all shell interactions. This maintains the same API level as the
        completion methods for predictable buffer management.

        Args:
            cmd: Command to run

        Returns:
            str: Command output (text between command and prompt)
        """
        assert self.shell is not None
        prompt = self.config.prompt_change.split('"')[1]
        self.shell.send(cmd + '\n')
        self.shell.expect_exact(prompt, timeout=5.0)
        output = self.shell.before

        # Clear buffers
        self.shell.buffer = ''
        self.shell.before = ''
        self.shell.after = ''

        return output

    def complete(self, partial_input, *, cursor_offset=0, cursor='', rstrip=True):
        """Test completion of bead command using tab key.

        Works identically for both bash and zsh using low-level pexpect API:
        1. Sends partial command
        2. Sends TAB to trigger completion
        3. Inserts cursor marker string at cursor position (if provided)
        4. Uses CTRL_A + echo 'line' + CTRL_E to capture with single quotes
        5. Single quotes preserve all content (spaces, special chars)
        6. Processes result: optionally rstrips

        Args:
            partial_input: The partial command line (e.g., "bead inp")
            cursor_offset: Offset from end of string where cursor should be positioned.
                          0 means cursor at end (default). Positive values move cursor
                          left from the end. For example, cursor_offset=5 with input
                          "bead input" would place cursor after "bead in".
            cursor: String to insert at cursor position after completion.
                   Empty string (default) inserts nothing.
                   E.g., '=CURSOR=' marks cursor position in result.
            rstrip: If True (default), strip trailing whitespace from result.

        Returns:
            dict: {'completed_line': str, 'success': True/False, ...}
        """
        assert self.shell is not None
        try:
            prompt = self.config.prompt_change.split('"')[1]

            # Clear pexpect's internal buffers before starting completion
            self.shell.buffer = ''
            self.shell.before = ''
            self.shell.after = ''

            # Send the full input line
            self.shell.send(partial_input)

            # Move cursor left from end by cursor_offset positions using CTRL_B.
            # Enables testing mid-word completion. No conditionals needed:
            # backward movement at EOL works naturally in both bash and zsh.
            for _ in range(cursor_offset):
                self.shell.send(CTRL_B)

            # Send TAB to trigger completion
            self.shell.send(TAB)

            # Insert cursor marker at current position (right after completion)
            # This marks exactly where the shell placed the cursor after TAB
            if cursor:
                self.shell.send(cursor)

            # Use CTRL_A to move to start of line, then echo with quoted line to preserve all content
            # Without quotes, `echo foo ` outputs just `foo` (space is lost)
            # With quotes, `echo 'foo '` outputs `foo ` (space preserved)
            # The =CURSOR= marker shows cursor position and protects content from rstrip
            self.shell.send(CTRL_A + "echo COMPLETED LINE='")

            # Move to end of line, close the quote, and execute
            # After CTRL_A + type, cursor is before the completed line
            # CTRL_E moves cursor to end of line, then we add closing quote
            self.shell.send(CTRL_E + "'" + NEWLINE)

            # DEBUG: Show buffer state before expect
            if self.debug:
                print(f"\n[DEBUG] Buffer before expect: {repr(self.shell.buffer)}")

            # Marker-based synchronization: expect() waits for known marker while
            # consuming async control codes (e.g., zsh bracketed paste sequences).
            # Replaces timeout-based draining which was unreliable and ~10s slower.
            #
            # Negative lookbehind filters echoed input (macOS ignores echo=False).
            # (?<!echo ) succeeds when "echo " NOT present, including at buffer start.
            # - Filters: "echo COMPLETED LINE=" (echoed command) won't match
            # - Matches: "COMPLETED LINE=bead input" (actual output) will match
            # Tried positive lookbehind (?<=[\r\n]) but caused CI timeouts when
            # newline wasn't in pexpect's buffer window.
            self.shell.expect(r'(?<!echo )COMPLETED LINE=(?P<line>[^\r\n]+)', timeout=2)

            # Ensure match succeeded and is not EOF/TIMEOUT
            assert self.shell.match is not None
            assert self.shell.match is not pexpect.EOF
            assert self.shell.match is not pexpect.TIMEOUT
            match = cast(MatchType, self.shell.match)

            # DEBUG: Show what was matched
            if self.debug:
                print(f"[DEBUG] Match object: {match}")
                print(f"[DEBUG] Match groups: {match.groups()}")
                print(f"[DEBUG] Match groupdict: {match.groupdict()}")
                print(f"[DEBUG] Before buffer: {repr(self.shell.before)}")
                print(f"[DEBUG] After buffer: {repr(self.shell.after)}")

            completed_line = match.group('line')

            # DEBUG: Show raw captured line before any processing
            if self.debug:
                print(f"\n[COMPLETION DEBUG] {self.shell_name} - Input: {partial_input}")
                print(f"[COMPLETION DEBUG] Raw captured line: {repr(completed_line)}")
                print(f"[COMPLETION DEBUG] cursor={repr(cursor)}, rstrip={rstrip}")

            # Process the result: apply rstrip if requested
            if rstrip:
                completed_line = completed_line.rstrip()

            if self.debug:
                print(f"[COMPLETION DEBUG] Processed line: {repr(completed_line)}")

            # Then wait for prompt
            self.shell.expect_exact(prompt, timeout=2)

            return {'completed_line': completed_line, 'success': True}

        except Exception as e:
            if self.debug:
                print(f"\n[COMPLETION ERROR] {self.shell_name}: {e}")
                print(f"[COMPLETION ERROR] Buffer was: {repr(self.shell.before)}")
            return {'error': str(e), 'success': False}


@pytest.fixture(params=['bash', 'zsh', 'fish'], scope="module")
def shell_tester(request, home_env):
    """Provide ShellTester instance for completion tests.

    Parametrized to run tests for both bash and zsh shells.
    Skips if the shell is not available on the system.

    ZSH Implementation Notes:
    - Uses separate TAB sends (not embedded in command string)
    - Pexpect's default zsh() helper has bugs (looks for bash $ prompt)
    - Our manual setup with PROMPT variable works correctly
    - Shell initialization: autoload -U compinit && compinit -u
    - ZSH option: setopt BASH_AUTO_LIST (makes completion display like bash)
    - Tab behavior: double TAB like bash (first shows menu, second selects)
    - Output parsing: Strip ANSI with regex, remove backspace and bell sequences
    - Uses Ctrl-A + echo pattern like bash to capture completed line
    - Fundamental difference: ZSH's completion menu doesn't auto-insert into line
      the way bash does, requiring workarounds for automated testing
    """
    shell = request.param
    if not _shell_available(shell):
        pytest.skip(f"{shell} not available")

    tester = ShellTester(home_env, shell, debug=True)
    tester.setup()
    try:
        yield tester
    finally:
        tester.cleanup()


def test_input_command_not_directory(shell_tester):
    """Test that 'bead inpu' completes to 'input' command, not 'input/' directory.

    In a bead workspace (created by shell_tester fixture), there's an input/
    directory. This test verifies that argcomplete completes to the 'input'
    command rather than treating it as filesystem completion to 'input/'
    directory (with slash).

    This is a regression test: if argcomplete incorrectly does filesystem
    completion, it would complete to 'bead input/=CURSOR=' instead of
    'bead input =CURSOR='.

    Uses cursor='=CURSOR=' to mark cursor position, allowing us to verify
    there's no slash in the completion.
    """
    # Complete 'bead inpu' with cursor marker
    result = shell_tester.complete("bead inpu", cursor='=CURSOR=')
    assert result['success']

    # Should complete to 'bead input =CURSOR=' (command with space before cursor)
    # NOT 'bead input/=CURSOR=' (directory with slash)
    assert result['completed_line'] == 'bead input =CURSOR=', \
        f"Expected 'bead input =CURSOR=' but got: {result['completed_line']}"


def test_box_name_completion(shell_tester, completion_debug_log):
    """Test completing bead specs in 'bead input add' context.

    The test bead 'name' from box 'box' should appear in completions.
    """
    result = shell_tester.complete("bead input add test box:")
    assert result['success']

    # Check if completion function was called
    if completion_debug_log.exists():
        debug_content = completion_debug_log.read_text()
        print(f"\n[DEBUG LOG]\n{debug_content}\n")
        # Verify that complete_bead_spec was called
        assert "[ENTRY]" in debug_content, "Completion function was not called"
    else:
        print(f"\n[NO DEBUG LOG] File not created at: {completion_debug_log}")

    # Should complete to 'bead input add test box:name'
    assert result['completed_line'] == 'bead input add test box:name', \
        f"Expected 'bead input add test box:name' but got: {result['completed_line']}"


def test_colon_separator_in_completion(shell_tester):
    """Test that completing with colon separator works.

    This tests the key wordbreak issue: when typing 'box:name',
    bash splits on ':' (COMP_WORDBREAKS includes ':').
    The completion should handle this and suggest beads.
    """
    # Try to complete with partial bead name after colon
    result = shell_tester.complete("bead input add test box:na")
    assert result['success']
    # Should complete to 'bead input add test box:name'
    assert result['completed_line'] == 'bead input add test box:name', \
        f"Expected 'bead input add test box:name' but got: {result['completed_line']}"


def test_at_separator_in_completion(shell_tester, completion_debug_log):
    """Test that completing with @ separator works.

    The completion should handle @ and suggest timestamps.
    """
    # Try to complete partial timestamp after @
    result = shell_tester.complete("bead input add test box:name@la")
    assert result['success']

    # Check debug log
    if completion_debug_log.exists():
        debug_content = completion_debug_log.read_text()
        print(f"\n[DEBUG LOG]\n{debug_content}\n")

    # Should complete to 'bead input add test box:name@latest'
    assert result['completed_line'] == 'bead input add test box:name@latest', \
        f"Expected 'bead input add test box:name@latest' but got: {result['completed_line']}"


def test_bead_name_with_time_completion(shell_tester, completion_debug_log):
    """Test completing bead name with time expression (no box prefix).

    Tests if @ completion works without a box: prefix.
    """
    result = shell_tester.complete("bead input add test name@lat")
    assert result['success']
    assert result['completed_line'] == 'bead input add test name@latest'


def test_input_name_completion_delete(shell_tester):
    """Test completing input names for 'bead input delete' command.

    First add an input, then verify we can complete its name.
    """
    # Add an input using low-level API to avoid buffer sync issues
    shell_tester.send_command("bead input add training-data name")

    # Test completion of the input name
    result = shell_tester.complete("bead input delete train")
    assert result['success']
    # Should complete to 'bead input delete training-data'
    assert result['completed_line'] == 'bead input delete training-data', \
        f"Expected 'bead input delete training-data' but got: {result['completed_line']}"


def test_input_name_completion_update(shell_tester):
    """Test completing input names for 'bead input update' command.

    Tests optional input name completion.
    """
    # Add an input using low-level API to avoid buffer sync issues
    shell_tester.send_command("bead input add test-input name")

    # Test completion of the optional input name
    result = shell_tester.complete("bead input update test")
    assert result['success']
    # Should complete to 'bead input update test-input'
    assert result['completed_line'] == 'bead input update test-input', \
        f"Expected 'bead input update test-input' but got: {result['completed_line']}"


def test_box_name_completion_save(shell_tester):
    """Test completing box names for 'bead save' command.

    The test box 'box' should be available for completion.
    """
    result = shell_tester.complete("bead save bo")
    assert result['success']
    # Should complete to 'bead save box'
    assert result['completed_line'] == 'bead save box', \
        f"Expected 'bead save box' but got: {result['completed_line']}"


def test_box_name_completion_forget(shell_tester):
    """Test completing box names for 'bead box forget' command.

    The test box 'box' should be available for completion.
    """
    result = shell_tester.complete("bead box forget bo")
    assert result['success']
    # Should complete to 'bead box forget box'
    assert result['completed_line'] == 'bead box forget box', \
        f"Expected 'bead box forget box' but got: {result['completed_line']}"


def test_box_name_completion_enable(shell_tester):
    """Test completing box names for 'bead box enable' command.

    The test box 'box' should be available for completion.
    """
    result = shell_tester.complete("bead box enable bo")
    assert result['success']
    # Should complete to 'bead box enable box'
    assert result['completed_line'] == 'bead box enable box', \
        f"Expected 'bead box enable box' but got: {result['completed_line']}"


def test_box_name_completion_index(shell_tester):
    """Test completing box names for 'bead box index' command.

    The test box 'box' should be available for completion.
    """
    result = shell_tester.complete("bead box index bo")
    assert result['success']
    # Should complete to 'bead box index box'
    assert result['completed_line'] == 'bead box index box', \
        f"Expected 'bead box index box' but got: {result['completed_line']}"


def test_input_name_unique_prefix_completion(shell_tester):
    """Test completing input names when prefix uniquely identifies one input.

    Add multiple inputs with similar names and test that a unique prefix
    completes to the correct input.
    """
    # Add multiple inputs with similar names
    shell_tester.send_command("bead input add train-data name")
    shell_tester.send_command("bead input add train-labels name")

    # Test completion with 'train-d' - only 'train-data' matches this prefix
    result = shell_tester.complete("bead input delete train-d")
    assert result['success']
    # Should complete uniquely to 'train-data' since only it starts with 'train-d'
    assert result['completed_line'] == 'bead input delete train-data', \
        f"Expected unique completion to 'train-data' (only match for 'train-d'), got: {result['completed_line']}"


def test_complex_spec_completion(shell_tester, bead_freeze_time):
    """Test completing full box:name@time spec.

    This is the most complex case combining both ':' and '@' separators.
    Bash splits on both (COMP_WORDBREAKS), but our CompletionFinder properly
    preserves @ in the completion context.

    Uses the actual bead's freeze_time from archive metadata, avoiding any
    race condition near midnight when the bead might be created on one date
    but the test could run on the next date.
    """
    # Get the bead's freeze_time in YYYY-MM-DD format
    bead_date = bead_freeze_time.strftime('%Y-%m-%d')
    bead_month = bead_freeze_time.strftime('%Y-%m')
    bead_day = bead_freeze_time.strftime('%d')

    # Construct partial input matching the beginning of the bead's freeze_time
    # e.g., for 2025-10-19, use "2025-10-1"
    partial_date = f"{bead_month}-{bead_day[0]}"

    # Try to complete partial timestamp with year-month-day prefix
    result = shell_tester.complete(f"bead input add test box:name@{partial_date}")
    assert result['success']
    # Should complete to the bead's actual freeze_time date
    expected = f'bead input add test box:name@{bead_date}'
    assert result['completed_line'] == expected, \
        f"Expected '{expected}' but got: {result['completed_line']}"


def test_cursor_in_middle_of_word(shell_tester):
    """Test tab completion when cursor is in the middle of a word.

    This is where bash's wordbreak complexity shows up.
    User types: bead input add test box:name@2024
    Cursor is in the middle, user presses TAB.

    Requires bash 4.0+ for reliable mid-word completion support.
    Bash 3.2 (macOS system bash) lacks skip-completed-text feature.
    """
    # Skip on bash < 4.0 (e.g., macOS system bash 3.2)
    if (shell_tester.shell_name == 'bash' and
        shell_tester.bash_version is not None and
        shell_tester.bash_version < 4):
        pytest.skip(
            f"Bash {shell_tester.bash_version}.x doesn't support skip-completed-text. "
            "Mid-word completion unreliable. Install bash 4.0+ via homebrew for full support."
        )

    # Complete with cursor in middle
    full_input = "bead input add test box:name@2024"
    result = shell_tester.complete(full_input, cursor_offset=5)
    assert result['success']
    # Cursor at position after "name@20", completion should complete the year
    assert result['completed_line'] == 'bead input add test box:name@2024'
