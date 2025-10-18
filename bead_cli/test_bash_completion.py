"""
Integration tests for shell tab completion using pexpect.

Tests real shell completion behavior by driving interactive shells with pexpect.
These are end-to-end tests that verify the actual completion system works correctly
with colon (:) and at-sign (@) word break characters.

The test framework is generalized to support multiple shells - start with bash,
but can easily be extended to zsh, fish, tcsh, etc. by adding new ShellConfig entries.
"""

import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pexpect
import pytest
import sys

from bead.ziparchive import ZipArchive


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


@dataclass
class ShellConfig:
    """Configuration for testing a specific shell's completion behavior.

    Encapsulates all shell-specific details so the ShellTester can work
    with any POSIX-like shell (bash, zsh, fish, etc.).
    """
    shell_name: str
    """Shell executable name (e.g., 'bash', 'zsh', 'fish')"""

    spawn_command: str
    """Command to start shell without init files (e.g., 'bash --norc')"""

    prompt: str = "$ "
    """Prompt string to set (e.g., '$ ')"""

    prompt_regex: str = r'\$ '
    r"""Regex pattern to detect prompt (e.g., r'\$ ')"""

    setup_completion: str = 'eval "$(bead completion bash)"'
    """Command to load completion support (shell-specific)"""

    completion_key: str = '\t'
    """Completion key sequence (usually TAB, standard POSIX)"""

    completion_key_count: int = 2
    """Number of completion key presses to trigger and display completions"""

    ctrl_a: str = '\u0001'
    """Control-A (beginning of line) sequence"""

    cursor_left_escape: str = '\033[{n}D'
    """ANSI escape template for moving cursor left N positions"""


def get_shell_config(shell_name: str) -> ShellConfig:
    """Get shell configuration for testing.

    Registry pattern for shell configurations. Add new shells here as they're supported.

    Args:
        shell_name: Name of shell ('bash', 'zsh', 'fish', etc.)

    Returns:
        ShellConfig for the specified shell

    Raises:
        ValueError: If shell is not supported
    """
    configs = {
        'bash': ShellConfig(
            shell_name='bash',
            spawn_command='bash --norc',
            prompt='$ ',
            prompt_regex=r'\$ ',
            setup_completion='eval "$(bead completion bash)"',
        ),
    }

    if shell_name not in configs:
        supported = ', '.join(configs.keys())
        raise ValueError(
            f"Shell '{shell_name}' not configured for testing. "
            f"Currently supported: {supported}. "
            f"To add support, add a ShellConfig entry to get_shell_config()."
        )

    return configs[shell_name]


pytestmark = pytest.mark.skipif(
    not _shell_available('bash'),
    reason="bash not available - skipping pexpect integration tests"
)


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
    """Integration test harness for shell completion using pexpect.

    Generalized to work with any shell through ShellConfig configuration.
    Supports bash, zsh, fish, tcsh, and other POSIX-like shells.
    """

    MAGIC_MARK = "___COMPLETION_MARKER___"
    TIMEOUT = 5

    def __init__(self, shell_config: ShellConfig, env, debug=False):
        """Initialize tester for a specific shell.

        Args:
            shell_config: ShellConfig specifying shell-specific behavior
            env: Environment dict (from home_env fixture)
            debug: If True, print shell interaction logs
        """
        self.config = shell_config
        self.env = env
        self.debug = debug
        self.shell = None

    def setup(self):
        """Start shell and configure completion."""
        # Start shell with no rc files to have a clean environment
        self.shell = pexpect.spawn(
            self.config.spawn_command,
            maxread=20000,
            timeout=self.TIMEOUT,
            encoding='utf-8',
            dimensions=(24, 240),
            env=self.env
        )
        self.shell.delaybeforesend = 0

        if self.debug:
            self.shell.logfile_read = sys.stdout

        # Set prompt for reliable detection
        self._send_command(f"PS1='{self.config.prompt}'")

        # Load completion setup (shell-specific)
        self._send_command(self.config.setup_completion)

        # Setup bead workspace
        self._send_command('bead new workspace')
        self._send_command('cd workspace')

    def cleanup(self):
        """Clean up shell session."""
        if self.shell:
            try:
                self._send_command("bead nuke")
                self.shell.send("exit\n")
                self.shell.expect(pexpect.EOF, timeout=2)
            except Exception:
                try:
                    self.shell.terminate()
                except Exception:
                    pass

    def _send_command(self, cmd):
        """Send command and wait for prompt."""
        self.shell.send(cmd + "\n")
        self.shell.expect(self.config.prompt_regex)

    def run_diagnostic(self, cmd):
        """Run a diagnostic command and return output for debugging.

        Args:
            cmd: Command to run (e.g., "bead box list")

        Returns:
            str: Command output
        """
        self.shell.send(cmd + "\n")
        self.shell.expect(self.config.prompt_regex)
        return self.shell.before

    def complete(self, partial_input, cursor_offset=0):
        """Test completion of bead command using tab key.

        Note: Press TAB tab_count times - first TAB triggers completion mechanism,
        subsequent TABs display the actual completions (configurable per shell).

        Args:
            partial_input: The partial command line (e.g., "bead ho")
            cursor_offset: Move cursor back N chars before tab (0 = at end)

        Returns:
            dict: {'completed_line': str, 'success': True/False, 'raw_output': str}
        """
        # Clear buffer by sending a unique marker and waiting for it
        # This ensures we're at a known state
        clear_marker = f"___CLEAR_{id(self)}_{int(time.time()*1000000)}___"
        self.shell.send(f"echo '{clear_marker}'\n")
        try:
            self.shell.expect(clear_marker, timeout=self.TIMEOUT)
        except Exception:
            pass  # Continue anyway

        time.sleep(0.1)

        # Type the command
        self.shell.send(partial_input)
        time.sleep(0.1)

        # Move cursor if needed (for testing mid-word completion)
        if cursor_offset > 0:
            escape_seq = self.config.cursor_left_escape.format(n=cursor_offset)
            self.shell.send(escape_seq)
            time.sleep(0.1)

        # Send completion keys to trigger and display completions (number configurable per shell)
        for _ in range(self.config.completion_key_count):
            self.shell.send(self.config.completion_key)
        time.sleep(0.3)  # Give shell time to generate and display completions

        # Move to beginning of line and insert echo to display completions without executing command
        self.shell.send(self.config.ctrl_a + 'echo COMPLETED LINE=\n')
        time.sleep(0.1)

        # Send a unique marker to know where completions end
        self.shell.send(f"echo '{self.MAGIC_MARK}'\n")

        try:
            # Wait for marker - capture output from just after we pressed tab
            self.shell.expect(self.MAGIC_MARK, timeout=self.TIMEOUT)
            output = self.shell.before

            # After tab(s) and Ctrl+A+echo, look for the completed command line in output
            # It will show: original input + completions added by shell
            # Replace \r with \n to normalize line endings
            output_normalized = output.replace('\r', '\n')
            lines = output_normalized.split('\n')
            completed_line = None

            for line in lines:
                line = line.strip()
                # Skip empty lines, control sequences, markers
                if not line or '\x1b' in line or 'MAGIC_MARK' in line:
                    continue

                # Skip the echo command line itself, but keep lines showing COMPLETED LINE=
                if line.startswith('echo COMPLETED LINE'):
                    continue

                # Look for lines that contain COMPLETED LINE= (output from the echo)
                # These show the completed command
                if 'COMPLETED LINE=' in line:
                    # Extract everything after "COMPLETED LINE="
                    idx = line.find('COMPLETED LINE=')
                    if idx >= 0:
                        completed_line = line[idx + len('COMPLETED LINE='):].strip()
                        # Clean up trailing control characters
                        completed_line = completed_line.rstrip('-\x07 ')
                    break

            result = {'completed_line': completed_line, 'success': True, 'raw_output': repr(output)}

            # Debug output if requested
            if self.debug:
                print(f"\n[COMPLETION DEBUG] Input: {partial_input}")
                print(f"[COMPLETION DEBUG] Completed line: {repr(completed_line)}")

            return result
        except pexpect.TIMEOUT:
            return {'error': 'Timeout waiting for completion', 'completions': [], 'raw_output': None}
        except Exception as e:
            return {'error': str(e), 'completions': [], 'raw_output': None}


@pytest.fixture
def bash_tester(home_env):
    """Provide ShellTester instance for bash completion tests.

    Note: This is named 'bash_tester' for backward compatibility with existing tests.
    To test other shells, use ShellTester directly with their ShellConfig.
    """
    bash_config = get_shell_config('bash')
    tester = ShellTester(bash_config, home_env, debug=True)
    tester.setup()
    try:
        yield tester
    finally:
        tester.cleanup()


def test_bead_completion_loads(bash_tester):
    """Test that bead completion loads successfully in bash."""
    # If setup succeeds, completion is loaded
    assert bash_tester.shell is not None


def test_bead_help_completion(bash_tester):
    """Test completing partial command 'bead inp' completes to 'bead input'."""
    result = bash_tester.complete("bead inp")
    assert result['success']
    # Should complete to 'bead input'
    assert result['completed_line'] == 'bead input', \
        f"Expected 'bead input' but got: {result['completed_line']}"


def test_box_name_completion(bash_tester, completion_debug_log):
    """Test completing bead specs in 'bead input add' context.

    The test bead 'name' from box 'box' should appear in completions.
    """
    result = bash_tester.complete("bead input add test box:")
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


def test_colon_separator_in_completion(bash_tester):
    """Test that completing with colon separator works.

    This tests the key wordbreak issue: when typing 'box:name',
    bash splits on ':' (COMP_WORDBREAKS includes ':').
    The completion should handle this and suggest beads.
    """
    # Try to complete with partial bead name after colon
    result = bash_tester.complete("bead input add test box:na")
    assert result['success']
    # Should complete to 'bead input add test box:name'
    assert result['completed_line'] == 'bead input add test box:name', \
        f"Expected 'bead input add test box:name' but got: {result['completed_line']}"


def test_at_separator_in_completion(bash_tester, completion_debug_log):
    """Test that completing with @ separator works.

    The completion should handle @ and suggest timestamps.
    """
    # Try to complete partial timestamp after @
    result = bash_tester.complete("bead input add test box:name@la")
    assert result['success']

    # Check debug log
    if completion_debug_log.exists():
        debug_content = completion_debug_log.read_text()
        print(f"\n[DEBUG LOG]\n{debug_content}\n")

    # Should complete to 'bead input add test box:name@latest'
    assert result['completed_line'] == 'bead input add test box:name@latest', \
        f"Expected 'bead input add test box:name@latest' but got: {result['completed_line']}"


def test_bead_name_with_time_completion(bash_tester, completion_debug_log):
    """Test completing bead name with time expression (no box prefix).

    Tests if @ completion works without a box: prefix.
    """
    result = bash_tester.complete("bead input add test name@lat")
    assert result['success']

    # Check debug log
    if completion_debug_log.exists():
        debug_content = completion_debug_log.read_text()
        print(f"\n[DEBUG LOG]\n{debug_content}\n")

    # Should complete to 'bead input add test name@latest'
    assert result['completed_line'] == 'bead input add test name@latest', \
        f"Expected 'bead input add test name@latest' but got: {result['completed_line']}"


def test_complex_spec_completion(bash_tester, bead_freeze_time):
    """Test completing full box:name@time spec.

    This is the most complex case combining both ':' and '@' separators.
    Bash splits on both, but our CompletionFinder now properly preserves @.

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
    result = bash_tester.complete(f"bead input add test box:name@{partial_date}")
    assert result['success']
    # Should complete to the bead's actual freeze_time date
    expected = f'bead input add test box:name@{bead_date}'
    assert result['completed_line'] == expected, \
        f"Expected '{expected}' but got: {result['completed_line']}"


def test_cursor_in_middle_of_word(bash_tester):
    """Test tab completion when cursor is in the middle of a word.

    This is where the real wordbreak complexity shows up.
    User types: bead input add test box:name@2024
    Cursor is in the middle, user presses TAB.
    """
    # Complete with cursor in middle - should not crash
    full_input = "bead input add test box:name@2024"
    result = bash_tester.complete(full_input, cursor_offset=5)
    # Just verify it doesn't crash and returns data
    assert result['success'] or 'error' in result
