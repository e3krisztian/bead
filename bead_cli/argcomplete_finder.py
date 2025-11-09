"""Custom argcomplete CompletionFinder that preserves @ character in completions.

The default argcomplete quote_completions method strips the @ character when
it's in COMP_WORDBREAKS. This custom finder fixes that by preserving @ in
completions while keeping the wordbreak trimming logic for other characters.
"""

import os
from argcomplete.finders import CompletionFinder


def _debug(msg: str) -> None:
    """Write debug message to log file if BEAD_COMPLETION_DEBUG is set."""
    debug_log = os.environ.get('BEAD_COMPLETION_DEBUG')
    if debug_log:
        with open(debug_log, 'a') as f:
            f.write(msg + '\n')


class BeadCompletionFinder(CompletionFinder):
    """Custom completion finder that preserves @ character in completions."""

    def __call__(self, *args, **kwargs):
        """Override __call__ to add debug logging at entry point."""
        _debug(f"[BeadCompletionFinder.__call__] ENTRY - args={args}, kwargs={kwargs}")
        _debug(f"[BeadCompletionFinder.__call__] _ARGCOMPLETE={os.environ.get('_ARGCOMPLETE')}")
        _debug(f"[BeadCompletionFinder.__call__] COMP_LINE={os.environ.get('COMP_LINE')}")
        result = super().__call__(*args, **kwargs)
        _debug(f"[BeadCompletionFinder.__call__] EXIT - result={result}")
        return result

    def quote_completions(self, completions, cword_prequote, last_wordbreak_pos):
        """Override quote_completions to preserve @ in completions (bash-only).

        THE PROBLEM:
        In bash, @ is in COMP_WORDBREAKS. When user types "box:name@la<TAB>",
        bash splits this as: "box", ":", "name", "@", "la"
        Completion returns: "box:name@latest"
        argcomplete's default behavior (finders.py:518-519):
          - Sets last_wordbreak_pos to position of @ (e.g., 9)
          - Trims completion: completion[last_wordbreak_pos + 1:] = "latest"
        Result: Shell shows "latest" instead of "@latest" (@ is lost!)

        THE SOLUTION:
        1. Detect if any completion contains @
        2. Manually trim completions at @ position (NOT +1): "@latest"
        3. Set last_wordbreak_pos = None to prevent parent from trimming again
        4. Parent's quote_completions sees None and skips trimming (finders.py:518)

        SHELLS:
        - bash: @ is a wordbreak → hack needed
        - zsh/fish: @ is NOT a wordbreak → bypass hack

        Args:
            completions: List of completion strings (e.g., ['box:name@latest'])
            cword_prequote: Quote character if word started with quote
            last_wordbreak_pos: Position of last wordbreak character in completion

        Returns:
            List of quoted/trimmed completions
        """
        _debug(f"[quote_completions] completions={completions}, last_wordbreak_pos={last_wordbreak_pos}")

        # Detect which shell we're running under
        target_shell = os.environ.get('_ARGCOMPLETE_SHELL', 'bash')
        _debug(f"[quote_completions] target_shell={target_shell}")

        # Check if @ preservation is needed (bash with @ in completions)
        if not _should_preserve_at_character(completions, target_shell):
            _debug(f"[quote_completions] Skipping @ preservation for {target_shell}")
            return super().quote_completions(completions, cword_prequote, last_wordbreak_pos)

        # Apply @ preservation hack for bash
        _debug("[quote_completions] Applying @ preservation hack")
        completions = _trim_preserving_at(completions)
        _debug(f"[quote_completions] Trimmed completions: {completions}")

        # Clear last_wordbreak_pos to prevent parent from trimming again
        # When parent sees None, it skips the trimming step (finders.py:518 check fails)
        last_wordbreak_pos = None

        return super().quote_completions(completions, cword_prequote, last_wordbreak_pos)


def _should_preserve_at_character(completions: list[str], shell_type: str) -> bool:
    """Check if @ preservation hack is needed.

    Args:
        completions: List of completion strings
        shell_type: Shell type from _ARGCOMPLETE_SHELL env var

    Returns:
        True if we need to apply @ preservation (bash with @ in completions)
    """
    # zsh and fish don't have @ in COMP_WORDBREAKS, so bypass the hack
    if shell_type in ('zsh', 'fish'):
        return False
    # bash needs the hack only if completions contain @
    return any('@' in c for c in completions)


def _trim_preserving_at(completions: list[str]) -> list[str]:
    """Trim completions to start from @ character, preserving it.

    Manually trim each completion to start FROM @ (includes the @ character).
    Example: 'box:name@latest' → '@latest'

    This is different from argcomplete's default trimming which would do +1:
    'box:name@latest'[10:] = 'latest' (@ is lost)

    Args:
        completions: List of completion strings

    Returns:
        List of trimmed completions
    """
    trimmed = []
    for c in completions:
        if '@' in c:
            at_idx = c.index('@')
            trimmed.append(c[at_idx:])  # Keep @ by starting AT index, not index+1
        else:
            # Mixed case: some completions have @, some don't
            trimmed.append(c)
    return trimmed
