"""Custom argcomplete CompletionFinder that preserves @ character in completions.

The default argcomplete quote_completions method strips the @ character when
it's in COMP_WORDBREAKS. This custom finder fixes that by preserving @ in
completions while keeping the wordbreak trimming logic for other characters.
"""

import os
from argcomplete import CompletionFinder


def _debug(msg):
    """Write debug message to log file if BEAD_COMPLETION_DEBUG is set."""
    debug_log = os.environ.get('BEAD_COMPLETION_DEBUG')
    if debug_log:
        with open(debug_log, 'a') as f:
            f.write(msg + '\n')


class BeadCompletionFinder(CompletionFinder):
    """Custom completion finder that preserves @ character in completions."""

    def quote_completions(self, completions, cword_prequote, last_wordbreak_pos):
        """Override quote_completions to preserve @ in completions.

        The default implementation trims at last_wordbreak_pos + 1, which for @
        removes the @ character. We detect @ and preserve it by trimming at
        the exact position instead.

        Args:
            completions: List of completion strings
            cword_prequote: Quote character if word started with quote
            last_wordbreak_pos: Position of last wordbreak character

        Returns:
            List of quoted/trimmed completions
        """
        _debug(f"[quote_completions] completions={completions}, last_wordbreak_pos={last_wordbreak_pos}")

        comp_line = os.environ.get('COMP_LINE', '')
        comp_point = os.environ.get('COMP_POINT', '')
        _debug(f"[quote_completions] comp_line={repr(comp_line)}, COMP_POINT={comp_point}")

        # Search for @ character in completions (don't strip it)
        # The @ character may be a wordbreak, but we want to keep it in the completion
        has_at = any('@' in c for c in completions)
        if has_at:
            # Find the position of @ in the first completion string
            at_pos_in_completion = -1
            if completions and '@' in completions[0]:
                at_pos_in_completion = completions[0].index('@')
                _debug(f"[quote_completions] Found @ in completion at position {at_pos_in_completion}")

                # Trim completions at the @ position to preserve it
                completions_trimmed = []
                for c in completions:
                    if '@' in c:
                        at_idx = c.index('@')
                        trimmed = c[at_idx:]
                        completions_trimmed.append(trimmed)
                    else:
                        completions_trimmed.append(c)
                _debug(f"[quote_completions] Trimmed to preserve @: {completions_trimmed}")
                completions = completions_trimmed
                # Clear last_wordbreak_pos so parent doesn't trim again
                last_wordbreak_pos = None

        # Call parent's quote_completions
        result = super().quote_completions(completions, cword_prequote, last_wordbreak_pos)
        _debug(f"[quote_completions] Final result from parent={result}")
        return result
