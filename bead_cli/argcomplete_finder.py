"""Custom argcomplete CompletionFinder that preserves @ character in completions.

The default argcomplete quote_completions method strips the @ character when
it's in COMP_WORDBREAKS. This custom finder fixes that by preserving @ in
completions while keeping the wordbreak trimming logic for other characters.
"""

import os
from argcomplete import CompletionFinder


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
        # Check if the wordbreak character is @ by looking at COMP_LINE
        if last_wordbreak_pos is not None:
            comp_line = os.environ.get('COMP_LINE', '')
            if last_wordbreak_pos < len(comp_line) and comp_line[last_wordbreak_pos] == '@':
                # For @ wordbreaks, trim at the position to keep the @
                # instead of trimming at position + 1 which removes it
                completions_trimmed = []
                for c in completions:
                    # Trim to keep @ by taking from position onwards
                    trimmed = c[last_wordbreak_pos:]
                    completions_trimmed.append(trimmed)
                completions = completions_trimmed
                # Clear last_wordbreak_pos so parent doesn't trim again
                last_wordbreak_pos = None

        # Call parent's quote_completions
        return super().quote_completions(completions, cword_prequote, last_wordbreak_pos)
