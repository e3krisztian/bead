"""Unit tests for BeadCompletionFinder.

These tests characterize the current behavior of the @ preservation hack
for bash shell completion.
"""

import os
import re
import pytest
from unittest.mock import patch

from bead_cli.argcomplete_finder import BeadCompletionFinder


@pytest.fixture
def finder():
    """Create a BeadCompletionFinder instance for testing."""
    return BeadCompletionFinder()


def test_at_preservation_in_bash(finder):
    """Test that @ character is preserved in bash completions.

    This answers: What happens with completions containing @?

    When bash has @ in COMP_WORDBREAKS and completion is 'box:name@latest',
    last_wordbreak_pos points to @. We want to preserve @ in the output.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['box:name@latest', 'box:name@2024']
        cword_prequote = ''
        last_wordbreak_pos = 9  # Position of @ in 'box:name@latest'

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # Should preserve @ by trimming to '@latest', '@2024'
        assert result == ['@latest', '@2024']


def test_single_completion_with_at(finder):
    """Test single completion with @ character.

    This answers: Does it work with a single completion?
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['box:name@latest']
        cword_prequote = ''
        last_wordbreak_pos = 9  # Position of @

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # Should be '@latest' followed by space (single completion gets trailing space)
        assert len(result) == 1
        assert result[0].startswith('@')


def test_mixed_completions_with_and_without_at(finder):
    """Test completions where some have @ and some don't.

    This answers: What happens when completions are mixed?

    Current behavior: If ANY completion has @, we preserve it.
    Completions without @ are left unchanged.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['box:name@latest', 'other:bead']
        cword_prequote = ''
        last_wordbreak_pos = 9

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # First has @, so we trigger preservation
        # Completions without @ are appended unchanged
        result_str = ' '.join(result)
        assert '@latest' in result_str
        assert 'other:bead' in result_str


def test_zsh_bypasses_at_preservation(finder):
    """Test that zsh doesn't need @ preservation.

    This answers: How does zsh differ?

    In zsh, @ is NOT in COMP_WORDBREAKS, so the hack is not needed.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'zsh'}):
        completions = ['box:name@latest']
        cword_prequote = ''
        last_wordbreak_pos = None  # zsh doesn't break on @

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # Should call parent without modification
        # Parent may add escaping (e.g., box\:name@latest for zsh)
        # but the full completion should be preserved (no trimming)
        result_str = ' '.join(result)
        # Match with optional backslash before special chars
        assert re.search(r'box\\?:name\\?@latest', result_str)


def test_fish_bypasses_at_preservation(finder):
    """Test that fish doesn't need @ preservation."""
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'fish'}):
        completions = ['box:name@latest']
        cword_prequote = ''
        last_wordbreak_pos = None

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # Should call parent without modification
        result_str = ' '.join(result)
        # Match with optional backslash before special chars
        assert re.search(r'box\\?:name\\?@latest', result_str)


def test_normal_colon_trimming_without_at(finder):
    """Test normal wordbreak trimming for colon without @.

    This answers: Does normal trimming still work?

    When completion is 'box:name' without @, the parent's trimming
    should work normally.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['box:name', 'box:other']
        cword_prequote = ''
        last_wordbreak_pos = 3  # Position of : in 'box:name'

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # Parent should trim at last_wordbreak_pos + 1
        # So 'box:name' becomes 'name', 'box:other' becomes 'other'
        assert 'name' in result
        assert 'other' in result
        result_str = ' '.join(result)
        assert 'box:' not in result_str


def test_empty_completions(finder):
    """Test with empty completion list.

    This answers: Does it handle edge cases?
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = []
        cword_prequote = ''
        last_wordbreak_pos = None

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        assert result == []


def test_completions_without_at_character(finder):
    """Test completions that don't contain @ at all.

    This answers: Does @ detection work correctly?

    If no completion has @, should not trigger preservation hack.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['box:name', 'other:bead']
        cword_prequote = ''
        last_wordbreak_pos = 3  # Position of :

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # No @, so normal parent trimming happens
        assert 'name' in result


def test_at_not_in_first_position(finder):
    """Test @ appearing later in the completion string.

    This documents current behavior when @ is deep in the string.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['very:long:box:name@latest']
        cword_prequote = ''
        last_wordbreak_pos = 19  # Position of @

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # Should find @ and trim to '@latest'
        assert result[0].strip() == '@latest'


def test_multiple_at_characters(finder):
    """Test completion with multiple @ characters.

    This answers: What happens with multiple @?

    Current code uses index(), which finds the FIRST @.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['box@name@latest']
        cword_prequote = ''
        last_wordbreak_pos = 3  # First @

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # index() finds first @, so should trim to '@name@latest'
        assert '@' in result[0]


def test_quoted_completion(finder):
    """Test completion with quote character.

    This documents behavior when word started with a quote.
    """
    with patch.dict(os.environ, {'_ARGCOMPLETE_SHELL': 'bash'}):
        completions = ['box:name@latest']
        cword_prequote = '"'  # User typed "box:name@la
        last_wordbreak_pos = 9

        result = finder.quote_completions(
            completions, cword_prequote, last_wordbreak_pos)

        # When quoted, parent handles escaping
        # Our @ preservation should still work
        assert len(result) == 1
