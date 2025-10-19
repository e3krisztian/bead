"""
Tests for bead specification autocomplete.
"""

import pytest
from argparse import Namespace
from unittest.mock import Mock, patch

from .autocomplete import complete_bead_spec


class TestBeadSpecCompletion:
    """Test bead spec autocomplete functionality."""

    @pytest.fixture
    def mock_env(self):
        """Create a mock environment with test boxes and beads."""
        with patch('bead_cli.autocomplete.get_environment') as mock_get_env:
            env = Mock()

            # Mock boxes
            box1 = Mock()
            box1.name = 'home'
            box2 = Mock()
            box2.name = 'archive'

            # Mock beads in home box
            bead1 = Mock()
            bead1.name = 'hotel-dataset'
            bead1.freeze_time = Mock()
            bead1.freeze_time.strftime = Mock(side_effect=lambda fmt: {
                '%Y': '2024',
                '%Y-%m': '2024-06',
                '%Y-%m-%d': '2024-06-15'
            }.get(fmt, '2024'))

            bead2 = Mock()
            bead2.name = 'hotel-bookings'
            bead2.freeze_time = Mock()
            bead2.freeze_time.strftime = Mock(side_effect=lambda fmt: {
                '%Y': '2024',
                '%Y-%m': '2024-09',
                '%Y-%m-%d': '2024-09-20'
            }.get(fmt, '2024'))

            box1.index.get_beads.return_value = [bead1, bead2]
            box2.index.get_beads.return_value = [bead1]

            env.get_boxes.return_value = [box1, box2]
            env.get_box.return_value = box1

            mock_get_env.return_value = env
            yield env

    def test_complete_box_or_bead_names(self, mock_env):
        """Test completion of box names and bead names without separators."""
        # Empty prefix should return all boxes and beads with full names
        results = complete_bead_spec('', Namespace())
        assert 'home:' in results  # full box name with separator
        assert 'archive:' in results  # full box name with separator
        assert 'hotel-dataset' in results  # full bead name
        assert 'hotel-bookings' in results  # full bead name

    def test_complete_bead_names_with_prefix(self, mock_env):
        """Test completion of bead names with partial prefix."""
        # Returns full bead names matching the prefix
        results = complete_bead_spec('hotel', Namespace())
        assert 'hotel-dataset' in results  # full name for hotel-dataset
        assert 'hotel-bookings' in results  # full name for hotel-bookings
        assert 'home:' not in results  # Doesn't match prefix

    def test_complete_box_name_with_prefix(self, mock_env):
        """Test completion of box names with partial prefix."""
        # Returns full box names with separator
        results = complete_bead_spec('ho', Namespace())
        assert 'home:' in results  # full box name with separator
        assert 'archive:' not in results  # doesn't start with 'ho'

    def test_complete_bead_after_box(self, mock_env):
        """Test completion of bead names after box: separator."""
        # When full_spec is 'home:', returns bead names from that box
        results = complete_bead_spec('home:', Namespace())
        assert 'hotel-dataset' in results
        assert 'hotel-bookings' in results

    def test_complete_bead_after_box_with_prefix(self, mock_env):
        """Test completion of bead names after box: with partial name."""
        # When full_spec is 'home:hotel-d', returns full bead names matching prefix
        results = complete_bead_spec('home:hotel-d', Namespace())
        assert 'hotel-dataset' in results  # full bead name
        assert 'hotel-bookings' not in results  # doesn't match prefix

    def test_complete_time_expressions_full_prefix(self, mock_env):
        """Test completion of time expressions with full box:name@ prefix."""
        # When full_spec is 'home:hotel-dataset@', returns time completions
        results = complete_bead_spec('home:hotel-dataset@', Namespace())

        # Should have latest keyword
        assert 'latest' in results

        # Should have timestamps from the bead
        assert '2024' in results
        assert '2024-06' in results
        assert '2024-06-15' in results

    def test_complete_name_with_time_prefix(self, mock_env):
        """Test completion of bead name with @ but no box."""
        # When full_spec is 'hotel-dataset@', returns time completions
        results = complete_bead_spec('hotel-dataset@', Namespace())

        assert 'latest' in results
        assert '2024' in results

    def test_complete_time_with_partial_timestamp(self, mock_env):
        """Test completion with partial timestamp."""
        # When full_spec is 'hotel-dataset@2024-', returns matching time completions
        results = complete_bead_spec('hotel-dataset@2024-', Namespace())

        # Should complete timestamps starting with 2024-
        assert any(r == '2024-06' for r in results)  # 2024-06 completion
        assert any(r == '2024-09' for r in results)  # 2024-09 completion

    def test_file_path_returns_empty(self, mock_env):
        """Test that file paths return empty to let shell handle."""
        results = complete_bead_spec('/path/to/bead.zip', Namespace())
        assert results == []

        results = complete_bead_spec('./bead.zip', Namespace())
        assert results == []

    def test_environment_error_returns_empty(self):
        """Test that environment errors return empty completions."""
        with patch('bead_cli.autocomplete.get_environment', side_effect=Exception("No config")):
            results = complete_bead_spec('hotel', Namespace())
            assert results == []

    def test_parse_error_returns_empty(self, mock_env):
        """Test that parse errors return empty completions."""
        # This shouldn't actually fail to parse, but test the error handling
        results = complete_bead_spec('!!!invalid!!!', Namespace())
        # Should still return empty or handle gracefully
        assert isinstance(results, list)
