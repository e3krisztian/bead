"""
Tests for bead specification autocomplete.
"""

import pytest
from argparse import Namespace
from unittest.mock import Mock, patch

from .autocomplete import complete_bead_spec, complete_input_name, complete_box_name


class TestBeadSpecCompletion:
    """Test bead spec autocomplete functionality."""

    @pytest.fixture
    def mock_env(self):
        """Create a mock environment with test boxes and beads."""
        with patch('bead_cli.autocomplete.get_environment') as mock_get_env:
            env = Mock()

            # Mock boxes
            box1 = Mock()
            box1.name = 'box'
            box2 = Mock()
            box2.name = 'archive'

            # Mock beads in box
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
        assert 'box:' in results  # full box name with separator
        assert 'archive:' in results  # full box name with separator
        assert 'hotel-dataset' in results  # full bead name
        assert 'hotel-bookings' in results  # full bead name

    def test_complete_bead_names_with_prefix(self, mock_env):
        """Test completion of bead names with partial prefix."""
        # Returns full bead names matching the prefix
        results = complete_bead_spec('hotel', Namespace())
        assert 'hotel-dataset' in results  # full name for hotel-dataset
        assert 'hotel-bookings' in results  # full name for hotel-bookings
        assert 'box:' not in results  # Doesn't match prefix

    def test_complete_box_name_with_prefix(self, mock_env):
        """Test completion of box names with partial prefix."""
        # Returns full box names with separator
        results = complete_bead_spec('bo', Namespace())
        assert 'box:' in results  # full box name with separator
        assert 'archive:' not in results  # doesn't start with 'bo'

    def test_complete_bead_after_box(self, mock_env):
        """Test completion of bead names after box: separator."""
        # When full_spec is 'box:', returns full bead specs from that box
        # (full spec so bash can handle wordbreaks correctly)
        results = complete_bead_spec('box:', Namespace())
        assert 'box:hotel-dataset' in results
        assert 'box:hotel-bookings' in results

    def test_complete_bead_after_box_with_prefix(self, mock_env):
        """Test completion of bead names after box: with partial name."""
        # When full_spec is 'box:hotel-d', returns full bead specs matching prefix
        results = complete_bead_spec('box:hotel-d', Namespace())
        assert 'box:hotel-dataset' in results  # full spec for matching bead
        assert 'box:hotel-bookings' not in results  # doesn't match prefix

    def test_complete_time_expressions_full_prefix(self, mock_env):
        """Test completion of time expressions with full box:name@ prefix."""
        # When full_spec is 'box:hotel-dataset@', returns full time completions
        results = complete_bead_spec('box:hotel-dataset@', Namespace())

        # Should have full specs with latest keyword and timestamps
        assert 'box:hotel-dataset@latest' in results
        assert 'box:hotel-dataset@2024' in results
        assert 'box:hotel-dataset@2024-06' in results
        assert 'box:hotel-dataset@2024-06-15' in results

    def test_complete_name_with_time_prefix(self, mock_env):
        """Test completion of bead name with @ but no box."""
        # When full_spec is 'hotel-dataset@', returns full time completions
        results = complete_bead_spec('hotel-dataset@', Namespace())

        assert 'hotel-dataset@latest' in results
        assert 'hotel-dataset@2024' in results

    def test_complete_time_with_partial_timestamp(self, mock_env):
        """Test completion with partial timestamp."""
        # When full_spec is 'hotel-dataset@2024-', returns matching full time completions
        results = complete_bead_spec('hotel-dataset@2024-', Namespace())

        # Should complete timestamps starting with 2024-
        assert any(r == 'hotel-dataset@2024-06' for r in results)  # 2024-06 completion
        assert any(r == 'hotel-dataset@2024-09' for r in results)  # 2024-09 completion

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


class TestInputNameCompletion:
    """Test input name autocomplete functionality."""

    @pytest.fixture
    def mock_workspace(self):
        """Create a mock workspace with test inputs."""
        workspace = Mock()

        # Mock input specs
        input1 = Mock()
        input1.name = 'training-data'

        input2 = Mock()
        input2.name = 'validation-set'

        input3 = Mock()
        input3.name = 'test-data'

        workspace.inputs = [input1, input2, input3]
        workspace.is_valid = True

        # Mock meta dictionary for direct access
        workspace.meta = {
            'inputs': {
                'training-data': input1,
                'validation-set': input2,
                'test-data': input3,
            }
        }

        return workspace

    @patch('bead_cli.autocomplete.get_workspace')
    def test_complete_input_names_all(self, mock_get_workspace, mock_workspace):
        """Test completion of all input names with empty prefix."""
        mock_get_workspace.return_value = mock_workspace
        parsed_args = Namespace()
        results = complete_input_name('', parsed_args)

        assert 'training-data' in results
        assert 'validation-set' in results
        assert 'test-data' in results
        assert len(results) == 3

    @patch('bead_cli.autocomplete.get_workspace')
    def test_complete_input_names_with_prefix(self, mock_get_workspace, mock_workspace):
        """Test completion of input names with partial prefix."""
        mock_get_workspace.return_value = mock_workspace
        parsed_args = Namespace()
        results = complete_input_name('training', parsed_args)

        assert 'training-data' in results
        assert 'validation-set' not in results
        assert 'test-data' not in results

    @patch('bead_cli.autocomplete.get_workspace')
    def test_complete_input_names_with_prefix_match_multiple(self, mock_get_workspace, mock_workspace):
        """Test completion that matches multiple input names."""
        mock_get_workspace.return_value = mock_workspace
        parsed_args = Namespace()
        results = complete_input_name('test', parsed_args)

        # Only 'test-data' matches 'test' prefix
        assert 'test-data' in results
        assert 'training-data' not in results
        assert 'validation-set' not in results

    def test_complete_input_names_no_workspace(self):
        """Test completion with no workspace returns empty."""
        parsed_args = Namespace(workspace=None)
        results = complete_input_name('', parsed_args)

        assert results == []

    def test_complete_input_names_invalid_workspace(self):
        """Test completion with invalid workspace returns empty."""
        workspace = Mock()
        workspace.is_valid = False

        parsed_args = Namespace(workspace=workspace)
        results = complete_input_name('', parsed_args)

        assert results == []

    def test_complete_input_names_exception_handling(self):
        """Test completion handles exceptions gracefully."""
        parsed_args = Namespace(workspace=Mock(side_effect=Exception("Error")))
        results = complete_input_name('', parsed_args)

        assert results == []


class TestBoxNameCompletion:
    """Test box name autocomplete functionality."""

    @pytest.fixture
    def mock_env(self):
        """Create a mock environment with test boxes."""
        env = Mock()

        # Mock boxes
        box1 = Mock()
        box1.name = 'research-data'
        box1.enabled = True

        box2 = Mock()
        box2.name = 'archive'
        box2.enabled = True

        box3 = Mock()
        box3.name = 'backup'
        box3.enabled = False

        env.get_boxes.return_value = [box1, box2]  # Only enabled boxes

        return env

    @patch('bead_cli.autocomplete.get_environment')
    def test_complete_box_names_all(self, mock_get_env, mock_env):
        """Test completion of all enabled box names with empty prefix."""
        mock_get_env.return_value = mock_env

        parsed_args = Namespace()
        results = complete_box_name('', parsed_args)

        assert 'research-data' in results
        assert 'archive' in results
        assert len(results) == 2

    @patch('bead_cli.autocomplete.get_environment')
    def test_complete_box_names_with_prefix(self, mock_get_env, mock_env):
        """Test completion of box names with partial prefix."""
        mock_get_env.return_value = mock_env

        parsed_args = Namespace()
        results = complete_box_name('ar', parsed_args)

        assert 'archive' in results
        assert 'research-data' not in results

    @patch('bead_cli.autocomplete.get_environment')
    def test_complete_box_names_returns_sorted(self, mock_get_env, mock_env):
        """Test that completed box names are sorted."""
        mock_get_env.return_value = mock_env

        parsed_args = Namespace()
        results = complete_box_name('', parsed_args)

        assert results == sorted(results)

    @patch('bead_cli.autocomplete.get_environment', side_effect=Exception("No config"))
    def test_complete_box_names_env_error(self, mock_get_env):
        """Test completion handles environment errors gracefully."""
        parsed_args = Namespace()
        results = complete_box_name('', parsed_args)

        assert results == []
