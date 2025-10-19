"""
Unit tests for bead specification parser.
"""

import pytest
from .bead_spec import BeadSpec, parse_relative_offset, is_relative_offset


class TestBeadSpecParsing:
    """Test parsing of bead specifications."""

    def test_name_only(self):
        """Parse plain bead name."""
        spec = BeadSpec.parse("hotel-dataset")
        assert spec == BeadSpec(
            name="hotel-dataset",
            box="",
            time="",
            is_file_path=False,
            file_path=""
        )

    def test_box_and_name(self):
        """Parse box:name format."""
        spec = BeadSpec.parse("home:hotel-dataset")
        assert spec == BeadSpec(
            name="hotel-dataset",
            box="home",
            time="",
            is_file_path=False,
            file_path=""
        )

    def test_name_and_time(self):
        """Parse name@time format."""
        spec = BeadSpec.parse("hotel-dataset@2024-06-15")
        assert spec == BeadSpec(
            name="hotel-dataset",
            box="",
            time="2024-06-15",
            is_file_path=False,
            file_path=""
        )

    def test_box_name_time(self):
        """Parse box:name@time format."""
        spec = BeadSpec.parse("home:hotel-dataset@2024-06-15")
        assert spec == BeadSpec(
            name="hotel-dataset",
            box="home",
            time="2024-06-15",
            is_file_path=False,
            file_path=""
        )

    def test_box_only(self):
        """Parse box: format (context name)."""
        spec = BeadSpec.parse("home:")
        assert spec == BeadSpec(
            name="",
            box="home",
            time="",
            is_file_path=False,
            file_path=""
        )

    def test_time_only(self):
        """Parse @time format (context name, all boxes)."""
        spec = BeadSpec.parse("@2024")
        assert spec == BeadSpec(
            name="",
            box="",
            time="2024",
            is_file_path=False,
            file_path=""
        )

    def test_box_and_time(self):
        """Parse box:@time format (context name, specific box)."""
        spec = BeadSpec.parse("home:@2024-06-15")
        assert spec == BeadSpec(
            name="",
            box="home",
            time="2024-06-15",
            is_file_path=False,
            file_path=""
        )

    def test_relative_time_shorthand(self):
        """Parse relative time expressions (shorthand)."""
        # Previous
        assert BeadSpec.parse("hotel-dataset@-").time == "-"
        assert BeadSpec.parse("hotel-dataset@--").time == "--"
        assert BeadSpec.parse("hotel-dataset@---").time == "---"

        # Next
        assert BeadSpec.parse("hotel-dataset@+").time == "+"
        assert BeadSpec.parse("hotel-dataset@++").time == "++"

        # Git alias
        assert BeadSpec.parse("hotel-dataset@^").time == "^"
        assert BeadSpec.parse("hotel-dataset@^^").time == "^^"

    def test_relative_time_explicit(self):
        """Parse relative time expressions (explicit numbers)."""
        assert BeadSpec.parse("hotel-dataset@-5").time == "-5"
        assert BeadSpec.parse("hotel-dataset@+10").time == "+10"
        assert BeadSpec.parse("hotel-dataset@^7").time == "^7"

    def test_latest_keyword(self):
        """Parse 'latest' keyword."""
        spec = BeadSpec.parse("hotel-dataset@latest")
        assert spec.time == "latest"

    def test_partial_timestamps(self):
        """Parse partial ISO timestamps."""
        # Year only
        assert BeadSpec.parse("dataset@2024").time == "2024"

        # Year-month
        assert BeadSpec.parse("dataset@2024-06").time == "2024-06"

        # Full date
        assert BeadSpec.parse("dataset@2024-06-15").time == "2024-06-15"

        # Date + time
        assert BeadSpec.parse("dataset@2024-06-15T14:30").time == "2024-06-15T14:30"

        # Full timestamp with timezone
        assert BeadSpec.parse("dataset@2024-06-15T14:30:45+0200").time == "2024-06-15T14:30:45+0200"


class TestFilePathDetection:
    """Test file path detection."""

    def test_absolute_unix_path(self):
        """Detect absolute Unix path."""
        spec = BeadSpec.parse("/path/to/bead.zip")
        assert spec.is_file_path
        assert spec.file_path == "/path/to/bead.zip"
        assert spec.name == ""
        assert spec.box == ""
        assert spec.time == ""

    def test_relative_unix_path(self):
        """Detect relative Unix path."""
        spec = BeadSpec.parse("./local/bead.zip")
        assert spec.is_file_path
        assert spec.file_path == "./local/bead.zip"

    def test_parent_relative_path(self):
        """Detect parent-relative path."""
        spec = BeadSpec.parse("../beads/hotel-dataset.zip")
        assert spec.is_file_path
        assert spec.file_path == "../beads/hotel-dataset.zip"

    def test_windows_path(self):
        """Detect Windows path."""
        spec = BeadSpec.parse("C:\\Users\\data\\bead.zip")
        assert spec.is_file_path
        assert spec.file_path == "C:\\Users\\data\\bead.zip"

    def test_zip_extension(self):
        """Detect .zip extension without path separators."""
        spec = BeadSpec.parse("archive.zip")
        assert spec.is_file_path
        assert spec.file_path == "archive.zip"

    def test_not_a_path(self):
        """Names without path separators are not file paths."""
        spec = BeadSpec.parse("hotel-dataset")
        assert not spec.is_file_path
        assert spec.file_path == ""


class TestRelativeOffsetParsing:
    """Test relative offset parsing."""

    def test_minus_shorthand(self):
        """Parse minus shorthand."""
        assert parse_relative_offset("-") == -1
        assert parse_relative_offset("--") == -2
        assert parse_relative_offset("---") == -3
        assert parse_relative_offset("----") == -4

    def test_plus_shorthand(self):
        """Parse plus shorthand."""
        assert parse_relative_offset("+") == 1
        assert parse_relative_offset("++") == 2
        assert parse_relative_offset("+++") == 3
        assert parse_relative_offset("++++") == 4

    def test_caret_shorthand(self):
        """Parse caret (git alias) shorthand."""
        assert parse_relative_offset("^") == -1
        assert parse_relative_offset("^^") == -2
        assert parse_relative_offset("^^^") == -3
        assert parse_relative_offset("^^^^") == -4

    def test_minus_explicit(self):
        """Parse minus with explicit numbers."""
        assert parse_relative_offset("-5") == -5
        assert parse_relative_offset("-10") == -10
        assert parse_relative_offset("-100") == -100

    def test_plus_explicit(self):
        """Parse plus with explicit numbers."""
        assert parse_relative_offset("+5") == 5
        assert parse_relative_offset("+10") == 10
        assert parse_relative_offset("+100") == 100

    def test_caret_explicit(self):
        """Parse caret with explicit numbers."""
        assert parse_relative_offset("^5") == -5
        assert parse_relative_offset("^10") == -10

    def test_empty_expression(self):
        """Empty expression raises ValueError."""
        with pytest.raises(ValueError, match="Empty relative offset"):
            parse_relative_offset("")

    def test_invalid_direction(self):
        """Invalid direction raises ValueError."""
        with pytest.raises(ValueError, match="must start with"):
            parse_relative_offset("x")

        with pytest.raises(ValueError, match="must start with"):
            parse_relative_offset("123")

    def test_invalid_number(self):
        """Invalid number raises ValueError."""
        with pytest.raises(ValueError, match="Invalid relative offset"):
            parse_relative_offset("-abc")

        with pytest.raises(ValueError, match="Invalid relative offset"):
            parse_relative_offset("+x5")


class TestRelativeOffsetDetection:
    """Test relative offset detection."""

    def test_is_relative(self):
        """Detect relative offset expressions."""
        assert is_relative_offset("-")
        assert is_relative_offset("--")
        assert is_relative_offset("-5")
        assert is_relative_offset("+")
        assert is_relative_offset("++")
        assert is_relative_offset("+10")
        assert is_relative_offset("^")
        assert is_relative_offset("^^")
        assert is_relative_offset("^3")

    def test_is_not_relative(self):
        """Non-relative expressions return False."""
        assert not is_relative_offset("")
        assert not is_relative_offset("2024")
        assert not is_relative_offset("2024-06-15")
        assert not is_relative_offset("latest")
        assert not is_relative_offset("abc")


class TestEdgeCases:
    """Test edge cases and special characters."""

    def test_empty_string(self):
        """Empty string parses as empty name."""
        spec = BeadSpec.parse("")
        assert spec == BeadSpec(
            name="",
            box="",
            time="",
            is_file_path=False,
            file_path=""
        )

    def test_colons_in_name(self):
        """Only first colon is separator."""
        spec = BeadSpec.parse("box:name:with:colons")
        assert spec.box == "box"
        assert spec.name == "name:with:colons"

    def test_at_signs_in_name(self):
        """Only first @ is separator (after :)."""
        spec = BeadSpec.parse("name@time@more")
        assert spec.name == "name"
        assert spec.time == "time@more"

    def test_complex_combination(self):
        """Complex case with multiple special chars."""
        spec = BeadSpec.parse("archive:hotel-bookings@2024-06-15T14:30:45+0200")
        assert spec.box == "archive"
        assert spec.name == "hotel-bookings"
        assert spec.time == "2024-06-15T14:30:45+0200"
