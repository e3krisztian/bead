from datetime import datetime
from datetime import timedelta

from freezegun import freeze_time
import pytest

from .timestamp import FixedOffset
from .timestamp import Local
from .timestamp import parse_iso8601
from .timestamp import parse_timedelta
from .timestamp import time_from_timestamp
from .timestamp import time_from_user
from .timestamp import timestamp
from .timestamp import TimePrecision
from .timestamp import detect_time_precision
from .timestamp import add_one_unit


@pytest.mark.parametrize(
    "text, value",
    [
        ('1w', timedelta(weeks=1)),
        ('1d', timedelta(days=1)),
        ('1H', timedelta(hours=1)),
        ('1M', timedelta(minutes=1)),
        ('1S', timedelta(seconds=1)),
        ('1d1d', timedelta(days=2)),
        ('3w2d5S', timedelta(weeks=3, days=2, seconds=5)),
        ('1w3w2d5S', timedelta(weeks=4, days=2, seconds=5)),
    ]
)
def test_parse_timedelta(text, value):
    assert parse_timedelta(text) == value


@pytest.mark.parametrize("text", ['1y', '1m', '1x', '1', '3w2d5'])
def test_parse_invalid_timedelta(text):
    with pytest.raises(ValueError):
        parse_timedelta(text)


UTC = FixedOffset(0, 'UTC')


@pytest.mark.parametrize(
    "text, value",
    [
        ('2017', datetime(2017, 1, 1, tzinfo=UTC)),
        ('201511', datetime(2015, 11, 1, tzinfo=UTC)),
        ('2012-11', datetime(2012, 11, 1, tzinfo=UTC)),
        ('20121129', datetime(2012, 11, 29, tzinfo=UTC)),
        ('2012-11-29', datetime(2012, 11, 29, tzinfo=UTC)),
        (
            '2012-11-30T23:59:47-0100',
            datetime(2012, 11, 30, 23, 59, 47, tzinfo=FixedOffset(-60, 'UTC-1'))),
        (
            '20121130T235947-0100',
            datetime(2012, 11, 30, 23, 59, 47, tzinfo=FixedOffset(-60, 'UTC-1'))),
        ('20121120T090000-0100', parse_iso8601('20121120T120000+0200'))
    ]
)
def test_parse_iso8601(text, value):
    assert parse_iso8601(text) == value


@pytest.mark.parametrize("text", ['1y', '12345', '2012-11-30T23', '20121130T23:59:47-0100'])
def test_parse_invalid_iso8601(text):
    with pytest.raises(ValueError):
        parse_iso8601(text)


def test_time_from_timestamp():
    assert (
        time_from_timestamp('20000102T030405000006+0123')
        == datetime(2000, 1, 2, 3, 4, 5, 6, FixedOffset(60 + 23, 'epoch')))
    with pytest.raises(ValueError):
        time_from_timestamp('20000101T000000000000')


def test_time_from_user():
    assert time_from_user('1234') == datetime(1234, 1, 1, tzinfo=UTC)
    assert time_from_user('21340228') == datetime(2134, 2, 28, tzinfo=UTC)
    with freeze_time('2019-11-01', tz_offset=0):
        assert (
            abs(time_from_user('-3w') - datetime(2019, 10, 11, tzinfo=Local))
            < timedelta(days=1))
    with pytest.raises(ValueError):
        time_from_user('21340228x')


def test_timestamp():
    with freeze_time('2000-01-01T00:00:00.000000+0000'):
        assert (
            time_from_timestamp(timestamp())
            == time_from_timestamp('20000101T000000000000+0000'))
    with freeze_time('2019-11-01T01:02:03.000004+0500'):
        assert (
            time_from_timestamp(timestamp())
            == time_from_timestamp('20191101T010203000004+0500'))


@pytest.mark.parametrize(
    "timeish_str, expected_precision",
    [
        # ISO8601 date-only formats
        ('2025', TimePrecision.YEAR),
        ('2025-09', TimePrecision.MONTH),
        ('2025-09-19', TimePrecision.DAY),
        # Compact date-only formats
        ('201509', TimePrecision.MONTH),
        ('20250919', TimePrecision.DAY),
        # ISO8601 date+time formats with colons
        ('2025-09-19T14', TimePrecision.HOUR),
        ('2025-09-19T14:30', TimePrecision.MINUTE),
        ('2025-09-19T14:30:45', TimePrecision.SECOND),
        ('2025-09-19T14:30:45.123456', TimePrecision.MICROSECOND),
        # ISO8601 with timezone
        ('2025-09-19T14:30:45+0200', TimePrecision.SECOND),
        ('2025-09-19T14:30:45.123456+0200', TimePrecision.MICROSECOND),
        # Compact bead timestamp format (no colons/dashes)
        ('20250919T14', TimePrecision.HOUR),
        ('20250919T1430', TimePrecision.MINUTE),
        ('20250919T143045', TimePrecision.SECOND),
        ('20250919T143045123456', TimePrecision.MICROSECOND),
        # Compact with timezone
        ('20250919T143045+0200', TimePrecision.SECOND),
        ('20250919T143045123456+0200', TimePrecision.MICROSECOND),
    ]
)
def test_detect_time_precision(timeish_str, expected_precision):
    assert detect_time_precision(timeish_str) == expected_precision


@pytest.mark.parametrize(
    "timeish_str",
    [
        # Invalid formats
        '2025-09-19T',               # Ends with T separator
        '2025-09-19 14:30:45',       # Space instead of T
        '2025/09/19',                # Slashes instead of dashes
        '25-09-19',                  # 2-digit year
        '2025-09-19T14:30:45:12',    # Extra colon
        '2025-13-01',                # Invalid month
        '2025-09-31',                # Invalid day for September
        '20250919T25',               # Invalid hour
        '20250919T145',              # Odd number of digits in time
        'not-a-date',                # Non-numeric
        '',                          # Empty string
        '2025-09-19T14:30:45.12345', # Incomplete microseconds (5 instead of 6)
    ]
)
def test_detect_time_precision_invalid(timeish_str):
    with pytest.raises(ValueError):
        detect_time_precision(timeish_str)


@pytest.mark.parametrize(
    "dt, precision, expected",
    [
        # Year precision
        (datetime(2025, 1, 1, 0, 0, 0, 0, UTC), TimePrecision.YEAR, datetime(2026, 1, 1, 0, 0, 0, 0, UTC)),
        # Leap year handling: Feb 29 → Feb 28 (non-leap year)
        (datetime(2020, 2, 29, 0, 0, 0, 0, UTC), TimePrecision.YEAR, datetime(2021, 2, 28, 0, 0, 0, 0, UTC)),
        # Month precision
        (datetime(2025, 9, 15, 0, 0, 0, 0, UTC), TimePrecision.MONTH, datetime(2025, 10, 15, 0, 0, 0, 0, UTC)),
        # Month boundary: December → January of next year
        (datetime(2025, 12, 31, 0, 0, 0, 0, UTC), TimePrecision.MONTH, datetime(2026, 1, 31, 0, 0, 0, 0, UTC)),
        # Month edge case: Jan 31 → Feb 28
        (datetime(2025, 1, 31, 0, 0, 0, 0, UTC), TimePrecision.MONTH, datetime(2025, 2, 28, 0, 0, 0, 0, UTC)),
        # Day precision
        (datetime(2025, 9, 19, 0, 0, 0, 0, UTC), TimePrecision.DAY, datetime(2025, 9, 20, 0, 0, 0, 0, UTC)),
        # Hour precision
        (datetime(2025, 9, 19, 14, 0, 0, 0, UTC), TimePrecision.HOUR, datetime(2025, 9, 19, 15, 0, 0, 0, UTC)),
        # Minute precision
        (datetime(2025, 9, 19, 14, 30, 0, 0, UTC), TimePrecision.MINUTE, datetime(2025, 9, 19, 14, 31, 0, 0, UTC)),
        # Second precision
        (datetime(2025, 9, 19, 14, 30, 45, 0, UTC), TimePrecision.SECOND, datetime(2025, 9, 19, 14, 30, 46, 0, UTC)),
        # Microsecond precision
        (datetime(2025, 9, 19, 14, 30, 45, 123456, UTC), TimePrecision.MICROSECOND, datetime(2025, 9, 19, 14, 30, 45, 123457, UTC)),
    ]
)
def test_add_one_unit(dt, precision, expected):
    result = add_one_unit(dt, precision)
    assert result == expected
