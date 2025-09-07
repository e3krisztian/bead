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
