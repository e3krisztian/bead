from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from enum import Enum, auto
import re
import time as _time

#########################################################
# source: https://docs.python.org/2/library/datetime.html
# Example tzinfo classes:
# A class capturing the platform's idea of local time.


ZERO = timedelta(0)


class FixedOffset(tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset, name: str):
        self.__offset = timedelta(minutes = offset)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO


STDOFFSET = timedelta(seconds = -_time.timezone)
if _time.daylight:
    DSTOFFSET = timedelta(seconds = -_time.altzone)
else:
    DSTOFFSET = STDOFFSET

DSTDIFF = DSTOFFSET - STDOFFSET


class LocalTimezone(tzinfo):

    def utcoffset(self, dt):
        if self._isdst(dt):
            return DSTOFFSET
        else:
            return STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return DSTDIFF
        else:
            return ZERO

    def tzname(self, dt):
        return _time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0


Local = LocalTimezone()


# end of code from python docs
#########################################################


# it would be nice if we could use datetime.strptime
# but timezone parsing (%z) is not working on Python 2.*
# http://stackoverflow.com/questions/20194496/iso-to-datetime-object-z-is-a-bad-directive
#
# so the 2 options are:
# - use dateutil (cons: external dependency)
# - implement just what is needed (cons: errors?)


_NAMED_REGEXPS = (
    ('{YEAR}',     '(?P<year>DIGIT{4})'),
    ('{MONTH}',    '(?P<month>DIGIT{2})'),
    ('{DAY}',      '(?P<day>DIGIT{2})'),
    ('{HOUR}',     '(?P<hour>DIGIT{2})'),
    ('{MINUTE}',   '(?P<minute>DIGIT{2})'),
    ('{SECOND}',   '(?P<second>DIGIT{2})'),
    ('{MICROSEC}', '(?P<microsec>DIGIT{6})'),
    ('{TIMEZONE}', '(?P<tzsign>[-+])(?P<tzhour>DIGIT{2})(?P<tzmin>DIGIT{2})'),
    ('DIGIT',      '[0-9]')
)


def _compile_parser(template):
    for name, regexp in _NAMED_REGEXPS:
        template = template.replace(name, regexp)
    match = re.compile(template + '$').match

    def convert(timeish: str):
        parts = match(timeish)
        if parts:
            values = parts.groupdict()

            # v for value
            def v(key, default):
                return int(values.get(key, default))
            tzoffset = (
                (-1 if values.get('tzsign', '+') == '-' else 1)
                *
                (v('tzhour', 0) * 60 + v('tzmin', 0)))

            return datetime(
                v('year', 0),
                v('month', 1),
                v('day', 1),
                v('hour', 0),
                v('minute', 0),
                v('second', 0),
                v('microsec', 0),
                FixedOffset(tzoffset, 'TZ' + str(tzoffset)))
    return convert


_DEFAULT_FULL_TIMESTAMP = '{YEAR}{MONTH}{DAY}T{HOUR}{MINUTE}{SECOND}{MICROSEC}{TIMEZONE}'
_parse_default_timestamp = _compile_parser(_DEFAULT_FULL_TIMESTAMP)

_ISO8601_PARSERS = [
    _parse_default_timestamp
] + [
    _compile_parser(template) for template in (
        '{YEAR}',
        '{YEAR}{MONTH}',
        '{YEAR}-{MONTH}',
        '{YEAR}{MONTH}{DAY}',
        '{YEAR}-{MONTH}-{DAY}',
        '{YEAR}{MONTH}{DAY}T{HOUR}{MINUTE}{SECOND}{TIMEZONE}',
        '{YEAR}-{MONTH}-{DAY}T{HOUR}:{MINUTE}:{SECOND}{TIMEZONE}',
        '{YEAR}-{MONTH}-{DAY}T{HOUR}:{MINUTE}:{SECOND}.{MICROSEC}{TIMEZONE}',
    )]


def parse_iso8601(timeish):
    '''
        Parse some iso-8601 date/time formats to a datetime with timezone.
    '''
    for parse in _ISO8601_PARSERS:
        parsed = parse(timeish)
        if parsed is not None:
            return parsed
    raise ValueError('Time is not in a recognised iso-8601 format', timeish)


_TIME_UNITS = {
    'y': 'years',
    'm': 'months',
    'w': 'weeks',
    'd': 'days',
    # expected to be less used:
    'H': 'hours',
    'M': 'minutes',
    'S': 'seconds',
}

_DELTA = r'([+-]?\d+)([{units}])'.format(units='wdHMS')
_DELTAS = f'(?:{_DELTA})*$'


def parse_timedelta(delta_str):
    '''
        Parse a time-delta of the format {AMOUNT}{UNIT}[{AMOUNT}{UNIT}[..]]

        E.g. '2w4d' for 2 weeks and 4 days
    '''
    match = re.match(_DELTAS, delta_str)
    if match:
        delta = timedelta()
        for amount, unit_abbrev in re.findall(_DELTA, delta_str):
            delta += timedelta(**{_TIME_UNITS[unit_abbrev]: int(amount)})
        return delta
    raise ValueError(
        'Invalid delta format: expecting {AMOUNT}{UNIT}* where UNIT is one of w,d,H,M,S',
        delta_str)


def timestamp():
    '''
        A string representation of this moment.

        With millisecond resolution and time zone so that
        - users recognise the time they made it,
          even if they live in non-trivial time zones (think: +0800)
        - when parsed back, can be compared with others
          even from different time zones
    '''
    return datetime.now(Local).strftime('%Y%m%dT%H%M%S%f%z')


# a not so forgiving parser
def time_from_timestamp(timestamp: str):
    '''
        Parse a datetime from a timestamp string - strict!
    '''
    parsed = _parse_default_timestamp(timestamp)
    if parsed is None:
        raise ValueError(
            'Not a full, basic timestamp (%s)' % _DEFAULT_FULL_TIMESTAMP,
            timestamp)
    return parsed


# The earliest time, beads could be created (actually it could be 10+ years later)
EPOCH_ISO = '20000101T000000000000+0000'
assert time_from_timestamp(EPOCH_ISO) == datetime(2000, 1, 1, 0, 0, 0, 0, FixedOffset(0, 'epoch'))


def time_from_user(timeish):
    '''
        Parse a datetime from user entered string - multiple formats

        Allows informal differences from current time.
    '''
    try:
        return parse_iso8601(timeish)
    except ValueError:
        pass
    try:
        # fall back to interpreting it as a time-delta added to `now`
        return datetime.now(Local) + parse_timedelta(timeish)
    except ValueError:
        raise ValueError(
            'Can not interpret string either as time or as delta', timeish)


class TimePrecision(Enum):
    """Enum representing the precision/granularity of a time specification."""
    YEAR = auto()
    MONTH = auto()
    DAY = auto()
    HOUR = auto()
    MINUTE = auto()
    SECOND = auto()
    MICROSECOND = auto()


def detect_time_precision(timeish_str: str) -> TimePrecision:
    """
    Detect the finest time unit specified in a user's time string.

    Args:
        timeish_str: Time specification string (e.g., "2025", "2025-09", "2025-09-19T14:30:45")

    Returns:
        TimePrecision enum value indicating the finest specified unit

    Raises:
        ValueError: If the time string format is not recognized
    """
    # Remove any leading/trailing whitespace
    timeish_str = timeish_str.strip()

    # Templates organized by string length: (length, precision, template)
    # Note: %f outputs 6 digits (microseconds), %z outputs 5 chars (+/-HHMM)
    templates_by_length = [
        # MICROSECOND precision
        (21, TimePrecision.MICROSECOND, '%Y%m%dT%H%M%S%f'),           # Compact: YYYYMMDDTHHMMSSnnnnnn (8+1+6+6)
        (26, TimePrecision.MICROSECOND, '%Y%m%dT%H%M%S%f%z'),         # Compact: YYYYMMDDTHHMMSSnnnnnn+HHMM (8+1+6+6+5)
        (26, TimePrecision.MICROSECOND, '%Y-%m-%dT%H:%M:%S.%f'),      # ISO8601: YYYY-MM-DDTHH:MM:SS.nnnnnn
        (31, TimePrecision.MICROSECOND, '%Y-%m-%dT%H:%M:%S.%f%z'),    # ISO8601: YYYY-MM-DDTHH:MM:SS.nnnnnn+HHMM
        # SECOND precision
        (15, TimePrecision.SECOND, '%Y%m%dT%H%M%S'),                  # Compact: YYYYMMDDTHHmmss (8+1+6)
        (19, TimePrecision.SECOND, '%Y-%m-%dT%H:%M:%S'),              # ISO8601: YYYY-MM-DDTHH:MM:SS
        (20, TimePrecision.SECOND, '%Y%m%dT%H%M%S%z'),                # Compact: YYYYMMDDTHHmmss+HHMM (8+1+6+5)
        (24, TimePrecision.SECOND, '%Y-%m-%dT%H:%M:%S%z'),            # ISO8601: YYYY-MM-DDTHH:MM:SS+HHMM
        # MINUTE precision
        (13, TimePrecision.MINUTE, '%Y%m%dT%H%M'),                    # Compact: YYYYMMDDTHHmm
        (16, TimePrecision.MINUTE, '%Y-%m-%dT%H:%M'),                 # ISO8601: YYYY-MM-DDTHH:MM
        (18, TimePrecision.MINUTE, '%Y%m%dT%H%M%z'),                  # Compact: YYYYMMDDTHHmm+HHMM
        (21, TimePrecision.MINUTE, '%Y-%m-%dT%H:%M%z'),               # ISO8601: YYYY-MM-DDTHH:MM+HHMM
        # HOUR precision
        (11, TimePrecision.HOUR, '%Y%m%dT%H'),                        # Compact: YYYYMMDDTHH
        (13, TimePrecision.HOUR, '%Y-%m-%dT%H'),                      # ISO8601: YYYY-MM-DDTHH
        (16, TimePrecision.HOUR, '%Y%m%dT%H%z'),                      # Compact: YYYYMMDDTHH+HHMM
        (18, TimePrecision.HOUR, '%Y-%m-%dT%H%z'),                    # ISO8601: YYYY-MM-DDTHH+HHMM
        # DAY precision
        (8, TimePrecision.DAY, '%Y%m%d'),                             # Compact: YYYYMMDD
        (10, TimePrecision.DAY, '%Y-%m-%d'),                          # ISO8601: YYYY-MM-DD
        # MONTH precision
        (6, TimePrecision.MONTH, '%Y%m'),                             # Compact: YYYYMM
        (7, TimePrecision.MONTH, '%Y-%m'),                            # ISO8601: YYYY-MM
        # YEAR precision
        (4, TimePrecision.YEAR, '%Y'),                                # Year only
    ]

    # Try to match against templates for the actual length
    actual_len = len(timeish_str)
    for expected_len, precision, template in templates_by_length:
        if expected_len == actual_len:
            try:
                datetime.strptime(timeish_str, template)
                return precision
            except (ValueError, TypeError):
                continue

    # No matching format found
    raise ValueError(f'Time specification not recognized: {timeish_str}')


def add_one_unit(dt: datetime, precision: TimePrecision) -> datetime:
    """
    Add one unit of time based on the specified precision.

    Handles month/year boundary conditions (e.g., December → January of next year).
    When adding a month/year would result in an invalid day (e.g., Feb 31), loops
    down to find the first valid day.

    Args:
        dt: datetime object to add to
        precision: TimePrecision enum indicating which unit to add

    Returns:
        New datetime with one unit added
    """
    if precision == TimePrecision.YEAR:
        # Add 1 year
        new_year = dt.year + 1
        # Loop down from current day until it succeeds (handles leap year edge cases)
        for day in range(dt.day, 0, -1):
            try:
                return dt.replace(year=new_year, day=day)
            except ValueError:
                continue
        # Should never reach here
        raise ValueError(f"Could not add 1 year to {dt}")

    elif precision == TimePrecision.MONTH:
        # Add 1 month, handling year boundary
        if dt.month == 12:
            new_year = dt.year + 1
            new_month = 1
        else:
            new_year = dt.year
            new_month = dt.month + 1

        # Loop down from current day until it succeeds (handles different month lengths)
        for day in range(dt.day, 0, -1):
            try:
                return dt.replace(year=new_year, month=new_month, day=day)
            except ValueError:
                continue
        # Should never reach here
        raise ValueError(f"Could not add 1 month to {dt}")

    elif precision == TimePrecision.DAY:
        return dt + timedelta(days=1)

    elif precision == TimePrecision.HOUR:
        return dt + timedelta(hours=1)

    elif precision == TimePrecision.MINUTE:
        return dt + timedelta(minutes=1)

    elif precision == TimePrecision.SECOND:
        return dt + timedelta(seconds=1)

    elif precision == TimePrecision.MICROSECOND:
        return dt + timedelta(microseconds=1)

    else:
        raise ValueError(f"Unknown precision: {precision}")
