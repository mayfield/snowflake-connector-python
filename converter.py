#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
#
import decimal
import time
from datetime import datetime, timedelta, tzinfo
from logging import getLogger

import pytz

from .compat import (IS_BINARY, TO_UNICODE, IS_NUMERIC)
from .errorcode import (ER_NOT_SUPPORT_DATA_TYPE)
from .errors import (ProgrammingError)
from .sfbinaryformat import (binary_to_python,
                             binary_to_snowflake)
from .sfdatetime import (sfdatetime_total_seconds_from_timedelta,
                         sfdatetime_to_snowflake)

try:
    import numpy
except ImportError:
    numpy = None
try:
    import tzlocal
except ImportError:
    tzlocal = None

BITS_FOR_TIMEZONE = 14
MASK_OF_TIMEZONE = int((1 << BITS_FOR_TIMEZONE) - 1)
ZERO_TIMEDELTA = timedelta(seconds=0)
ZERO_EPOCH = datetime.utcfromtimestamp(0)

# Tzinfo cache
_TZINFO_CACHE = {}

logger = getLogger(__name__)


class SnowflakeConverter(object):

    def __init__(self, **kwargs):
        self._parameters = {}
        self._use_sfbinaryformat = kwargs.get('use_sfbinaryformat', False)
        self._use_numpy = kwargs.get('use_numpy', False) and numpy is not None

        logger.debug('use_sfbinaryformat: %s, use_numpy: %s',
                     self._use_sfbinaryformat, self._use_numpy)

    def set_parameters(self, parameters):
        self._parameters = {}
        for kv in parameters:
            self._parameters[kv[u'name']] = kv[u'value']

    def set_parameter(self, param, value):
        self._parameters[param] = value

    def get_parameters(self):
        return self._parameters

    def get_parameter(self, param):
        return self._parameters[param] if param in self._parameters else None

    def make_converters(self, chunk_rowtype, chunk_version):
        """
        Produce a list of conversion functions for a given chunk/version.

        The rowtype and verion values are the snowflake API represenation
        of table columns.
        """
        return [self.to_python_method(col[u'type'].upper(), col, chunk_version)
                for col in chunk_rowtype]

    def _generate_tzinfo_from_tzoffset(self, tzoffset_minutes):
        """
        Generates tzinfo object from tzoffset.
        """
        try:
            return _TZINFO_CACHE[tzoffset_minutes]
        except KeyError:
            pass
        _TZINFO_CACHE[tzoffset_minutes] = tz = SnowflakeTZ()
        tz.setoffset(tzoffset_minutes)
        return tz

    #
    # FROM Snowflake to Python Objects
    #
    def to_python_method(self, type_name, column, data_version):
        ctx = column.copy()
        ctx['data_version'] = data_version
        converters = [u'_{type_name}_to_python'.format(type_name=type_name)]
        if self._use_numpy:
            converters.insert(0, u'_{type_name}_numpy_to_python'.format(
                type_name=type_name))
        for conv in converters:
            try:
                return getattr(self, conv)(ctx)
            except AttributeError:
                pass
        logger.warn("No column converter found for type: %s", type_name)
        return None  # Skip conversion

    def _FIXED_to_python(self, ctx):
        return int if ctx['scale'] == 0 else decimal.Decimal

    def _FIXED_numpy_to_python(self, ctx):
        if ctx['scale']:
            return decimal.Decimal
        else:

            def conv(value):
                try:
                    return numpy.int64(value)
                except OverflowError:
                    return int(value)
            return conv

    def _REAL_to_python(self, ctx):
        return float

    def _REAL_numpy_to_python(self, ctx):
        return numpy.float64

    def _TEXT_to_python(self, ctx):
        return None  # skip conv

    def _BINARY_to_python(self, ctx):
        return binary_to_python

    def _DATE_to_python(self, ctx):
        """
        DATE to datetime

        No timezone is attached.
        """
        return lambda x: datetime.utcfromtimestamp(int(x) * 86400).date()

    def _DATE_numpy_to_python(self, ctx):
        """
        DATE to datetime

        No timezone is attached.
        """
        return lambda x: numpy.datetime64(int(x), 'D')

    def _extract_timestamp(self, value, ctx, has_tz=False):
        """Extracts timestamp from a raw data
        """
        scale = ctx['scale']
        try:
            value1 = decimal.Decimal(value)
            big_int = int(value1.scaleb(scale))  # removed fraction

            if has_tz:
                tzoffset = (big_int & MASK_OF_TIMEZONE) - 1440
                secs_wo_tz_off = (big_int >> BITS_FOR_TIMEZONE)
            else:
                tzoffset = 0
                secs_wo_tz_off = big_int

            nanoseconds = secs_wo_tz_off * 10 ** (9 - scale)
            microseconds = nanoseconds // 1000

            fraction_of_nanoseconds = nanoseconds % 1000000000
            return tzoffset, microseconds, fraction_of_nanoseconds, nanoseconds
        except decimal.InvalidOperation:
            return None, None, None

    def _pre_TIMESTAMP_TZ_to_python(self, value, ctx):
        u"""
        try to split value by space for handling new timestamp with timezone
        encoding format which has timezone index separate from the timestamp
        value
        """
        tzoffset_extracted = None

        valueComponents = str(value).split(" ")
        if len(valueComponents) == 2:
            tzoffset_extracted = int(valueComponents[1]) - 1440
            value = valueComponents[0]

        tzoffset, microseconds, fraction_of_nanoseconds, nanoseconds = \
            self._extract_timestamp(value, ctx,
                                    has_tz=(tzoffset_extracted is None))

        if tzoffset_extracted is not None:
            tzoffset = tzoffset_extracted

        if tzoffset is None:
            return None

        tzinfo_value = self._generate_tzinfo_from_tzoffset(tzoffset)

        t = ZERO_EPOCH + timedelta(seconds=(microseconds / float(1000000)))
        if pytz.utc != tzinfo_value:
            t += tzinfo_value.utcoffset(t, is_dst=False)
        return t.replace(tzinfo=tzinfo_value), fraction_of_nanoseconds

    def _TIMESTAMP_TZ_to_python(self, ctx):
        """
        TIMESTAMP TZ to datetime

        The timezone offset is piggybacked.
        """
        if ctx['data_version'] >= 1:
            return self._v1_TIMESTAMP_TZ_to_python(ctx)
        else:
            return self._v0_TIMESTAMP_TZ_to_python(ctx)

    def _v0_TIMESTAMP_TZ_to_python(self, ctx):
        """ Version 0 data is a single decimal value with the timezone stored
        in the lower bits and the epoch value in the higher bit range. """

        def conv(encoded_value):
            t = int(decimal.Decimal(encoded_value).scaleb(ctx['scale']))
            tzoffset = (t & MASK_OF_TIMEZONE) - 1440
            value = t >> BITS_FOR_TIMEZONE
            tzinfo = self._generate_tzinfo_from_tzoffset(tzoffset)
            return datetime.fromtimestamp(value, tz=tzinfo)
        return conv

    def _v1_TIMESTAMP_TZ_to_python(self, ctx):
        """ Version 1 data is formatted as `<TS> <TZ>`. """

        def conv(encoded_value):
            value, tz = encoded_value.split()
            tzinfo = self._generate_tzinfo_from_tzoffset(int(tz) - 1440)
            return datetime.fromtimestamp(float(value), tz=tzinfo)
        return conv

    def _TIMESTAMP_TZ_numpy_to_python(self, ctx):
        """TIMESTAMP TZ to datetime

        The timezone offset is piggybacked.
        """

        def conv(value):
            t, fraction_of_nanoseconds = self._pre_TIMESTAMP_TZ_to_python(
                value, ctx)
            ts = int(time.mktime(t.timetuple())) * 1000000000 + int(
                fraction_of_nanoseconds)
            return numpy.datetime64(ts, 'ns')
        return conv

    def _get_session_tz(self):
        """ Get the session timezone or use the local computer's timezone. """
        try:
            return pytz.timezone(self.get_parameter(u'TIMEZONE'))
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warn('converting to tzinfo failed')
            if tzlocal is not None:
                return tzlocal.get_localzone()
            else:
                try:
                    return datetime.timezone.utc
                except AttributeError:  # py2k
                    return pytz.timezone('UTC')

    def _pre_TIMESTAMP_LTZ_to_python(self, value, ctx):
        u""" TIMESTAMP LTZ to datetime

        This takes consideration of the session parameter TIMEZONE if
        available. If not, tzlocal is used
        """
        tzoffset, microseconds, fraction_of_nanoseconds, nanoseconds = \
            self._extract_timestamp(value, ctx)
        if tzoffset is None:
            return None
        try:
            tzinfo_value = pytz.timezone(self.get_parameter(u'TIMEZONE'))
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warn('converting to tzinfo_value failed')
            if tzlocal is not None:
                tzinfo_value = tzlocal.get_localzone()
            else:
                tzinfo_value = pytz.timezone('UTC')

        try:
            t0 = ZERO_EPOCH + timedelta(seconds=(microseconds / float(1000000)))
            t = pytz.utc.localize(t0, is_dst=False).astimezone(tzinfo_value)
            return t, fraction_of_nanoseconds
        except OverflowError:
            logger.debug(
                "OverflowError in converting from epoch time to "
                "timestamp_ltz: %s(ms). Falling back to use struct_time."
            )
            t = time.gmtime(microseconds / float(1000000))
            return t, fraction_of_nanoseconds

    def _TIMESTAMP_LTZ_to_python(self, ctx):
        tzinfo = self._get_session_tz()
        return lambda x: datetime.fromtimestamp(float(x), tz=tzinfo)

    def _TIMESTAMP_LTZ_numpy_to_python(self, ctx):

        def conv(value):
            t, fraction_of_nanoseconds = self._pre_TIMESTAMP_LTZ_to_python(
                value, ctx)
            ts = int(time.mktime(t.timetuple())) * 1000000000 + int(
                fraction_of_nanoseconds)
            return numpy.datetime64(ts, 'ns')
        return conv

    _TIMESTAMP_to_python = _TIMESTAMP_LTZ_to_python

    def _pre_TIMESTAMP_NTZ_to_python(self, value, ctx):
        """TIMESTAMP NTZ to datetime

        No timezone info is attached.
        """
        tzoffset, microseconds, fraction_of_nanoseconds, nanoseconds = \
            self._extract_timestamp(value, ctx)

        if tzoffset is None:
            return None, None, None

        return nanoseconds, microseconds, fraction_of_nanoseconds

    def _TIMESTAMP_NTZ_to_python(self, ctx):
        """
        TIMESTAMP NTZ to datetime

        No timezone info is attached.
        """
        return lambda value: datetime.utcfromtimestamp(float(value))

    def _TIMESTAMP_NTZ_numpy_to_python(self, ctx):
        """
        TIMESTAMP NTZ to datetime64

        No timezone info is attached.
        """

        def conv(value):
            nanoseconds, _, _ = self._pre_TIMESTAMP_NTZ_to_python(value, ctx)
            return numpy.datetime64(nanoseconds, 'ns')
        return conv

    def _extract_time(self, value, ctx):
        u"""Extracts time from raw data

        Returns a pair containing microseconds since midnight and nanoseconds
        since the last whole-numebr second. The last 6 digits of microseconds
        will be the same as the first 6 digits of nanoseconds.
        """
        scale = ctx['scale']
        try:
            value1 = decimal.Decimal(value)
            big_int = int(value1.scaleb(scale))  # removed fraction

            nanoseconds = big_int * 10 ** (9 - scale)
            microseconds = nanoseconds // 1000

            fraction_of_nanoseconds = nanoseconds % 1000000000
            return microseconds, fraction_of_nanoseconds
        except decimal.InvalidOperation:
            return None, None

    def _TIME_to_python(self, ctx):
        """
        TIME to formatted string, SnowflakeDateTime, or datetime.time

        No timezone is attached.
        """
        return lambda x: datetime.utcfromtimestamp(float(x)).time()

    def _VARIANT_to_python(self, ctx):
        return None  # skip conv

    _OBJECT_to_python = _VARIANT_to_python

    _ARRAY_to_python = _VARIANT_to_python

    def _BOOLEAN_to_python(self, ctx):
        return lambda value: value in (u'1', u'TRUE')

    #
    # From Python to Snowflake
    #
    def to_snowflake(self, value):
        type_name = value.__class__.__name__.lower()
        return getattr(self, u"_{type_name}_to_snowflake".format(
            type_name=type_name))(value)

    def _int_to_snowflake(self, value):
        return int(value)

    def _long_to_snowflake(self, value):
        return long(value)

    def _float_to_snowflake(self, value):
        return float(value)

    def _str_to_snowflake(self, value):
        return TO_UNICODE(value)

    def _unicode_to_snowflake(self, value):
        return TO_UNICODE(value)

    def _bytes_to_snowflake(self, value):
        return binary_to_snowflake(value)

    def _bytearray_to_snowflake(self, value):
        return binary_to_snowflake(value)

    def _bool_to_snowflake(self, value):
        return value

    def _nonetype_to_snowflake(self, value):
        del value
        return None

    def _total_seconds_from_timedelta(self, td):
        return sfdatetime_total_seconds_from_timedelta(td)

    def _datetime_to_snowflake(self, value):
        tzinfo_value = value.tzinfo
        if tzinfo_value:
            if pytz.utc != tzinfo_value:
                td = tzinfo_value.utcoffset(value, is_dst=False)
            else:
                td = ZERO_TIMEDELTA
            sign = u'+' if td >= ZERO_TIMEDELTA else u'-'
            td_secs = sfdatetime_total_seconds_from_timedelta(td)
            h, m = divmod(abs(td_secs // 60), 60)
            if value.microsecond:
                return (
                    u'{year:d}-{month:02d}-{day:02d} '
                    u'{hour:02d}:{minute:02d}:{second:02d}.'
                    u'{microsecond:06d}{sign}{tzh:02d}:{tzm:02d}').format(
                    year=value.year, month=value.month, day=value.day,
                    hour=value.hour, minute=value.minute,
                    second=value.second,
                    microsecond=value.microsecond, sign=sign, tzh=h,
                    tzm=m
                )
            return (
                u'{year:d}-{month:02d}-{day:02d} '
                u'{hour:02d}:{minute:02d}:{second:02d}'
                u'{sign}{tzh:02d}:{tzm:02d}').format(
                year=value.year, month=value.month, day=value.day,
                hour=value.hour, minute=value.minute,
                second=value.second,
                sign=sign, tzh=h, tzm=m
            )
        else:
            if value.microsecond:
                return (u'{year:d}-{month:02d}-{day:02d} '
                        u'{hour:02d}:{minute:02d}:{second:02d}.'
                        u'{microsecond:06d}').format(
                    year=value.year, month=value.month, day=value.day,
                    hour=value.hour, minute=value.minute,
                    second=value.second,
                    microsecond=value.microsecond
                )
            return (u'{year:d}-{month:02d}-{day:02d} '
                    u'{hour:02d}:{minute:02d}:{second:02d}').format(
                year=value.year, month=value.month, day=value.day,
                hour=value.hour, minute=value.minute,
                second=value.second
            )

    def _sfdatetime_to_snowflake(self, value):
        return sfdatetime_to_snowflake(value)

    def date_to_snowflake(self, value):
        """
        Converts Date object to Snowflake object
        """
        return self._date_to_snowflake(value)

    def _date_to_snowflake(self, value):
        return u'{year:d}-{month:02d}-{day:02d}'.format(year=value.year,
                                                        month=value.month,
                                                        day=value.day)

    def _time_to_snowflake(self, value):
        if value.microsecond:
            return value.strftime(u'%H:%M:%S.%%06d') % value.microsecond
        return value.strftime(u'%H:%M:%S')

    def _struct_time_to_snowflake(self, value):
        tzinfo_value = self._generate_tzinfo_from_tzoffset(
            -time.timezone // 60)
        t = datetime.fromtimestamp(time.mktime(value))
        if pytz.utc != tzinfo_value:
            t += tzinfo_value.utcoffset(t)
        t = t.replace(tzinfo=tzinfo_value)
        return self._datetime_to_snowflake(t)

    def _timedelta_to_snowflake(self, value):
        (hours, r) = divmod(value.seconds, 3600)
        (mins, secs) = divmod(r, 60)
        hours += value.days * 24
        if value.microseconds:
            return (u'{hour:02d}:{minute:02d}:{second:02d}.'
                    u'{microsecond:06d}').format(
                hour=hours, minute=mins,
                second=secs,
                microsecond=value.microseconds)
        return u'{hour:02d}:{minute:02d}:{second:02d}'.format(hour=hours,
                                                              minute=mins,
                                                              second=secs)

    def _decimal_to_snowflake(self, value):
        if isinstance(value, decimal.Decimal):
            return TO_UNICODE(value)

        return None

    def _list_to_snowflake(self, value):
        return [SnowflakeConverter.quote(v0) for v0 in
                [SnowflakeConverter.escape(v) for v in value]]

    _tuple_to_snowflake = _list_to_snowflake

    def __numpy_to_snowflake(self, value):
        return value

    _int8_to_snowflake = __numpy_to_snowflake
    _int16_to_snowflake = __numpy_to_snowflake
    _int32_to_snowflake = __numpy_to_snowflake
    _int64_to_snowflake = __numpy_to_snowflake
    _uint8_to_snowflake = __numpy_to_snowflake
    _uint16_to_snowflake = __numpy_to_snowflake
    _uint32_to_snowflake = __numpy_to_snowflake
    _uint64_to_snowflake = __numpy_to_snowflake
    _float16_to_snowflake = __numpy_to_snowflake
    _float32_to_snowflake = __numpy_to_snowflake
    _float64_to_snowflake = __numpy_to_snowflake

    def _datetime64_to_snowflake(self, value):
        return TO_UNICODE(value)

    def _quoted_name_to_snowflake(self, value):
        return TO_UNICODE(value)

    def __getattr__(self, item):
        if item.endswith('_to_snowflake'):
            raise ProgrammingError(
                msg=u"Binding data in type ({0}) is not supported.".format(
                    item[1:item.find('_to_snowflake')]),
                errno=ER_NOT_SUPPORT_DATA_TYPE
            )
        raise AttributeError('No method is available: {0}'.format(item))

    @staticmethod
    def escape(value):
        if isinstance(value, list):
            return value
        if value is None or IS_NUMERIC(value) or IS_BINARY(value):
            return value
        res = value
        res = res.replace(u'\\', u'\\\\')
        res = res.replace(u'\n', u'\\n')
        res = res.replace(u'\r', u'\\r')
        res = res.replace(u'\047', u'\134\047')  # single quotes
        return res

    @staticmethod
    def quote(value):
        if isinstance(value, list):
            return ','.join(value)
        if value is None:
            return u'NULL'
        elif isinstance(value, bool):
            return u'TRUE' if value else u'FALSE'
        elif IS_NUMERIC(value):
            return TO_UNICODE(repr(value))
        elif IS_BINARY(value):
            # Binary literal syntax
            return u"X'{0}'".format(value.decode('ascii'))

        return u"'{0}'".format(value)


class SnowflakeTZ(tzinfo):
    """ Handle arbitrary timezones. """

    def setoffset(self, tzoffset_minutes):
        sign = u'P' if tzoffset_minutes >= 0 else u'N'
        abs_tzoffset_minutes = abs(tzoffset_minutes)
        hour, minute = divmod(abs_tzoffset_minutes, 60)
        self._tzoffset_minutes = tzoffset_minutes
        self._name = u'GMT{sign:s}{hour:02d}{minute:02d}'.format(
            sign=sign,
            hour=hour,
            minute=minute)
        #super(SnowflakeTZ, self).__init__()

    #def __str__(self):
    #    return self._name

    def utcoffset(self, dt, is_dst=False):
        return timedelta(minutes=self._tzoffset_minutes)

    def tzname(self, dt):
        return self._name

    def dst(self, dt):
        return ZERO_TIMEDELTA
