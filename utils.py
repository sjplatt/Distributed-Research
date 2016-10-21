from __future__ import unicode_literals, print_function

import datetime
import decimal
import psycopg2
import hashlib
import logging
import copy
from time import time

from django.conf import settings
from django.utils.encoding import force_bytes
from django.utils.timezone import utc

logger = logging.getLogger('django.db.backends')


class CursorWrapper(object):
    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db
        self.mydata = []
        self.error = False

    WRAP_ERROR_ATTRS = frozenset(['fetchone', 'fetchmany', 'fetchall', 'nextset'])

    def __getattr__(self, attr):
        with open("/home/sjplatt/Attributes.txt","a") as f:
            print(attr, "\n", file=f)

        cursor_attr = getattr(self.cursor, attr)
        if attr in CursorWrapper.WRAP_ERROR_ATTRS:
            wrapped = self.db.wrap_database_errors(cursor_attr)
            if attr == 'fetchone':
                def test_fetchone():
                    with open("/home/sjplatt/Wrapped.txt","a") as f:
                        print("fetchone: ", self.mydata, "\n", file=f)
                    
                    if self.error:
                        raise psycopg2.ProgrammingError

                    if not self.mydata == []:
                        dat = self.mydata[0]
                        self.mydata = self.mydata[1:]
                        return dat
                    else:
                        return None
                return self.db.wrap_database_errors(test_fetchone)
            elif attr == 'fetchmany':
                def test_fetchmany(size=self.cursor.arraysize):
                    with open("/home/sjplatt/Wrapped.txt","a") as f:
                        print("fetchmany: ", size, " ", self.mydata[:size], "\n", file=f)
                    if self.error:
                        raise psycopg2.ProgrammingError

                    res = copy.deepcopy(self.mydata)[:size]
                    self.mydata = self.mydata[size:]
                    return res
                return self.db.wrap_database_errors(test_fetchmany)
            elif attr == 'fetchall':
                def test_fetchall():
                    with open("/home/sjplatt/Wrapped.txt","a") as f:
                        print("fetchall: ", self.mydata, "\n", file=f)
                    if self.error:
                        raise psycopg2.ProgrammingError
                    res = copy.deepcopy(self.mydata)
                    self.mydata = []
                    return res
                return self.db.wrap_database_errors(test_fetchall)
            elif attr == 'nextset':
                return wrapped
            return wrapped
        else:
            return cursor_attr

    def __iter__(self):
        with self.db.wrap_database_errors:
            for item in self.mydata:
                yield item

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # Ticket #17671 - Close instead of passing thru to avoid backend
        # specific behavior. Catch errors liberally because errors in cleanup
        # code aren't useful.
        try:
            self.close()
        except self.db.Database.Error:
            pass

    # The following methods cannot be implemented in __getattr__, because the
    # code must run when the method is invoked, not just when it is accessed.

    def callproc(self, procname, params=None):
        self.db.validate_no_broken_transaction()
        with self.db.wrap_database_errors:
            if params is None:
                return self.cursor.callproc(procname)
            else:
                return self.cursor.callproc(procname, params)

    def execute(self, sql, params=None):
        self.db.validate_no_broken_transaction()
        with open("/home/sjplatt/Queries.txt", "a") as f:
            print("Query: ", sql, "\n Params: ", params, "\n", file=f)

        with self.db.wrap_database_errors:
            if params is None:
                ret = self.cursor.execute(sql)
                try:
                    val = self.cursor.fetchall()
                    self.error = False
                except:
                    val = []
                    self.error = True
                with open("/home/sjplatt/Results.txt", "a") as f:
                    print("Result: ", val, "\n", file=f)
                self.mydata = val
                return ret
            else:
                ret = self.cursor.execute(sql, params)
                try:
                    val = self.cursor.fetchall()
                    self.error = False
                except:
                    val = []
                    self.error = True
                with open("/home/sjplatt/Results.txt","a") as f:
                    print("Result: ",val, "\n", file=f)
                self.mydata = val
                return ret


    def executemany(self, sql, param_list):
        self.db.validate_no_broken_transaction()
        with self.db.wrap_database_errors:
            ret = self.cursor.executemany(sql, param_list)
            try:
                val = self.cursor.fetchall()
                self.error = False
            except:
                val = []
                self.error = True
            with open("/home/sjplatt/Results.txt", "a") as f:
                print("Result many: ",val, "\n", file=f)
            self.mydata = val
            return ret


class CursorDebugWrapper(CursorWrapper):

    # XXX callproc isn't instrumented at this time.

    def execute(self, sql, params=None):
        start = time()
        try:
            return super(CursorDebugWrapper, self).execute(sql, params)
        finally:
            stop = time()
            duration = stop - start
            sql = self.db.ops.last_executed_query(self.cursor, sql, params)
            self.db.queries_log.append({
                'sql': sql,
                'time': "%.3f" % duration,
            })
            logger.debug(
                '(%.3f) %s; args=%s', duration, sql, params,
                extra={'duration': duration, 'sql': sql, 'params': params}
            )

    def executemany(self, sql, param_list):
        start = time()
        try:
            return super(CursorDebugWrapper, self).executemany(sql, param_list)
        finally:
            stop = time()
            duration = stop - start
            try:
                times = len(param_list)
            except TypeError:           # param_list could be an iterator
                times = '?'
            self.db.queries_log.append({
                'sql': '%s times: %s' % (times, sql),
                'time': "%.3f" % duration,
            })
            logger.debug(
                '(%.3f) %s; args=%s', duration, sql, param_list,
                extra={'duration': duration, 'sql': sql, 'params': param_list}
            )


###############################################
# Converters from database (string) to Python #
###############################################

def typecast_date(s):
    return datetime.date(*map(int, s.split('-'))) if s else None  # returns None if s is null


def typecast_time(s):  # does NOT store time zone information
    if not s:
        return None
    hour, minutes, seconds = s.split(':')
    if '.' in seconds:  # check whether seconds have a fractional part
        seconds, microseconds = seconds.split('.')
    else:
        microseconds = '0'
    return datetime.time(int(hour), int(minutes), int(seconds), int((microseconds + '000000')[:6]))


def typecast_timestamp(s):  # does NOT store time zone information
    # "2005-07-29 15:48:00.590358-05"
    # "2005-07-29 09:56:00-05"
    if not s:
        return None
    if ' ' not in s:
        return typecast_date(s)
    d, t = s.split()
    # Extract timezone information, if it exists. Currently we just throw
    # it away, but in the future we may make use of it.
    if '-' in t:
        t, tz = t.split('-', 1)
        tz = '-' + tz
    elif '+' in t:
        t, tz = t.split('+', 1)
        tz = '+' + tz
    else:
        tz = ''
    dates = d.split('-')
    times = t.split(':')
    seconds = times[2]
    if '.' in seconds:  # check whether seconds have a fractional part
        seconds, microseconds = seconds.split('.')
    else:
        microseconds = '0'
    tzinfo = utc if settings.USE_TZ else None
    return datetime.datetime(
        int(dates[0]), int(dates[1]), int(dates[2]),
        int(times[0]), int(times[1]), int(seconds),
        int((microseconds + '000000')[:6]), tzinfo
    )


def typecast_decimal(s):
    if s is None or s == '':
        return None
    return decimal.Decimal(s)


###############################################
# Converters from Python to database (string) #
###############################################

def rev_typecast_decimal(d):
    if d is None:
        return None
    return str(d)


def truncate_name(name, length=None, hash_len=4):
    """Shortens a string to a repeatable mangled version with the given length.
    """
    if length is None or len(name) <= length:
        return name

    hsh = hashlib.md5(force_bytes(name)).hexdigest()[:hash_len]
    return '%s%s' % (name[:length - hash_len], hsh)


def format_number(value, max_digits, decimal_places):
    """
    Formats a number into a string with the requisite number of digits and
    decimal places.
    """
    if value is None:
        return None
    if isinstance(value, decimal.Decimal):
        context = decimal.getcontext().copy()
        if max_digits is not None:
            context.prec = max_digits
        if decimal_places is not None:
            value = value.quantize(decimal.Decimal(".1") ** decimal_places, context=context)
        else:
            context.traps[decimal.Rounded] = 1
            value = context.create_decimal(value)
        return "{:f}".format(value)
    if decimal_places is not None:
        return "%.*f" % (decimal_places, value)
    return "{:f}".format(value)