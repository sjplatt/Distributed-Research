from __future__ import unicode_literals, print_function

import datetime
import decimal
import psycopg2
import hashlib
import logging
import copy
import json
import redis
import time as t
from time import time
import requests
import netifaces as ni
import dateutil.parser
ni.ifaddresses('eth0')

from django.conf import settings
from django.utils.encoding import force_bytes
from django.utils.timezone import utc

logger = logging.getLogger('django.db.backends')


def customSerializeDatetime(obj):
    if isinstance(obj, datetime.datetime):
        return "datetime:" + obj.isoformat()
    raise TypeError("Type not serializable Boo")

def customDeserializeDatetime(obj):
    if isinstance(obj, list):
        return [customDeserializeDatetime(d) for d in obj]
    try:
        if "datetime" in str(obj):
            return dateutil.parser.parse(obj.split(":")[1])
        else:
            return obj
    except:
        return obj

class CursorWrapper(object):
    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db
        self.mydata = []
        self.error = False
        self.rc = -1
        self.cache = redis.StrictRedis(host='localhost', port=6379, db=0)
        #self.cache.flushall()

    WRAP_ERROR_ATTRS = frozenset(['fetchone', 'fetchmany', 'fetchall', 'nextset'])

    def __getattr__(self, attr):
        cursor_attr = getattr(self.cursor, attr)
        if attr in CursorWrapper.WRAP_ERROR_ATTRS:
            wrapped = self.db.wrap_database_errors(cursor_attr)
            if attr == 'fetchone':
                def test_fetchone():
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
                    if self.error:
                        raise psycopg2.ProgrammingError

                    res = copy.deepcopy(self.mydata)[:size]
                    self.mydata = self.mydata[size:]
                    return res
                return self.db.wrap_database_errors(test_fetchmany)
            elif attr == 'fetchall':
                def test_fetchall():
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
            if attr == 'rowcount':
               cursor_attr = self.rc
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

    # Returns the datetime parameter from params
    def removeDateTime(self, params):
        if params is None:
            return None
        else:
            res = []
            for temp in params:
                if not isinstance(temp, datetime.datetime):
                    res += [temp]

            return tuple(res)
    
    # Returns True if data contains a datetime and false otherwise
    def containsDateTime(self, data):
        return "datetime.datetime" in str(data)

    def lookupCache(self, sql, params):
        params = self.removeDateTime(params)
        if params is None:
            value = self.cache.get(str(sql))
            if value:
                # with open("/home/samuel/Results/Cache.txt","a") as f:
                #     print("Hit: ", str(sql) +"\n", file=f)
                
                values = json.loads(value)
                self.mydata = [customDeserializeDatetime(v) for v in values[0]]
                self.rc = values[1]

                if values[2] == "False":
                    self.error = False
                else:
                    self.error = True
                return True
            else:
                # with open("/home/samuel/Results/Cache.txt","a") as f:
                #     print("Miss: ", str(sql) + "\n", file=f)
                return False
        else:
            value = self.cache.get(str(sql) + "|" + str(params))
            if value:
                # with open("/home/samuel/Results/Cache.txt","a") as f:
                #     print("Hit: ", str(sql) + "|" + str(params) + "\n", file=f)
                
                values = json.loads(value)
                self.mydata = [customDeserializeDatetime(v) for v in values[0]]
                self.rc = values[1]
                
                if values[2] == "False":
                    self.error = False
                else:
                    self.error = True
                return True
            else:
                # with open("/home/samuel/Results/Cache.txt","a") as f:
                #     print("Miss: ", str(sql) + "|" + str(params) + "\n", file=f)
                return False
    
    def putInCache(self, sql, params, data, rc, error):
        params = self.removeDateTime(params)
        
        if not "SELECT" in str(sql):
            return

        if params is None:
            key = str(sql)
            self.cache.set(key, json.dumps([data, rc, str(error)], default=customSerializeDatetime))
        else:
            key = str(sql) + "|" + str(params)
            self.cache.set(key, json.dumps([data, rc, str(error)], default=customSerializeDatetime))

    def sendQueryToCloud(self, sql, params):
        params = json.dumps(params, default=customSerializeDatetime)
        key = str(sql) + "|" + params
        ip = ni.ifaddresses('eth0')[2][0]['addr']
        # Send the request to the cloud and get the response
        r = requests.post("http://54.213.170.131:8000/polls/", data=key, headers={"source":ip})
        ret, data, rc, error = json.loads(r.text)
        
        data = [customDeserializeDatetime(d) for d in data]
        #print(ret, data,rc,error)
        return ret, data, rc, error

    def execute(self, sql, params=None):
        self.db.validate_no_broken_transaction()

        with self.db.wrap_database_errors:
            if params is None:
                # If the element is in the cache return None
                if self.lookupCache(sql,params):
                    print("HIT")
                    return None
                else:
                    print("MISS")
                    temp_time = t.time()
                    ret, val, rc, error = self.sendQueryToCloud(sql,None)
                    #ret = self.cursor.execute(sql)
                    print(t.time()-temp_time)
                    self.rc = rc
                    self.error = error
                    self.mydata = val
                    # try:
                    #     val = self.cursor.fetchall()
                    #     self.error = False
                    # except:
                    #     val = []
                    #     self.error = True
                    # self.mydata = val

                    # Only add to the cache if ret is not null
                    if ret == None:
                        self.putInCache(sql, params, self.mydata, self.rc, self.error)
                    return ret
            else:
                # If the element is in the cache return None
                if self.lookupCache(sql,params):
                    print("HIT")
                    return None
                else:
                    print("MISS")
                    temp_time = t.time()
                    #ret = self.cursor.execute(sql, params)
                    ret, val, rc, error = self.sendQueryToCloud(sql,params)
                    print(t.time()-temp_time)
                    self.rc = rc
                    self.error = error
                    self.mydata = val
                    # self.rc = self.cursor.rowcount
                    # try:
                    #     val = self.cursor.fetchall()
                    #     self.error = False
                    # except:
                    #     val = []
                    #     self.error = True
                    # self.mydata = val
                
                    # Only add to the cache if ret is not null
                    if ret == None:
                        self.putInCache(sql, params, self.mydata, self.rc, self.error)
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
