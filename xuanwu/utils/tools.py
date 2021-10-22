# -*- coding: utf-8 -*-
"""
  @ Author:   Mr.Hat
  @ Email:    shenghong6560@gmail.com
  @ Date:     2020/9/27 17:17
  @ Description:
  @ History:
"""

import uuid
import time
import decimal
import datetime


def get_cur_timestamp():
    """ 获取当前时间戳
    """
    ts = int(time.time())
    return ts


def get_cur_timestamp_ms():
    """ 获取当前时间戳(毫秒)
    """
    ts = int(time.time() * 1000)
    return ts


def get_cur_datetime_m(fmt='%Y%m%d%H%M%S%f'):
    """ 获取当前日期时间字符串，包含 年 + 月 + 日 + 时 + 分 + 秒 + 微妙
    """
    today = datetime.datetime.today()
    str_m = today.strftime(fmt)
    return str_m


def date_str_to_dt(date_str=None, fmt='%Y%m%d', delta_day=0):
    """ 日期字符串转换到datetime对象
    :param date_str 日期字符串
    :param fmt 日期字符串格式
    :param delta_day 相对天数，<0减相对天数，>0加相对天数
    """
    if not date_str:
        dt = datetime.datetime.today()
    else:
        dt = datetime.datetime.strptime(date_str, fmt)
    if delta_day:
        dt += datetime.timedelta(days=delta_day)
    return dt


def dt_to_date_str(dt=None, fmt='%Y%m%d', delta_day=0):
    """ datetime对象转换到日期字符串
    :param dt datetime对象
    :param fmt 返回的日期字符串格式
    :param delta_day 相对天数，<0减相对天数，>0加相对天数
    """
    if not dt:
        dt = datetime.datetime.today()
    if delta_day:
        dt += datetime.timedelta(days=delta_day)
    str_d = dt.strftime(fmt)
    return str_d


def ts_to_datetime_str(ts=None, fmt="%Y-%m-%d %H:%M:%S"):
    """Convert timestamp to date time string.

    Args:
        ts: Timestamp, millisecond.
        fmt: Date time format, default is `%Y-%m-%d %H:%M:%S`.

    Returns:
        Date time string.
    """
    if not ts:
        ts = get_cur_timestamp()
    dt = datetime.datetime.fromtimestamp(int(ts))
    return dt.strftime(fmt)


def datetime_str_to_ts(dt_str, fmt="%Y-%m-%d %H:%M:%S"):
    """Convert date time string to timestamp.

    Args:
        dt_str: Date time string.
        fmt: Date time format, default is `%Y-%m-%d %H:%M:%S`.

    Returns:
        ts: Timestamp, millisecond.
    """
    ts = int(time.mktime(datetime.datetime.strptime(dt_str, fmt).timetuple()))
    return ts


def get_utc_time():
    """Get current UTC time."""
    utc_t = datetime.datetime.utcnow()
    return utc_t


def utctime_str_to_ts(utctime_str, fmt="%Y-%m-%dT%H:%M:%S.%fZ"):
    """ 将UTC日期时间格式字符串转换成时间戳
    :param utctime_str 日期时间字符串  eg: 2019-03-04T09:14:27.806Z
    :param fmt 日期时间字符串格式
    :return timestamp 时间戳(秒)
    """
    dt = datetime.datetime.strptime(utctime_str, fmt)
    timestamp = int(dt.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).timestamp())
    return timestamp


def utctime_str_to_mts(utctime_str, fmt="%Y-%m-%dT%H:%M:%S.%fZ"):
    """ 将UTC日期时间格式字符串转换成时间戳（毫秒）
    :param utctime_str 日期时间字符串 eg: 2019-03-04T09:14:27.806Z
    :param fmt 日期时间字符串格式
    :return timestamp 时间戳(毫秒)
    """
    dt = datetime.datetime.strptime(utctime_str, fmt)
    timestamp = int(dt.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).timestamp() * 1000)
    return timestamp


def get_utctime_str(fmt="%Y-%m-%dT%H:%M:%S.%fZ"):
    """Get current UTC time string.

    Args:
        fmt: UTC time format, e.g. `%Y-%m-%dT%H:%M:%S.%fZ`.

    Returns:
        utctime_str: UTC time string, e.g. `2019-03-04T09:14:27.806Z`.
    """
    utctime = get_utc_time()
    utctime_str = utctime.strftime(fmt)
    return utctime_str


def get_uuid1():
    """Generate a UUID based on the host ID and current time

    Returns:
        s: UUID1 string.
    """
    uid1 = uuid.uuid1()
    s = str(uid1)
    return s


def get_uuid3(str_in):
    """Generate a UUID using an MD5 hash of a namespace UUID and a name

    Args:
        str_in: Input string.

    Returns:
        s: UUID3 string.
    """
    uid3 = uuid.uuid3(uuid.NAMESPACE_DNS, str_in)
    s = str(uid3)
    return s


def get_uuid4():
    """Generate a random UUID.

    Returns:
        s: UUID5 string.
    """
    uid4 = uuid.uuid4()
    s = str(uid4)
    return s


def get_uuid5(str_in):
    """Generate a UUID using a SHA-1 hash of a namespace UUID and a name

    Args:
        str_in: Input string.

    Returns:
        s: UUID5 string.
    """
    uid5 = uuid.uuid5(uuid.NAMESPACE_DNS, str_in)
    s = str(uid5)
    return s


def float_to_str(f, p=20):
    """ Convert the given float to a string, without resorting to scientific notation.
    :param f 浮点数参数
    :param p 精读
    """
    if type(f) == str:
        f = float(f)
    ctx = decimal.Context(p)
    d1 = ctx.create_decimal(repr(f))
    return format(d1, 'f')


def noround_float(f, n):
    """ Get the given n digit float to the string, without rounding up or rounding down.
    """
    f_str = str(f)
    a, b, c = f_str.partition('.')
    c = (c + "0" * n)[:n]
    return ".".join([a, c])


def decimal_digit(digit):
    """ Get the Decimal n digit present for Decimal.
    """
    f_str = '0.'
    for i in range(digit):
        f_str += '0'
    return f_str


def decimal_digits(val: float):
    """ 获取小数位数
    :param val:
    :return:
    """
    val_str = str(val)
    digits_location = val_str.find('.')
    if digits_location:
        return len(val_str[digits_location + 1:])