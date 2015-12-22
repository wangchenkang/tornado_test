#! -*- coding: utf-8 -*-
import pytz
from datetime import datetime
from tornado.escape import url_unescape

timezone = 'Asia/Chongqing'

def utc_to_cst(dt):
    dt = dt.replace(tzinfo=pytz.utc)
    tz = pytz.timezone(timezone)
    return dt.astimezone(tz)

def date_to_query(dt, time_format='%Y-%m-%d'):
    return dt.strftime(time_format)

def date_from_query(dt_string, time_format='%Y-%m-%d'):
    return datetime.strptime(dt_string, time_format)

def fix_course_id(course_id):
    return url_unescape(course_id).replace(' ', '+')
