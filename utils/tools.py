#! -*- coding: utf-8 -*-
import pytz
from datetime import datetime, timedelta
from tornado.escape import url_unescape
import math
import settings

timezone = 'Asia/Chongqing'

def utc_to_cst(dt):
    dt = dt.replace(tzinfo=pytz.utc)
    tz = pytz.timezone(timezone)
    return dt.astimezone(tz)

def date_to_str(dt, time_format="%Y-%m-%d"):
    return dt.strftime(time_format)

def date_from_query(dt_string, time_format='%Y-%m-%d'):
    return datetime.strptime(dt_string, time_format)

#新的时间格式加了个+08:00
def date_from_new_date(time_string, time_format="%Y-%m-%dT%X+08:00"):
    try:
        return datetime.strptime(time_string, time_format)
    except (AttributeError, TypeError):
        return None

def is_ended(end_time, now=None):
    if now is None:
        now = datetime.utcnow()
    if end_time == 'now':
        return False
    end_time = date_convert(end_time)
    return end_time and end_time < now
    
def date_convert(date):
    if isinstance(date, basestring):
        return date_from_string(date)
    return date

def date_from_string(time_string, time_format='%Y-%m-%dT%X'):
    try:
        return datetime.strptime(time_string, time_format)
    except(AttributeError, TypeError):
        return None

def datedelta(dt, day):
    return date_to_str(datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=day))

def fix_course_id(course_id):
    return url_unescape(course_id).replace(' ', '+')

def var(data, total=None):
    if not total:
        total = len(data)
    data += [0]*(total-len(data))
    avg = sum(data)/total
    return math.sqrt(sum([(i-avg)*(i-avg) for i in data])/total)

def get_group_type(group_key):
    course_group_key = group_key
    group_keys = [('elective', settings.ELECTIVE_GROUP_KEY), \
                  ('cohort', settings.COHORT_GROUP_KEY),\
                  ('elective', settings.ELECTIVE_ALL_GROUP_KEY),\
                  ('tsinghua', settings.TSINGHUA_GROUP_KEY), \
                  ('spoc', settings.SPOC_GROUP_KEY), \
                  ('mooc', settings.MOOC_GROUP_KEY)]

    for group_key in group_keys:
        if course_group_key >= group_key[1]:
            return group_key[0]

