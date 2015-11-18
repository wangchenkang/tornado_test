#! -*- coding: utf-8 -*-
import pytz
from datetime import datetime

timezone = 'Asia/Chongqing'

def utc_to_cst(dt):
    dt = dt.replace(tzinfo=pytz.utc)
    tz = pytz.timezone(timezone)
    return dt.astimezone(tz)

def date_to_query(dt, time_format='%Y-%m-%d'):
    return dt.strftime(time_format)
