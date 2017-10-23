#! -*- coding: utf-8 -*-
import time
import threading

_local = threading.local()
_local.cache = {}


def set_data(prefix, key, value):
    if not hasattr(_local, 'cache'):
        _local.cache = {}

    _local.cache.setdefault(prefix, {})
    _local.cache[prefix][key] = {
        'data': value,
        'version': int(time.time())
    }

def get_data(prefix, key, expire):
    if not hasattr(_local, 'cache'):
        return None

    data = _local.cache.get(prefix, {})
    if not data.has_key(key):
        return None

    now = int(time.time())
    if now - data[key]['version'] > expire:
        del data[key]
        return None

    return data[key]['data']

def set(key, value):
    set_data('default_cahche', key, value)

def get(key, expire=3600):
    return get_data('default_cahche', key, expire)
