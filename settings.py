#! -*- coding: utf-8 -*-
import os
import yaml

PROJECT_NAME = "TAP API"

PROJECT_PATH = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(PROJECT_PATH, "settings.yaml")
COOKIE_SECRET = "11a0f5a5df09021b09ae9811ee0c2c11e64c781b"

MOOC_GROUP_KEY = 1
SPOC_GROUP_KEY = 2
TSINGHUA_GROUP_KEY = 3
ELECTIVE_ALL_GROUP_KEY = 4
COHORT_GROUP_KEY = 50000
ELECTIVE_GROUP_KEY = 80000


SEEK_FIELD = ['event_type','video_id', 'event_time', 'platform', 'duration', 'not_percent','video_st', 'video_et','video_last', 'seek_len']
MYSQL_PARAMS = {
            'teacher_power': {'host': 'tap-authority.xuetangx.info', 'db': 'tap_authority', 'user': 'data', 'password': 'data@xuetangx'}
}

DISPATCH_OPTIMIZE = True
ES_INDEX = 'tap'
#ES_INDEX = 'tapgo'
with open(CONFIG_FILE) as f:
    document = f.read()
    locals().update(yaml.load(document))

try:
    from private_settings import *
except:
    pass
