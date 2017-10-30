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
NEWCLOUD_COHORT_GROUP_KEY = 5000000000

THRIFT_SERVER = ['10.0.2.131', '10.0.2.132', '10.0.2.133', '10.0.2.134']
SEEK_FIELD = ['event_type','video_id', 'event_time', 'platform', 'duration', 'not_percent','video_st', 'video_et','video_last', 'seek_len']
MYSQL_PARAMS = {
            'teacher_power': {'host': 'tap-authority.xuetangx.info', 'db': 'tap_authority', 'user': 'data', 'password': 'data@xuetangx'},
            'auth_userprofile': {'host': 'datamysql.xuetangx.info', 'db': 'edxapp', 'user': 'mysql_ro', 'password': 'xuetangx.com168mysql'}
}

ROW_FILTER = {
                'grade':{'index': 'tap_table_grade', 'doc_type': 'grade_summary'},\
                'question':{'index': 'tap_table_question', 'doc_type': 'chapter_question'},\
                'discussion': {'index':'tap_table_discussion', 'doc_type': 'discussion_summary'},\
                'video': {'index': 'tap_table_video', 'doc_type': 'chapter_seq_video'},\
                'enroll': {'index': 'tap_table_enroll', 'doc_type': 'enroll_summary'},\
                'focus': {'index': 'problems_focused', 'doc_type': 'video_seek_summary'},\
                'warning': {'index': 'problems_focused', 'doc_type': 'study_warning_person'},\
                'newcloud_grade': {'index': 'newcloud_type', 'doc_type': 'score_realtime'},\
                }
DISPATCH_OPTIMIZE = True
ES_INDEX = 'tap'
NEWCLOUD_ES_INDEX = 'newcloud_tap'
ES_INDEX_LOCK = 'tap_lock'
NEWCLOUD_ACADEMIC_ES_INDEX = 'education_data'
TAP_REALTIME_INDEX = 'realtime'
NEWCLOUD_DATACONF = ['score_video', 'score_comment', 'score_rule', 'score_rule_item']
#ES_INDEX = 'tapgo'
with open(CONFIG_FILE) as f:
    document = f.read()
    locals().update(yaml.load(document))

try:
    from private_settings import *
except:
    pass
