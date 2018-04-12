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
CREDIT_GROUP_KEY = 5
COHORT_GROUP_KEY = 50000
ELECTIVE_GROUP_KEY = 80000
NEWCLOUD_COHORT_GROUP_KEY = 5000000000

THRIFT_SERVER = ['10.0.2.131', '10.0.2.132', '10.0.2.133', '10.0.2.134']
SEEK_FIELD = ['event_type','video_id', 'event_time', 'platform', 'duration', 'not_percent','video_st', 'video_et','video_last', 'seek_len']
MYSQL_PARAMS = {
            'teacher_power': {'host': 'tap-authority.xuetangx.info', 'db': 'tap_authority', 'user': 'data', 'password': 'data@xuetangx'},
            'auth_userprofile': {'host': 'datamysql.xuetangx.info', 'db': 'edxapp', 'user': 'mysql_ro', 'password': 'xuetangx.com168mysql'},
	    'course_manage': {'host': 'tap-authority.xuetangx.info', 'db': 'course_manage', 'user': 'course', 'password': 'course@xuetangx'}
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
MOOCND_ES_INDEX = 'moocnd_video'
NEWCLOUD_ES_INDEX = 'newcloud_tap'
ES_INDEX_LOCK = 'tap_lock'
SEARCH_ES_INDEX = 'course'
CREDIT_ES_INDEX = 'credit_data'

ES_INDEXS = ['tap', 'tap_lock', 'tap_table_grade', 'tap_table_grade_lock', 'tap_table_video_lock', 'tap_table_video_realtime', 'tap_table_question', 'tap_table_question_lock','realtime', 'realtime_discussion_table', 'tap_table_discussion_lock']
NEWCLOUD_ACADEMIC_ES_INDEX = 'education_data'
TAP_REALTIME_INDEX = 'realtime'
NEWCLOUD_DATACONF = ['subimissions_score', 'score_video', 'score_comment', 'import_score', 'score_answer_problem', 'score_rule_change', 'score_course_struct']
MAIL_TO = ['wangchenkang@xuetangx.com', 'wangxiaoke@xuetangx.com', 'rencan@xuetangx.com', 'zhuhaijun@xuetangx.com']
#MAIL_TO = ['wangchengkang@xuetangx.com']
MAIL_LOGIN = {'user': 'tap_feedback@xuetangx.com', 'password': 'xuetangX123'}
#ES_INDEX = 'tapgo'
with open(CONFIG_FILE) as f:
    document = f.read()
    locals().update(yaml.load(document))

try:
    from private_settings import *
except:
    pass
