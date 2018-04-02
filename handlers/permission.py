#! /usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from elasticsearch_dsl import Q

import settings
from utils.tools import var
from utils.routes import route
from utils import mysql_connect
from .base import BaseHandler


@route('/permission/course_permission')
class CoursePermission(BaseHandler):
    """
    教师是否有某门课的权限
    """
    def get(self):
        status = mysql_connect.MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).course_permission(self.user_id, self.course_id, self.group_key, self.host)
        self.success_response({'has_permission': status})

@route('/permission/group_key')
class GroupKey(BaseHandler):
    """
    教师某门课的group_key    
    """
    def get(self):
        result = mysql_connect.MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).course_group_key(self.user_id, self.course_id, self.host)
        if not result:
            self.success_response({'data': []})
        self.success_response({'data': result})

@route('/permission/cohort_group_key')
class CohortGroupKey(BaseHandler):
    """
    教师在某个平台上某门课的cohort group_key
    """
    def get(self):
        result = mysql_connect.MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).get_cohort_group_keys(self.host, self.user_id, self.course_id)
        self.success_response({'data': result})
