#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Q
from utils.routes import route
from utils.tools import var
from utils import mysql_connect
from .base import BaseHandler
import settings
import datetime


@route('/permission/teacher_permission')
class TeacherPermission(BaseHandler):
    """
    教师权限接口
    """
    def get(self):

        #获取page,size,status因为tap层已经有默认值，所以此处不必再设默认值
        page = int(self.get_argument("page"))
        size = int(self.get_argument("size"))
        status = self.get_argument("status")
        host = self.get_argument('host')
        now = datetime.datetime.now()
        
        query = self.es_query(doc_type='teacher_power')\
                .filter('term', host=host)\
                .filter('term', user_id=str(self.user_id)).sort("-start")
        #开课
        if status == "process":
            query = query.filter('range', **{'start': {'lte': now}})\
                    .filter('range', **{'end': {'gt': now}}).sort("-start")
        #结课
        if status == "close":
            query = query.filter('range', **{'end': {'lte': now}}).sort("-end")
        #即将开课
        if status == "unopen":
            query = query.filter('range', **{'start': {'gt': now}}).sort("start")

        query = query.filter(Q('range', **{'group_key': {'lt': settings.COHORT_GROUP_KEY}}) | Q('range', **{'group_key': {'gte': settings.ELECTIVE_GROUP_KEY}}))

        total = self.es_execute(query[:0]).hits.total
        if total%size != 0:
            total_page = total/size + 1
        else:
            total_page = total/size

        if page*size >= total:
            load_more = 0
        else:
            load_more = 1

        results = self.es_execute(query[:total])
        results = results.hits
        
        #得到课程ID列表和每个课程对应group_key list
        powers_dict  = {}
        course_ids = []
        for result in results:
            if result.course_id not in powers_dict:
                course_ids.append(result.course_id)
                powers_dict[result.course_id] = []
            power = result.group_key
            powers_dict[result.course_id].append(power)
        
        #获取指定数量的课程id dict
        powers = {}
        for course_id in course_ids[(page-1)*size:page*size]:
            powers[course_id]=powers_dict[course_id]
        
        course_result = {}
        course_result['powers'] = powers
        course_result['total'] = total
        course_result['total_page'] = total_page
        course_result['load_more'] = load_more
        
        self.success_response({'powers': course_result})


@route('/permission/course_permission')
class CoursePermission(BaseHandler):
    """
    教师是否有某门课的权限
    """
    def get(self):
        status = mysql_connect.MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).course_permission(self.user_id, self.course_id, self.group_key)
        self.success_response({'has_permission': status})

@route('/permission/group_key')
class GroupKey(BaseHandler):
    """
    教师某门课的group_key    
    """
    def get(self):
        result = mysql_connect.MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).course_group_key(self.user_id, self.course_id)
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
