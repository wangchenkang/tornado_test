#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Q
from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler
import settings
import datetime

Log.create('education')


@route('/education/course_overview')
class EducationCourseOverview(BaseHandler):
    """
    教务数据课程概览关键参数
    """
    def get(self):

        #TODO
        host = self.host
        user_id = self.user_id
        course_type = self.course_type
        course_status = self.course_status
        
        #TODO doc_type
        query = self.es_query(doc_type='xxxxx')\
                .filter('term', host=host)\
                .filter('term', user_id=str(self.user_id))\
                .filter('term', course_type=course_type)\
                .filter('term', course_status=course_status)

        result = self.es_execute(query[:1]).hits[0]
            
        course_result = {}
        course_result['course_num'] = result.course_num
        course_result['active_num'] = result.active_num
        course_result['video_length'] = result.video_length
        course_result['enrollment_num'] = result.enrollment_num
        
        self.success_response({'data': course_result})


@route('/education/course_study')
class EducationCourseStudy(BaseHandler):
    """
    教务数据气泡图数据
    """
    def get(self):

        #TODO host 学校标识需要主站传给我
        host = self.host
        user_id = self.user_id
        course_type = self.course_type
        course_status = self.course_status

        query = self.es_query(doc_type='teacher_power')\
                .filter('term', user_id=str(self.user_id)) \
                .filter('term', host=host)\
                .filter('term', course_type=course_type)\
                .filter('term', course_status=course_status)

        query_result = self.es_execute(query)
        result = []
        if course_status == '开课中':
            result = [{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'active_rate': hits.active_rate, 'course_name': hits.course_name } for hits in query_result.hits]
        if course_status == '已结课':
            result = [{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'pass_rate': hits.pass_rate, 'course_name': hits.course_name } for hits in query_result.hits]
        if course_status == '即将开课':
            result = [{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'course_name': hits.course_name } for hits in query_result.hits]

        self.success_response({'data': result})

@route('/education/course_name_search')
class EducationCourseNameSearch(BaseHandler):
    """
    教务数据课程概览课程名称搜索数据
    """
    def get(self):

        #TODO
        query = self.es_query(doc_type='teacher_power')\
                    .filter('term', user_id=str(self.user_id))\
                    .filter('term', host=self.host)\
                    .filter('term', course_type=self.course_type)\
                    .filter('term', course_status=self.course_status)\
                    .filter(Q('bool', should=[Q('wildcard', course_name='*%s' % self.course_name)]))

        results = self.es_execute(query)
        if results.hits:
            #TODO
            data = [{''} for hits in results.hits]
            self.success_response({'data': data[0]})
        self.success_response({'data': {}})

@route('/education/course_download')
class EducationCourseDownload(BaseHandler):
    """
    教务数据课程下载数据
    """
    def get(self):
        #TODO
        fields = self.fields.split(',') if self.fields else []
        query = self.es_query(doc_type='xxxx')\
                    .filter('term', user_id=self.user_id)\
                    .filter('term', host=self.host)\
                    .filter('term', course_type=self.course_type)\
                    .filter('term', course_status=self.course_status)
        query = query.source(fields)
        query_result = self.es_execute(query)
        if query_result.hits:
            #TODO
            pass
        self.success_response({'data': {}})

