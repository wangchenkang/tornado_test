#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.web import gen
from utils.routes import route
from .base import BaseHandler
from elasticsearch_dsl import Q
import settings

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

COURSE_FIELD = ['active_rate_course', 'study_video_rate_course', 'no_watch_person_course',\
                'enroll_num', 'teacher_num_course', 'effort_course', 'score_avg_course']
TEACHER_FIELD = ['']
STUDENT_FIELD = [''] 

class AcademicData(BaseHandler):

    def get_param_data(self, key):
        data = self.get_param(key)
        return param

@route('/course/overview')
class CourseOverview(AcademicData):
    """
    新学堂云教务数据课程概况课程列表
    """
    @property 
    def query(self):
        org_id = self.get_param('org_id')
        #TODO
        term = self.get_param('term_id')
        service_line = self.get_param('service_line')
        course_status = self.get_param('course_status')
       
        query = self.es_query(index = settings.NEWCLOUD_ACADEMICS_ES_INDEX, doc_type = 'course_info')\
                             .filter('term', org_id = org_id)\
                             .filter('term', term = term)
        total = self.es_execute(query[:0]).hits.total
        if service_line != 'all':
            query = query.filter('term', service_line = service_line) 
        if course_status != 'all':
            query = query.filter('term', course_status = course_status)
        query.aggs.bucket('per_course_status', 'terms', field='course_status', size=total or 1)
        query.aggs.metric('video_total', 'sum', field='video_total_course')
        query.aggs.metric('video_avg', 'avg', field='video_total_course')
        query.aggs.metric('discussion_total', 'sum', field='discussion_total_course')
        query.aggs.metric('discussion_avg', 'avg', field='discussion_total_course')
        query.aggs.metric('accomplish_percent', 'avg', field='accomplish_percent_course')
        query.aggs.metric('correct_percent', 'avg', field='correct_percent_course')
           
        return query[:0]

    def get_result(self, query):
        result = self.es_execute(query)
        aggs = result.aggregations    
        course_num = result.hits.total
        unopen_num = 0
        open_num = 0
        close_num = 0
        for bucket in aggs.per_course_status.buckets:
            if bucket.key == 'unopen_num':
                unopen_num = bucket.doc_count
            elif bucket.key == 'open_num':
                open_num = bucket.doc_count
            else:
                close_num = bucket.doc_count
        video_total = aggs.video_total.value
        video_avg = aggs.video_avg.value
        discussion_total = aggs.discussion_total.value
        discussion_avg = aggs.discussion_avg.value
        accomplish_percent = aggs.accomplish_percent.value
        correct_percent = aggs.correct_percent.value
        data = {}
        data['course_num'] = course_num or 0
        data['unopen_num'] = unopen_num or 0
        data['open_num'] = open_num or 0
        data['close_num'] = close_num or 0
        data['video_total'] = video_total or 0
        data['video_avg'] = video_avg or 0
        data['discussion_total'] = discussion_total or 0
        data['discussion_avg'] = discussion_avg or 0
        data['accomplish_percent'] = accomplish_percent or 0
        data['correct_percent'] = correct_percent or 0
        return data

    @gen.coroutine
    def get(self):
        #TODO
        query = self.query
        result = self.get_result(query)
        self.success_response({'data': result})


@route('/course/list')
class CourseList(AcademicData):
    """
    课程概况，课程列表
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        #TODO 
        term = self.get_param('term_id')
        service_line = self.get_param('service_line')
        course_status = self.get_param('course_status').decode('utf-8')
        sort = self.get_argument('sort', 'start')
        sort_type = self.get_argument('sort_type', 0)
        sort = '%s' % sort if sort_type else '-%s' % sort
        query = self.es_query(index = settings.NEWCLOUD_ACADEMICS_ES_INDEX, doc_type = 'course_info')\
                            .filter('term', orgid_or_host = org_id)\
                            .filter('term', term = term)\
                            .source(COURSE_FIELD)\
                            .sort(sort)

        if service_line != 'all':
            query = query.filter('term', service_line = service_line)
        if course_status != 'all':
            query = query.filter('term', course_status = course_status)
        
        return query

    def get_result(self, query, page, num):
        total = self.es_execute(self.query[:0]).hits.total
        if total == 0:
            total_page = 0
        elif total <= num:
            total_page = 1
        else:
            if total % page == 0:
                total_page = total/page
            else:
                total_page = total/page + total%page
        result = self.es_execute(self.query[(page-1)*num: page*num]).hits
        return result, total, total_page

    @gen.coroutine
    def get(self):
        page = self.get_argument('page', 1)
        num = self.get_argument('num', 5)
        query = self.query
        results, total, total_page = self.get_result(query, page, num)
        data = []
        for result in results:
            course_info = result.to_dict()
            course_detail = self.course_detail(result.course_id)
            image_url = '' if not course_detail else course_detail['image_url']
            course_name = '' if not course_detail else course_detail['display_name']
            course_info['image_url'] = image_url
            course_info['course_name'] = course_name
            data.append(course_info)
        self.success_response({'data': data, 'total_page': total_page, 'course_num': total, 'current_page': page})


@route('/teacher/overview')
class TeacherOverview(AcademicData):
    """
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        faculty = self.get_param('faculty')

        query = self.es_query(index = settings.NEWCLOUD_ACADEMICS_ES_INDEX, doc_type='xxx')\
                            .filter('term', org_id = org_id)\
                            .filter('term', faculty = faculty)\
        return query
    
    def get_result(self, term):
        query = self.query
        teacher_total = self.es_execute(query[:0]).hits.total
        query.aggs.bucket('create_num', 'terms', field='can_create', size=teacher_total or 1)
        query = query.filter('term', term=term)
        query.aggs.metric('discussion_total', 'sum', field='discussion_num')
        query.aggs.metric('discussion_avg', 'avg', field='discussion_num')
        query.aggs.metric('discussion_course_total', 'cardinality', field='')
        pass

    def get(self):  
        pass


@route('/teacher/list')
class TeacherList(AcademicData):
    """
    教师概况，教师列表
    """
    @property
    def query(self):
        
        pass

    def get_result(self):
        pass

    def get(self):
        pass

@route('/student/overview')
class StudentOverview(AcademicData):
    """
    学生概况，学生
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        faculty = self.get_param('faculty')
        major = self.get_param('major')
        cohort = self.get_param('cohort')
        entrance_year = self.get_param('entrance_year')

        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_enroll')\
                                .filter('term', org_id = org_id)\
                                .filter('term', faculty = faculty)\
                                .filter('term', major = major)\
                                .filter('term', cohort = cohort)\
                                .filter('term', entrance_year = entrance_year)
        query.aggs.metric('enroll_total', 'sum', field = 'participate_total_user')
        query.aggs.metric('enroll_avg', 'avg', field = 'participate_total_user')
        query.aggs.metric('discussion_total', 'sum', field = 'discussion_num')
        query.aggs.metric('discussion_avg', 'avg', field = 'discussion_num')
        query.aggs.metric('accomplish_avg', 'avg', field = 'accomplish_percent')
        query.aggs.metric('correct_avg', 'avg', field = 'correct_percent')

        return query

    def get_result(self, query):
        result = self.es_execute(query[:0])
        student_num = result.hits.total
        aggs = result.aggregations
        enroll_total = aggs.enroll_total.value
        enroll_avg = aggs.enroll_avg.value
        discussion_total = aggs.discussion_total
        discussion_avg = aggs.discussion_avg
        accomplish_percent = aggs.accomplish_avg
        correct_percent = aggs.correct_avg
        
        data = {}
        data['student_num'] = student_num
        data['enroll_total'] = enroll_total
        data['enroll_avg'] = enroll_avg
        data['discussion_total'] = discussion_total
        data['discussion_avg'] = discussion_avg
        data['accomplish_percent'] = accomplish_percent
        data['correct_percent'] = correct_percent

        return data

    def get(self):
        query = self.query
        result = self.get_result()
        self.success_response({'data': result})


@route('/student/list')
class StudentList(AcademicData):
    """
    学生概况，学生首页列表
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        faculty = self.get_param('faculty')
        major = self.get_param('major')
        cohort = self.get_param('cohort')
        entrance_year = self.get_param('entrance_year')

        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_enroll')\
                                .filter('term', org_id = org_id)\
                                .filter('term', faculty = faculty)\
                                .filter('term', major = major)\
                                .filter('term', cohort = cohort)\
                                .source(STUDENT_FIELD)\
                                .sort('user_id')
        return query

    def get_result(self,query, page, num, student_keyword):
        if student_keyword != 'all':
            query = query.filter(Q('bool', should=[Q('wildcard', rname = '%s*' % student_keyword)\
                                                 | Q('wildcard', binding_uid = '%s*' % student_keyword)
                                                   ]))
        else:
            pass

        total = self.es_execute(query[:0]).hits.total
        
        if total == 0:
            total_page = 0
        elif total <= num:
            total_page =1
        else:
            if total%num == 0:
                total_page = total/num
            else:
                total_page = total/num + 1

        results = self.es_execute(query[(page-1)*num: page*num]).hits
        #TODO
        data = [result.to_dict() for result in results]
        return data, total_page

    def get(self):
        page = self.get_argument('page', 1)
        num = self.get_argument('num', 10)
        student_keyword = self.get_argument('student_keyword', 'all')

        query = self.query
        result, total_page = self.get_result(query, page, num, student_keyword)
        self.success_response({'data': result, 'total_page': total_page, 'current_page': page})


@route('/student/detail/overview')
class StudentDetailOverview(AcademicData):
    """
    学生概况，学生详情页汇总信息
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        term = self.get_param('term')
        faculty = self.get_param('faculty')
        major = self.get_param('major')
        cohort = self.get_param('cohort')
        entrance_year = self.get_param('entrance_year')
        user_id = self.param('user_id')

        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_info')\
                                .filter('term', org_id = org_id)\
                                .filter('term', term = term)\
                                .filter('term', faculty = faculty)\
                                .filter('term', major = major)\
                                .filter('term', cohort = cohort)\
                                .filter('term', entrance_year = entrance_year)

        query_user = query.filter('term', user_id = user_id)
        query_user.aggs.metric('course_total', 'sum', field = 'course_id')
        query_user.aggs.bucket('per_course_status', 'terms', field = 'course_status', size = 10000)
        query_user.aggs.metric('study_video_total', 'sum', field = 'study_video_total_user')
        query_user.aggs.metric('discussion_total', 'sum', field = 'discussion_total_user')
        query_user.aggs.metric('accomplish_percent', 'avg', field = 'accomplish_percent_user')
        query_user.aggs.metric('correct_percent', 'avg', field = 'correct_percent_user')

        query.aggs.bucket('user_total', 'terms', field = 'user_id', size = 100000)

        return query_user, query

    def get_result(self, query_user, query):
        result_user = self.es_execute(query_user)
        result = self.es_execute(query)

        aggs_user = result_user.aggregations
        aggs = result.aggregations

        course_total = aggs_user.course_total.value
        open_num = 0
        unopen_num = 0
        close_num = 0
        for bucket in aggs_user.per_course_status.buckets:
            if bucket.key == 'open_num':
                open_num = bucket.doc_count
            if bucket.key == 'unopen_num':
                unopen_num = bucket.doc_count
            if bucket.key == 'close_num':
                close_num = bucket.doc_count
        buckets = aggs.user_total.buckets
        student_num = len(buckets)
        study_video_total = aggs_user.study_video_total.value
        study_video_avg = round(study_video_total/student_num or 1, 2)
        discussion_total = aggs_user.discussion_total.value
        discussion_avg = round(discussion_total/student_num or 1, 2)
        accomplish_percent = aggs_user.accomplish_percent
        correct_percent = aggs_user.correct_percent

        data = {}
        data['course_total'] = course_total
        data['open_num'] = open_num
        data['unopen_num'] = unopen_num
        data['close_num'] = close_num
        data['study_video_avg'] = study_video_avg
        data['study_video_total'] = study_video_total
        data['discussion_total'] = discussion_total
        data['discussion_avg'] = discussion_avg
        data['accomplish_percent'] = accomplish_percent
        data['correct_percent'] = correct_percent

        return data

    def get(self):
        query_user, query = self.query
        result = self.get_result(query_user, query)
        self.success_response({'data': result})


@route('/student/detail/courses')
    """
    学生概况，学生详情页课程列表信息
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        term = self.get_param('term')
        faculty = self.get_param('faculty')
        major = self.get_param('major')
        cohort = self.get_param('cohort')
        entrance_year = self.get_param('entrance_year')
        user_id = self.param('user_id')

        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_course_info')\
                                .filter('term', org_id = org_id)\
                                .filter('term', term = term)\
                                .filter('term', faculty = faculty)\
                                .filter('term', major = major)\
                                .filter('term', cohort = cohort)\
                                .filter('term', entrance_year = entrance_year)\
                                .filter('term', user_id = user_id)

       
       return query

    def get_result(self, query, page, num):
        course_total = self.es_execute(query[:0]).hits.total 
        if course_total == 0:
            total_page = 0
        elif course_total <= num:
            total_page = 1
        else:
            if course_total%num == 0:
                total_page = course_total/num
            else:
                total_page = course_total/num + 1
        
        results = self.es_execute(query[(page-1)*num:page*num]).hits
        for result in results:
            course_id = result.course_id
            course_detail = yield self.course_detail('get', 'courses/detail', {'course_id': course_id})
            image_url = course_detail['image_url']
            course_info = result.to_dict()
            course_info

        pass

    def get(self):
        page = self.get_argument('page', 1)
        num = self.get_argument('num', 6)
        query = self.query
        result = self.get_result(query, page, num)
        self.success_response({'data': result})


