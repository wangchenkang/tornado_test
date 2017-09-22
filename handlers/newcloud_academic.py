#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.web import gen
from utils.routes import route
from .base import BaseHandler
from elasticsearch_dsl import Q
from elasticsearch_dsl import MultiSearch
import settings

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

COURSE_FIELD = ['course_id', 'course_name','active_rate_course', 'study_video_rate_course', 'no_watch_person_course',\
                'enroll_num_course', 'teacher_num_course', 'effort_course', 'score_avg_course']
TEACHER_FIELD = ['user_id', 'course_num_total', 'course_num', 'first_level', 'term_name', 'course_status', 'discussion_total']
STUDENT_FIELD = ['user_id', 'rname', 'binding_uid', 'faculty', 'major', 'cohort', 'entrance_year', 'participate_total_user', 'open_num_user', 'unopen_num_user', 'close_num_user'] 
STUDENT_FORM_HEADER = [u'编号', u'姓名', u'学号', u'院系', u'专业', u'班级', u'入学年份', u'参与课程', u'开课中', u'待开课', u'已结课']
STUDENT_COURSE_FIELD = ['course_status', 'course_id', 'course_name', 'effort_user', 'study_rate_user', 'accomplish_percent_user', 'correct_percent_user', 'grade']
STUDENT_USER_FIELD = ['open_num_user', 'unopen_num_user', 'close_user', 'video_user', 'discussion_num_user', 'accomplish_percent_user', 'correct_percent_user']

class AcademicData(BaseHandler):

    def get_param_data(self, key):
        data = self.get_param(key)
        return param

    def get_total_page(self, total, num):
        if total == 0:
            total_page = 0
        elif total <= num:
            total_page = 1
        else:
            if total % num == 0:
                total_page = total/num
            else:
                total_page = total/num + 1

        return total_page

    def get_course_num(self, buckets):
        unopen_num = 0
        open_num = 0
        close_num = 0
        for bucket in buckets:
            if bucket.key in ('unopen_num', 'unopen'):
                unopen_num = bucket.doc_count
            elif bucket.key in ('open_num', 'open'):
                open_num = bucket.doc_count
            else:
                close_num = bucket.doc_count
        return unopen_num, open_num, close_num

    def format_course_info(self, results):
        data = []
        for result in results:
            course_info = {}
            image_url = course_detail['image_url']
            course_info['course_name'] = result.course_name
            course_info['study_video_rate'] = self.round_data(result.study_video_rate_course)
            course_info['score_avg'] = self.round_data(result.score_avg_course)
            course_info['effort'] = self.round_data(result.effort_course)
            course_info['student_num'] = result.enroll_num_course
            course_info['teacher_num'] = result.teacher_num_course
            course_info['no_watch_person'] = result.no_watch_person_course

            data.append(course_info) 
        return data

    @gen.coroutine
    def add_course_image(self, results):
        for result in results:
            course_detail = self.course_detail(result['course_id'])
            result.update({'image_url': course_detail['image_url']})
        raise gen.Return(results)

    @property
    def course_query(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        service_line = self.get_argument('service_line', 'all')
        course_status = self.get_argument('course_status', 'all')

        query = self.es_query(index = settings.TEST_INDEX, doc_type = 'org_course_info')\
                             .filter('term', org_id = org_id)\
                             .filter('term', term_id = term_id)
        if service_line != 'all':
            query = query.filter('term', service_line = service_line) 
        if course_status != 'all':
            query = query.filter('term', course_status = course_status)
       
        return query
    
    @property
    def teacher_query(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        faculty = self.get_param('faculty')

        query = self.es_query(index = settings.NEWCLOUD_ACADEMICS_ES_INDEX, doc_type = 'teacher_info')\
                             .filter('term', org_id = org_id)\
                             .filter('term', faculty = faculty)
        return query

    def student_query(self, status=False):
        org_id = self.get_param('org_id')
        faculty = self.get_param('faculty')
        major = self.get_param('major')
        cohort = self.get_param('cohort')
        entrance_year = self.get_param('entrance_year')

        if status:
            query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_course_info')\
                                    .filter('term', is_teacher = 0)
        else:
            query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_enroll')

        query = query.filter('term', org_id = org_id)\
                     .filter('term', faculty = faculty)\
                     .filter('term', major = major)\
                     .filter('term', cohort = cohort)\
                     .filter('term', entrance_year = entrance_year)
        
        return query

    def round_data(self, data):
        return round(data, 4)

  
@route('/course/overview')
class CourseOverview(AcademicData):
    """
    新学堂云教务数据课程概况课程列表
    """
    @property 
    def query(self):
        query = self.course_query[:0]
        total = self.es_execute(query).hits.total
        
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

        buckets = aggs.per_course_status.buckets
        unopen_num, open_num, close_num = self.get_course_num(buckets)
        video_total = self.round_data(aggs.video_total.value or 0)
        video_avg = self.round_data(aggs.video_avg.value or 0)
        discussion_total = self.round_data(aggs.discussion_total.value or 0)
        discussion_avg = self.round_data(aggs.discussion_avg.value or 0)
        accomplish_percent = self.round_data(aggs.accomplish_percent.value or 0)
        correct_percent = self.round_data(aggs.correct_percent.value or 0)
        
        data = {}
        data['course_num'] = course_num
        data['unopen_num'] = unopen_num
        data['open_num'] = open_num
        data['close_num'] = close_num
        data['video_total'] = video_total
        data['video_avg'] = video_avg or 0
        data['discussion_total'] = discussion_total
        data['discussion_avg'] = discussion_avg or 0
        data['accomplish_percent'] = accomplish_percent or 0
        data['correct_percent'] = correct_percent or 0
       
        return data

    def get(self):
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
        sort = self.get_argument('sort', 'course_start')
        sort_type = self.get_argument('sort_type', 0)
        sort = '%s' % sort if int(sort_type) else '-%s' % sort

        query = self.course_query
        query = query.source(COURSE_FIELD)\
                     .sort(sort)
        
        return query[:0]

    def get_result(self, query, page, num):
        total = self.es_execute(query).hits.total
        total_page = self.get_total_page(total, num)

        result = self.es_execute(self.query[(page-1)*num: page*num]).hits
        
        return result, total, total_page

    @gen.coroutine
    def get(self):
        page = self.get_argument('page', 1)
        num = self.get_argument('num', 5)
        query = self.query
        
        results, total, total_page = self.get_result(query, page, num)
        data = self.format_course_info(results)
        data = yield self.add_course_image(data)

        self.success_response({'data': data, 'total_page': total_page, 'course_num': total, 'current_page': page})


@route('/teacher/overview')
class TeacherOverview(AcademicData):
    """
    教师概况，教师数据汇总
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        faculty = self.get_param('faculty')
        term_id = self.get_param('term_id')

        query = self.es_query(index = settings.TEST_INDEX, doc_type = 'org_teacher_level_term')\
                                .filter('term', org_id = org_id)\
                                .filter('term', first_level = faculty)

        size = self.es_execute(query[:0]).hits.total
        query.aggs.bucket('teacher_total', 'terms', field = 'user_id', size = size)
        result = self.es_execute(query[:0])
        buckets = result.aggregations.teacher_total.buckets
        teacher_total = len(buckets)
        
        query = self.es_query(index = settings.TEST_INDEX, doc_type = 'org_teacher_level_term_status')\
                                .filter('term', org_id = org_id)\
                                .filter('term', first_level = faculty)\
                                .filter('term', is_creator = 1)
        size = self.es_execute(query[:0]).hits.total
        query.aggs.bucket('teacher_creator_num', 'terms', field = 'user_id', size = size)
        result = self.es_execute(query[:0])
        buckets = result.aggregations.teacher_creator_num.buckets
        teacher_creator_num = len(buckets)


        query = self.es_query(index = settings.TEST_INDEX, doc_type = 'org_teacher_level_term')\
                                    .filter('term', org_id = org_id)\
                                    .filter('term', first_level = faculty)\
                                    .filter('term', term_id = term_id)
        query.aggs.metric('term_teacher_num', 'sum', field = 'user_id')
        result = self.es_execute(query[:0])
        term_teacher_num = result.aggregations.term_teacher_num.value
        
        query = self.es_query(index = settings.TEST_INDEX, doc_type = 'org_teacher_level_term_status')\
                            .filter('term', org_id = org_id)\
                            .filter('term', first_level = faculty)\
                            .filter('term', term_id = term_id)
        query.aggs.metric('discussion_total', 'sum', field = 'discussion_total')
        result = self.es_execute(query[:0])
        discussion_total = result.aggregations.discussion_total.value
        discussion_avg = discussion_total/(term_teacher_num or 1)

        query = self.es_query(index = settings.TEST_INDEX, doc_type = 'org_level_term')\
                                    .filter('term', org_id = org_id)\
                                    .filter('term', first_level = faculty)\
                                    .filter('term', term_id = term_id)\
                                    .source(['participate_course_num'])
        result = self.es_execute(query[:1]).hits
        participate_total = result[0].participate_course_num if result else 0
        participate_avg = term_teacher_num/(participate_total or 1)

        data = {}
        data['teacher_total'] = teacher_total
        data['creator_num'] = teacher_creator_num
        data['discussion_total'] = discussion_total
        data['discussion_avg'] = discussion_avg
        data['participate_total'] = participate_total
        data['participate_avg'] = participate_avg

        return data
    
    def get_result(self, query, term_id):
        pass
    def get(self):
        data = self.query
        self.success_response({'data': data})


@route('/teacher/list')
class TeacherList(AcademicData):
    """
    教师概况，教师列表
    """
    @property
    def query(self):
        org_id = self.get_param('org_id')
        faculty = self.get_param('faculty')
        term_id = self.get_param('term_id')
        page = self.get_argument('page', 1)
        num = self.get_argument('num', 9)
        sort = self.get_argument('sort', 'course_num_total')
        sort_type = self.get_argument('sort_type', 1)
        sort = sort if int(sort_type) else '-%s' % sort

        if sort in ('course_num_total', '-course_num_total'):
            es_type1 = 'org_teacher_level_term'
            es_type2 = 'org_teacher_level_term_status'
        else:
            es_type1 = 'org_teacher_level_term_status'
            es_type2 = 'org_teacher_level_term'

        query = self.es_query(index = settings.TEST_INDEX, doc_type = es_type1)\
                    .filter('term', org_id = org_id)\
                    .filter('term', first_level = faculty)\
                    .filter('term', term_id = term_id)\
                    .source(TEACHER_FIELD)\
                    .sort(sort)
        size = self.es_execute(query[:0]).hits.total
        if es_type1 == 'org_teacher_level_term_status':
            results = self.get_discussion_total(query, size)
        else:
            results = self.es_execute(query[(page-1)*num:page*num]).hits
            results = [result.to_dict() for result in results]

        user_ids = [result['user_id'] for result in results]
        size = len(user_ids)
        total_page = self.get_total_page(size, num)
        query = self.es_query(index = settings.TEST_INDEX, doc_type = es_type2)\
                    .filter('term', org_id = org_id)\
                    .filter('term', first_level = faculty)\
                    .filter('term', term_id = term_id)\
                    .filter('terms', user_id = user_ids)\
                    .source(TEACHER_FIELD) 
        if es_type2 == 'org_teacher_level_term_status':            
            results_2 = self.get_discussion_total(query, user_ids)
        else:
            results_2 = self.es_execute(query[:size]).hits
            results_2 = [result.to_dict() for result in results_2]
        
        if len(results) >= len(results_2):
            results = self.add2result(results, results_2)
        else:
            results = self.add2result(results_2, results)

        return results, total_page, page

    def get_discussion_total(self, query, user_ids):
        query.aggs.bucket('user_ids', 'terms', field = 'user_id', size = len(user_ids))\
                  .metric('discussion_total', 'sum', field = 'discussion_total')
        results_ = self.es_execute(query[:len(user_ids)])
        results_2 = [result.to_dict() for result in results_.hits]
        for result in results_2:
            result['discussion_total'] = 0
            for bucket in results_.aggregations.user_ids.buckets:
                if result['user_id'] == bucket.key:
                    result['discussion_total'] = bucket.discussion_total.value

        return results_2

    def add2result(self, result_1, result_2):
        for item in result_1:
            item['open_num'] = 0
            item['unopen_num'] = 0
            item['close_num'] = 0
            item['discussion_total'] = 0
            for data in result_2:
                if item['user_id'] == data['user_id']:
                    item.update(data)
        return result_1

    def get_result(self):
        pass

    def get(self):
        query, total_page, page = self.query
        self.success_response({'data': query, 'total_page': total_page, 'current_page': page})

@route('/student/overview')
class StudentOverview(AcademicData):
    """
    学生概况，学生
    """
    @property
    def query(self):
        query = self.student_query()
        query.aggs.metric('enroll_total', 'sum', field = 'participate_total_user')
        query.aggs.metric('enroll_avg', 'avg', field = 'participate_total_user')
        query.aggs.metric('discussion_total', 'sum', field = 'discussion_num_user')
        query.aggs.metric('discussion_avg', 'avg', field = 'discussion_num_user')
        query.aggs.metric('accomplish_avg', 'avg', field = 'accomplish_percent_user')
        query.aggs.metric('correct_avg', 'avg', field = 'correct_percent_user')
        
        return query[:0]

    def round_data(self, data):
        return round(data, 4)

    def get_result(self, query):
        result = self.es_execute(query)
        student_num = result.hits.total
        aggs = result.aggregations
        enroll_total = int(aggs.enroll_total.value or 0)
        enroll_avg = self.round_data(aggs.enroll_avg.value or 0)
        discussion_total = int(aggs.discussion_total.value or 0)
        discussion_avg = self.round_data(aggs.discussion_avg.value or 0) 
        accomplish_percent = self.round_data(aggs.accomplish_avg.value or 0)
        correct_percent = self.round_data(aggs.correct_avg.value or 0)
        
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
        result = self.get_result(query)

        self.success_response({'data': result})


@route('/student/list')
class StudentList(AcademicData):
    """
    学生概况，学生首页列表
    """
    @property
    def query(self):
        query = self.student_query()
        query = query.source(STUDENT_FIELD)\
             .sort('user_id')
        
        return query

    def get_result(self, query, page, num, student_keyword):
        if student_keyword != 'all':
            query = query.filter(Q('bool', should=[Q('wildcard', rname = '%s*' % student_keyword)\
                                                 | Q('wildcard', binding_uid = '%s*' % student_keyword)
                                                   ]))

        total = self.es_execute(query[:0]).hits.total
        total_page = self.get_total_page(total, num)
        
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
        header = dict(zip(STUDENT_FIELD, STUDENT_FORM_HEADER))

        self.success_response({'data': result, 'total_page': total_page, 'current_page': page, 'header': header})


@route('/student/detail/overview')
class StudentDetailOverview(AcademicData):
    """
    学生概况，学生详情页汇总信息
    """
    @property
    def query(self):
        user_id = self.get_param('user_id')
        query = self.student_query()
        query = query.filter('term', user_id = user_id)
        query.aggs.metric('study_video_avg', 'avg', field = 'study_rate_user')
        query.aggs.metric('discussion_num_avg', 'avg', field = 'discussion_num_user')
        query = query.source(STUDENT_USER_FIELD)
        
        return query

    def get_result(self, query):
        results = self.es_execute(query[:1])
        aggs = results.aggregations
        total = results.hits.total
        results = [result.to_dict() for result in results.hits]
        result = results[0]
        result['open_num'] = result.pop('open_num_user')
        result['unopen_num'] = result.pop('unopen_num_user')
        result['close_num'] = result.pop('close_user')
        result['accomplish_percent'] = result.pop('accomplish_percent_user')
        result['discussion_total'] = result.pop('discussion_num_user')
        result['correct_percent'] = result.pop('correct_percent_user')
        result['discussion_avg'] = self.round_data(aggs.discussion_num_avg.value or 0)
        result['course_total'] = total
        
        return result

    def get(self):
        query = self.query
        result = self.get_result(query)

        self.success_response({'data': result})


@route('/student/detail/courses')
class StudentDetailCourse(AcademicData):
    """
    学生概况，学生详情页课程列表信息
    """
    @property
    def query(self):
        user_id = self.get_param('user_id')
        query = self.student_query(status=True)
        query = query.filter('term', user_id = user_id)\
                     .source(STUDENT_COURSE_FIELD) 
        
        return query
    
    @gen.coroutine
    def get_result(self, query, page, num):
        course_total = self.es_execute(query[:0]).hits.total 
        total_page = self.get_total_page(course_total, num)
        
        results = self.es_execute(query[(page-1)*num:page*num]).hits
        results = [result.to_dict() for result in results]
        data = yield self.add_course_image(results)
        for item in data:
            item['study_rate'] = item.pop('study_rate_user')
            item['correct_percent'] = item.pop('correct_percent_user')
            item['accomplish_percent'] = item.pop('accomplish_percent_user')
            item['effort'] = item.pop('effort_user')

        raise gen.Return((data, total_page))

    @gen.coroutine
    def get(self):
        page = self.get_argument('page', 1)
        num = self.get_argument('num', 6)
        query = self.query
        result, total_page = yield self.get_result(query, page, num)

        self.success_response({'data': result, 'total_page': total_page, 'current_page': page})


