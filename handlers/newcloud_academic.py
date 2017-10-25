#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.web import gen
from utils.routes import route
from .base import BaseHandler
from elasticsearch_dsl import Q
from utils.mysql_connect import MysqlConnect
from utils.service import CourseService
import settings

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

COURSE_FIELD = ['course_id', 'course_name','active_rate_course', 'study_video_rate_course', 'no_watch_person_course',\
                'enroll_num_course', 'teacher_num_course', 'effort_course', 'score_avg_course', 'course_status', 'service_line']
TEACHER_FIELD = ['user_id', 'course_num_total', 'course_num', 'first_level', 'term_name', 'course_status', 'discussion_total']
STUDENT_FIELD = ['rname', 'binding_uid', 'faculty', 'major', 'cohort', 'entrance_year', 'participate_total_user', 'open_num_user', 'unopen_num_user', 'close_num_user'] 
STUDENT_FORM_HEADER = [u'姓名', u'学号', u'院系', u'专业', u'班级', u'入学年份', u'参与课程', u'开课中', u'待开课', u'已结课']
STUDENT_COURSE_FIELD = ['course_status', 'course_id', 'course_name', 'study_video_len_user', 'study_rate_user', 'accomplish_percent_user', 'correct_percent_user', 'grade', 'start', 'end']
STUDENT_USER_FIELD = ['open_num_user', 'unopen_num_user', 'close_num_user', 'study_video_user', 'discussion_num_user', 'accomplish_percent_user', 'participate_total_user', 'correct_percent_user']

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
            course_info['course_name'] = result.course_name
            course_info['study_video_rate'] = self.round_data_4(result.study_video_rate_course)
            course_info['score_avg'] = self.round_data_2(result.score_avg_course)
            course_info['effort'] = self.round_data_2(result.effort_course)
            course_info['student_num'] = result.enroll_num_course
            course_info['teacher_num'] = result.teacher_num_course
            course_info['no_watch_person'] = result.no_watch_person_course
            course_info['course_id'] = result.course_id
            course_info['active_rate'] = self.round_data_4(result.active_rate_course)
            course_info['course_status'] = result.course_status
            course_info['service_line'] = result.service_line
            data.append(course_info) 
        return data

    @gen.coroutine
    def get_course_image(self, result):
        course_detail = yield self.course_detail(result['course_id'])
        
        raise gen.Return(course_detail['image_url'])

    @property
    def course_query(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        service_line = self.get_argument('service_line', None)
        course_status = self.get_argument('course_status', None)

        service_line = 'all' if not service_line else service_line
        course_status = 'all' if not course_status else course_status

        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_course_info')\
                             .filter('term', org_id = org_id)\
                             .filter('term', term_id = term_id)
        if service_line != 'all':
            query = query.filter('term', service_line = service_line) 
        if course_status != 'all':
            query = query.filter('term', course_status = course_status)
       
        return query
    
    def student_query(self, status=0):
        org_id = self.get_param('org_id')
        faculty = self.get_argument('faculty', None)
        major = self.get_argument('major', None)
        cohort = self.get_argument('cohort', None)
        entrance_year = self.get_argument('entrance_year', None)

        faculty = 'all' if not faculty else faculty
        major = 'all' if not major else major
        cohort = 'all' if not cohort else cohort
        entrance_year = 'all' if not entrance_year else entrance_year

        if status == 1:
            query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_course_info')\
                                    .filter('term', is_teacher = 0)
        elif status == 2:
            query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_term_info')
        else:
            query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'student_enroll')

        query = query.filter('term', org_id = org_id)
        
        if faculty != 'all':
            query = query.filter('term', faculty = faculty)
        if major != 'all': 
            query = query.filter('term', major = major)
        if cohort != 'all':
            query = query.filter('term', cohort = cohort)
        if entrance_year != 'all':
            query = query.filter('term', entrance_year = entrance_year)
        
        return query

    def round_data_4(self, data):
        data = float(data)

        return round(data, 4)

    def round_data_2(self, data):
        data = float(data)

        return round(data, 2)

  
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
        video_total = self.round_data_2(aggs.video_total.value or 0)
        video_avg = self.round_data_2(aggs.video_avg.value or 0)
        discussion_total = self.round_data_2(aggs.discussion_total.value or 0)
        discussion_avg = self.round_data_2(aggs.discussion_avg.value or 0)
        accomplish_percent = self.round_data_4(aggs.accomplish_percent.value or 0)
        correct_percent = self.round_data_4(aggs.correct_percent.value or 0)
        
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
        page = int(self.get_argument('page', 1))
        num = int(self.get_argument('num', 5))
        query = self.query
        
        results, total, total_page = self.get_result(query, page, num)
        datas = self.format_course_info(results)
        for data in datas:
            data['image_url'] = yield self.get_course_image(data)
        self.success_response({'data': datas, 'total_page': total_page, 'course_num': total, 'current_page': page})


@route('/teacher/overview')
class TeacherOverview(AcademicData):
    """
    教师概况，教师数据汇总
    """
    def get_total_term_num(self, org_id, faculty, term_id):
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_teacher_level_term')\
                                .filter('term', org_id = org_id)
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)

        size = self.es_execute(query[:0]).hits.total
        query.aggs.bucket('teacher_total', 'terms', field = 'user_id', size = size or 1)
        result = self.es_execute(query[:0])
        buckets = result.aggregations.teacher_total.buckets
        teacher_total = len(buckets)
        
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_teacher_level_term')\
                                    .filter('term', org_id = org_id)\
                                    .filter('term', term_id = term_id)
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)

        size = self.es_execute(query[:0]).hits.total
        query.aggs.bucket('term_teacher_num', 'terms', field = 'user_id', size = size or 1)
        result = self.es_execute(query[:0])
        term_teacher_num = result.aggregations.term_teacher_num.buckets
        term_teacher_num = len(term_teacher_num)

        return teacher_total, term_teacher_num

    def get_creator_discussion_total(self, org_id, faculty, term_id):
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_teacher_level_term_status')\
                                .filter('term', org_id = org_id)\
                                .filter('term', is_creator = 1)
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)

        size = self.es_execute(query[:0]).hits.total
        query.aggs.bucket('teacher_creator_num', 'terms', field = 'user_id', size = size or 1)
        result = self.es_execute(query[:0])
        buckets = result.aggregations.teacher_creator_num.buckets
        teacher_creator_num = len(buckets)
        
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_teacher_level_term_status')\
                            .filter('term', org_id = org_id)\
                            .filter('term', term_id = term_id)
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)

        query.aggs.metric('discussion_total', 'sum', field = 'discussion_total')
        result = self.es_execute(query[:0])
        discussion_total = result.aggregations.discussion_total.value
        
        return teacher_creator_num, discussion_total

    def get_participate_total(self, org_id, faculty, term_id):
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_level_term')\
                                    .filter('term', org_id = org_id)\
                                    .filter('term', term_id = term_id)\
                                    .source(['participate_course_num'])
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)
        result = self.es_execute(query[:1000]).hits
        paticipate_num = []
        for item in result:
            paticipate_num.append(item.participate_course_num)
        participate_total = sum(paticipate_num)

        return participate_total

    @property
    def data(self):
        org_id = self.get_param('org_id')
        faculty = self.get_argument('faculty', None)
        term_id = self.get_param('term_id')
       
        faculty = 'all' if not faculty else faculty

        teacher_total, term_teacher_num = self.get_total_term_num(org_id, faculty, term_id)
        teacher_creator_num, discussion_total = self.get_creator_discussion_total(org_id, faculty, term_id) 
        discussion_avg = self.round_data_2(discussion_total/(term_teacher_num or 1))
        participate_total = self.get_participate_total(org_id, faculty, term_id)
        participate_avg = self.round_data_2(float(term_teacher_num)/(participate_total or 1))
        
        data = {}
        data['teacher_total'] = teacher_total
        data['creator_num'] = teacher_creator_num
        data['discussion_total'] = discussion_total
        data['discussion_avg'] = discussion_avg
        data['participate_total'] = participate_total
        data['participate_avg'] = participate_avg
        data['teacher_term'] = term_teacher_num

        return data
    
    def get(self):
        data = self.data
        self.success_response({'data': data})


@route('/teacher/list')
class TeacherList(AcademicData):
    """
    教师概况，教师列表
    """
    @property
    def data(self):
        org_id = self.get_param('org_id')
        faculty = self.get_argument('faculty', None)
        term_id = self.get_param('term_id')
        page = int(self.get_argument('page', 1))
        num = int(self.get_argument('num', 9))
        sort = self.get_argument('sort', 'course_num_total')
        sort_type = self.get_argument('sort_type', 1)
        sort = sort if int(sort_type) else '-%s' % sort

        faculty = 'all' if not faculty else faculty

        if sort in ('course_num_total', '-course_num_total'):
            es_type1 = 'org_teacher_level_term'
            es_type2 = 'org_teacher_level_term_status'
        else:
            es_type1 = 'org_teacher_level_term_status'
            es_type2 = 'org_teacher_level_term'

        #status
        results, size = self.get_result(org_id, faculty, term_id, es_type1, sort, page, num)
        user_ids = [result['user_id'] for result in results]
        #size = len(user_ids)
        total_page = self.get_total_page(size, num)
        results_status = self.get_result_status(org_id, faculty, term_id, user_ids, es_type2, num)
        results = self.add2result(results, results_status)

        return results, total_page, page

    def get_result(self, org_id, faculty, term_id, es_type1, sort, page, num):
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = es_type1)\
                    .filter('term', org_id = org_id)\
                    .filter('term', term_id = term_id)\
                    .source(TEACHER_FIELD)\
                    .sort(sort, 'user_id')
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)
        size = self.es_execute(query[:0]).hits.total
        if es_type1 == 'org_teacher_level_term_status':
            query.aggs.bucket('user_ids', 'terms', field = 'user_id', size = size)
            aggs = self.es_execute(query).aggregations
            buckets = aggs.user_ids.buckets
            size = len(buckets)
            results = self.get_discussion_total(query, num)
        else:
            results = self.es_execute(query[(page-1)*num:page*num]).hits
            results = [result.to_dict() for result in results]
        
        return results, size

    def get_result_status(self, org_id, faculty, term_id, user_ids, es_type2, size):
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = es_type2)\
                    .filter('term', org_id = org_id)\
                    .filter('term', term_id = term_id)\
                    .filter('terms', user_id = user_ids)\
                    .source(TEACHER_FIELD) 
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)

        if es_type2 == 'org_teacher_level_term_status':            
            results = self.get_discussion_total(query, size)
        else:
            results = self.es_execute(query[:size]).hits
            results = [result.to_dict() for result in results]
        
        return results

    def get_discussion_total(self, query, size):
        query.aggs.bucket('user_ids', 'terms', field = 'user_id', size = size or 1)\
                  .metric('discussion_total', 'sum', field = 'discussion_total')
        results_ = self.es_execute(query[:size])
        results_2 = [result.to_dict() for result in results_.hits]
        for result in results_2:
            result['discussion_total'] = 0
            for bucket in results_.aggregations.user_ids.buckets:
                if result['user_id'] == bucket.key:
                    result['discussion_total'] = bucket.discussion_total.value

        return results_2

    def add2result(self, result, result_status):
        for item in result:
            item['open_num'] = 0
            item['unopen_num'] = 0
            item['close_num'] = 0
            item['discussion_total'] = 0
            item['faculty'] = item.pop('first_level')
            for data in result_status:
                if item['user_id'] == data['user_id']:
                    item.update(data)
                    item.pop('first_level')
            item['course_total'] = item.pop('course_num_total')
            item.pop('course_status') if 'course_status' in item else item
            rname, image_url = self.get_rname_image(item['user_id'])
            item['rname'] = rname
            item['image_url'] = image_url

        return result

    def get_rname_image(self, user_id):
        image_url, rname = MysqlConnect(settings.MYSQL_PARAMS['auth_userprofile']).get_rname_image(user_id)
        return rname, image_url

    def get(self):
        query, total_page, page = self.data
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

    def get_result(self, query):
        result = self.es_execute(query)
        student_num = result.hits.total
        aggs = result.aggregations
        enroll_total = int(aggs.enroll_total.value or 0)
        enroll_avg = self.round_data_2(aggs.enroll_avg.value or 0)
        discussion_total = int(aggs.discussion_total.value or 0)
        discussion_avg = self.round_data_2(aggs.discussion_avg.value or 0) 
        accomplish_percent = self.round_data_4(aggs.accomplish_avg.value or 0)
        correct_percent = self.round_data_4(aggs.correct_avg.value or 0)
        
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
                     .sort('-participate_total_user', 'binding_uid')
        
        return query

    def get_result(self, query, page, num, student_keyword):
        if student_keyword != 'all':
            query = query.filter(Q('bool', should=[Q('wildcard', rname = '*%s*' % student_keyword)\
                                                 | Q('wildcard', binding_uid = '*%s*' % student_keyword)
                                                   ]))

        total = self.es_execute(query[:0]).hits.total
        total_page = self.get_total_page(total, num)
        
        results = self.es_execute(query[(page-1)*num: page*num]).hits
        header, datas = self.get_header_data(page, num ,results)

        return header, datas, total_page

    def get_header_data(self, page, num, results):
        datas = []
        header = [{'field': 'id', 'name': u'编号'}]
        for index, result in enumerate(results):
            data = [index + 1 if page == 1 else (page-1)*num + index + 1]
            result = result.to_dict()
            for field in STUDENT_FIELD:
                data.append(result[field])
            datas.append(data)
        for index, field in enumerate(STUDENT_FIELD):
            header_item = {}
            header_item['field'] = field
            header_item['name'] = STUDENT_FORM_HEADER[index]
            header.append(header_item)

        return header, datas

    def get(self):
        page = int(self.get_argument('page', 1))
        num = int(self.get_argument('num', 10))
        student_keyword = self.get_argument('student_keyword', 'all')

        query = self.query
        header, data, total_page = self.get_result(query, page, num, student_keyword)

        self.success_response({'data': data, 'total_page': total_page, 'current_page': page, 'header': header})


@route('/student/detail/overview')
class StudentDetailOverview(AcademicData):
    """
    学生概况，学生详情页汇总信息
    """
    @property
    def query(self):
        binding_uid = self.get_param('binding_uid')
        term_id = self.get_param('term_id')
        query = self.student_query(status=2)\
                    .source(STUDENT_USER_FIELD)
        query_total = query.filter('term', binding_uid = binding_uid)\
                     .filter('term', term_id = term_id)
        query_avg = query.filter('term', term_id = term_id)
        query_avg.aggs.metric('study_video_avg', 'avg', field = 'study_video_user')
        query_avg.aggs.metric('discussion_num_avg', 'avg', field = 'discussion_num_user')
        
        return query_total, query_avg

    def get_result(self, query_total, query_avg):
        result_total = self.es_execute(query_total[:1])
        result_avg = self.es_execute(query_avg[:0])
        aggs_avg = result_avg.aggregations
        total = result_total.hits[0].participate_total_user if result_total.hits else 0
        results = [result.to_dict() for result in result_total.hits]

        result = results[0] if results else {}
        result['open_num'] = result.pop('open_num_user') if results else 0
        result['unopen_num'] = result.pop('unopen_num_user') if results else 0
        result['close_num'] = result.pop('close_num_user') if results else 0
        result['accomplish_percent'] = self.round_data_4(result.pop('accomplish_percent_user') if results else 0 or 0)
        result['discussion_total'] = self.round_data_2(result.pop('discussion_num_user') if results else 0 or 0)
        result['correct_percent'] = self.round_data_4(result.pop('correct_percent_user') if results else 0)
        result['discussion_avg'] = self.round_data_2(aggs_avg.discussion_num_avg.value or 0)
        result['course_total'] = total
        result['study_video_total'] = self.round_data_2(result.pop('study_video_user') if results else 0)
        result['study_video_avg'] = self.round_data_2(aggs_avg.study_video_avg.value or 0)
        
        return result

    def get(self):
        query_total, query_avg = self.query
        result = self.get_result(query_total, query_avg)

        self.success_response({'data': result})


@route('/student/detail/courses')
class StudentDetailCourse(AcademicData):
    """
    学生概况，学生详情页课程列表信息
    """
    @property
    def query(self):
        binding_uid = self.get_param('binding_uid')
        term_id = self.get_param('term_id')
        query = self.student_query(status=1)
        query = query.filter('term', binding_uid = binding_uid)\
                     .filter('term', term_id = term_id)\
                     .source(STUDENT_COURSE_FIELD) 
        
        return query
   
    def get_result(self, query, page, num):
        course_total = self.es_execute(query[:0]).hits.total 
        total_page = self.get_total_page(course_total, num)
        
        results = self.es_execute(query[(page-1)*num:page*num]).hits
        results = [result.to_dict() for result in results]
        for index, item in enumerate(results):
            item['study_rate'] = self.round_data_4(item.pop('study_rate_user') or 0)
            item['correct_percent'] = self.round_data_4(item.pop('correct_percent_user') or 0)
            item['accomplish_percent'] = self.round_data_4(item.pop('accomplish_percent_user') or 0)
            item['study_video_len'] = self.round_data_2(item.pop('study_video_len_user') or 0)
            item['course_time'] = '%s-%s' % (self.formate_date(item, 'start'), self.formate_date(item, 'end'))
            item['id'] = index + 1
            item['grade'] = self.round_data_2(item.pop('grade') or 0)

        return results, total_page
    
    def formate_date(self, item, status):
        date = item.pop('start') if status == 'start' else item.pop('end')
        date = date.split(' ')[0].replace('-', '.') if date else ''
        
        return date

    def get(self):
        page = int(self.get_argument('page', 1))
        num = int(self.get_argument('num', 10))
        query = self.query
        result, total_page = self.get_result(query, page, num)
        
        self.success_response({'data': result, 'total_page': total_page, 'current_page': page})


