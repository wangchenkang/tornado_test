#!/usr/bin/env python
# -*- coding: utf-8 -*-

import settings
from tornado.web import gen
from base import BaseHandler
from utils.routes import route
from utils.tools import date_from_string
from utils.tools import fix_course_id
from elasticsearch_dsl import Q
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import A

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

VIDEO_COURSE = ['course_id','course_name','seek_persons_num','seek_persons_num_percent','not_seek_persons', \
                'not_seek_persons_percent','person_avg_seek_num','person_avg_seek_num_percent','person_avg_not_watch','person_avg_not_watch_percent']
WARNING_COURSE = ['course_id','course_name','study_week','time','study_warning_num','study_warning_num_rate','least_2_week_num', \
                  'least_2_week_num_rate','low_video_rate_num','low_video_num_rate','low_grade_num','low_grade_num_rate']
WARNING_TOTAL = ['course_id','course_name','course_status','course_start','study_warning_num','seek_persons_num']
COURSE_DETAIL = ['service_line', 'course_name', 'enroll_num_course', 'effort_course', 'course_status']
COURSE_STUDENT = ['service_line', 'course_name', 'course_status', 'study_rate', 'grade', 'start']
COURSE_FIELD = ['course_id', 'course_name', 'study_video_rate_course', 'no_watch_person_course', 'enroll_num_course', \
                'teacher_num_course', 'effort_course', 'score_avg_course', 'course_status', 'service_line', 'org_id', 'course_start']
TEACHER_FIELD = ['user_id', 'course_num_total', 'course_num', 'first_level', 'term_name', 'course_status', 'discussion_total']
STUDENT_FIELD = ['rname', 'binding_uid', 'faculty', 'major', 'cohort', 'entrance_year', 'participate_total_user', 'open_num_user', \
                 'unopen_num_user', 'close_num_user']
STUDENT_FORM_HEADER = [u'姓名', u'学号', u'院系', u'专业', u'班级', u'入学年份', u'参与课程', u'开课中', u'待开课', u'已结课']
STUDENT_COURSE_FIELD = ['course_status', 'course_id', 'course_name', 'study_video_len_user', 'study_rate_user', \
                        'accomplish_percent_user', 'correct_percent_user', 'grade', 'start', 'end']
STUDENT_USER_FIELD = ['open_num_user', 'unopen_num_user', 'close_num_user', 'study_video_user', 'discussion_num_user', \
                      'accomplish_percent_user', 'participate_total_user', 'correct_percent_user']

class AcademicData(BaseHandler):

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
            if bucket.key == 'unopen':
                unopen_num = bucket.doc_count
            elif bucket.key == 'open':
                open_num = bucket.doc_count
            else:
                close_num = bucket.doc_count

        return unopen_num, open_num, close_num

    def format_course_info(self, results):
        data = []
        for result in results:
            course_info = {}
            course_info['course_name'] = result.course_name
            course_info['study_video_rate'] = self.round_data(result.study_video_rate_course, 4)
            course_info['score_avg'] = self.round_data(result.score_avg_course, 2)
            course_info['effort'] = self.round_data(result.effort_course, 2)
            course_info['student_num'] = result.enroll_num_course
            course_info['teacher_num'] = result.teacher_num_course
            course_info['no_watch_person'] = result.no_watch_person_course
            course_info['course_id'] = result.course_id
            course_info['course_status'] = result.course_status
            course_info['service_line'] = result.service_line
            course_info['org_id'] = result.org_id
            course_info['course_start'] = result.course_start
            data.append(course_info)

        return data

    def format_teacher_course_info(self, results):
        data = []
        for result in results:
            course_info = {}
            course_info['service_line'] = result.service_line
            course_info['course_name'] = result.course_name
            course_info['enroll_num_course'] = result.enroll_num_course
            course_info['effort_course'] = self.round_data(result.effort_course, 4)
            course_info['course_status'] = result.course_status
            data.append(course_info)

        return data

    def format_student_course_info(self, results):
        data = []
        for result in results:
            course_info = {}
            course_info['service_line'] = result.service_line
            course_info['course_name'] = result.course_name
            course_info['course_status'] = result.course_status
            course_info['study_rate'] = self.round_data(result.study_rate, 2)
            course_info['grade'] = self.round_data(result.grade, 2)
            data.append(course_info)

        return data

    def format_teacher_info(self, results):
        data = []
        for result in results:
            teacher_info = {}
            teacher_info['course_num_total'] = result.course_num_total
            teacher_info['term_open_num'] = result.term_open_num
            teacher_info['enroll_num_course'] = result.enroll_num_course
            teacher_info['course_avg_enroll_num'] = self.round_data(result.course_avg_enroll_num, 2)
            teacher_info['discussion_total'] = result.discussion_total
            teacher_info['course_avg_discussion_num'] = self.round_data(result.course_avg_discussion_num, 2)
            teacher_info['accomplish_percent_course'] = self.round_data(result.accomplish_percent_course, 4)
            teacher_info['correct_percent_course'] = self.round_data(result.correct_percent_course, 4)
            data.append(teacher_info)
        return data



    @gen.coroutine
    def get_course_image(self, result):
        course_detail = yield self.course_detail(result['course_id'])
        from datetime import timedelta
        course_end = course_detail['end']
        course_end = date_from_string(course_end) + timedelta(hours=8) if course_end else ''
        course_end = str(course_end).split(' ')[0] if course_end else ''
        image_url = course_detail['image_url']

        raise gen.Return((image_url, course_end))

    @property
    def course_query(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        service_line = self.get_argument('service_line', None)
        service_line = 'all' if not service_line else service_line
        course_status = self.get_argument('course_status', None)
        course_status = 'all' if not course_status else course_status

        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_course_info')\
                    .filter('term', org_id = org_id)\
                    .filter('term', term_id = term_id)

        if service_line != 'all':
            query = query.filter('term', service_line = service_line) 
        if course_status != 'all':
            query = query.filter('term', course_status = course_status)
       
        return query

    @property
    def teacher_course_query(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        user_id = self.get_param('user_id')
        service_line = self.get_argument('service_line', None)
        service_line = 'all' if not service_line else service_line

        query = self.es_query(index = 'newcloud_wechat', doc_type = 'teacher_course').filter('term', org_id = org_id).filter('term', term_id = term_id).filter('term', user_id = user_id)

        if service_line != 'all':
            query = query.filter('term',service_line = service_line)

        return query

    @property
    def student_course_query(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        user_id = self.get_param('user_id')
        service_line = self.get_argument('service_line', None)
        service_line = 'all' if not service_line else service_line

        query = self.es_query(index='newcloud_wechat', doc_type='student_course').filter('term', org_id=org_id).filter('term', term_id=term_id).filter('term', user_id=user_id)

        if service_line != 'all':
            query = query.filter('term', service_line=service_line)

        return query

    @property
    def warning_total_query(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        service_line = self.get_param('service_line', None)
        service_line = 'all' if not service_line else service_line
        # sort = self.get_param('sort', 'course_start')
        # sort_type = self.get_argument('sort_type', 0)
        # sort = '%s' % sort if int(sort_type) else '-%s' % sort
        if not org_id or not term_id or not service_line:
            self.error_response(502, u'缺少参数')  # error

        query = self.es_query(index='newcloud_wechat', doc_type='video_warning_total').filter('term', org_id=org_id).filter(
            'term', term_id=term_id)
        if service_line != 'all':
            query = query.filter('term', service_line=service_line)

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

    def round_data(self, data, size):
        data = float(data)

        return round(data, size)

  
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
        query.aggs.metric('discussion_total', 'sum', field='discussion_total_course')
        query.aggs.metric('accomplish_percent', 'avg', field='accomplish_percent_course')
        query.aggs.metric('correct_percent', 'avg', field='correct_percent_course')
        
        return query[:0]

    def get_result(self, query):
        result = self.es_execute(query)
        aggs = result.aggregations    
        course_num = result.hits.total
        buckets = aggs.per_course_status.buckets
        unopen_num, open_num, close_num = self.get_course_num(buckets)

        video_total = self.round_data(aggs.video_total.value or 0, 2)
        discussion_total = self.round_data(aggs.discussion_total.value or 0, 2)
        accomplish_percent = self.round_data(aggs.accomplish_percent.value or 0, 4)
        correct_percent = self.round_data(aggs.correct_percent.value or 0, 4)
        
        data = {}
        data['course_num'] = course_num
        data['unopen_num'] = unopen_num
        data['open_num'] = open_num
        data['close_num'] = close_num
        data['video_total'] = video_total
        data['discussion_total'] = discussion_total
        data['accomplish_percent'] = accomplish_percent or 0
        data['correct_percent'] = correct_percent or 0
       
        return data

    def get(self):
        query = self.query
        result = self.get_result(query)

        self.success_response({'data': result})


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
        if size:
            query.aggs.bucket('teacher_total', 'terms', field = 'user_id', size = size)
            result = self.es_execute(query[:0])
            buckets = result.aggregations.teacher_total.buckets
            teacher_total = len(buckets)
        else:
            teacher_total = 0
        
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_teacher_level_term')\
                    .filter('term', org_id = org_id)\
                    .filter('term', term_id = term_id)
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)

        size = self.es_execute(query[:0]).hits.total
        if size:
            query.aggs.bucket('term_teacher_num', 'terms', field = 'user_id', size = size)
            result = self.es_execute(query[:0])
            term_teacher_num = result.aggregations.term_teacher_num.buckets
            term_teacher_num = len(term_teacher_num)
        else:
            term_teacher_num = 0

        return teacher_total, term_teacher_num

    def get_creator_discussion_total(self, org_id, faculty, term_id):
        query = self.es_query(index = settings.NEWCLOUD_ACADEMIC_ES_INDEX, doc_type = 'org_teacher_level_term_status')\
                    .filter('term', org_id = org_id)\
                    .filter('term', is_creator = 1)
        if faculty != 'all':
            query = query.filter('term', first_level = faculty)

        size = self.es_execute(query[:0]).hits.total
        if size:
            query.aggs.bucket('teacher_creator_num', 'terms', field = 'user_id', size = size)
            result = self.es_execute(query[:0])
            buckets = result.aggregations.teacher_creator_num.buckets
            teacher_creator_num = len(buckets)
        else:
            teacher_creator_num = 0
        
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
        faculty = 'all' if not faculty else faculty
        term_id = self.get_param('term_id')

        teacher_total, term_teacher_num = self.get_total_term_num(org_id, faculty, term_id)
        teacher_creator_num, discussion_total = self.get_creator_discussion_total(org_id, faculty, term_id) 
        discussion_avg = self.round_data(float(discussion_total)/(term_teacher_num or 1), 2)
        participate_total = self.get_participate_total(org_id, faculty, term_id)
        participate_avg = self.round_data(float(term_teacher_num)/(participate_total or 1), 2)
        
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
        enroll_avg = self.round_data(aggs.enroll_avg.value or 0, 2)
        discussion_total = int(aggs.discussion_total.value or 0)
        discussion_avg = self.round_data(aggs.discussion_avg.value or 0, 2)
        accomplish_percent = self.round_data(aggs.accomplish_avg.value or 0, 4)
        correct_percent = self.round_data(aggs.correct_avg.value or 0, 4)
        
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
        result['accomplish_percent'] = self.round_data(result.pop('accomplish_percent_user') if results else 0 or 0, 4)
        result['discussion_total'] = self.round_data(result.pop('discussion_num_user') if results else 0 or 0, 2)
        result['correct_percent'] = self.round_data(result.pop('correct_percent_user') if results else 0, 4)
        result['discussion_avg'] = self.round_data(aggs_avg.discussion_num_avg.value or 0, 2)
        result['course_total'] = total
        result['study_video_total'] = self.round_data(result.pop('study_video_user') if results else 0, 2)
        result['study_video_avg'] = self.round_data(aggs_avg.study_video_avg.value or 0, 2)
        
        return result

    def get(self):
        query_total, query_avg = self.query
        result = self.get_result(query_total, query_avg)

        self.success_response({'data': result})


@route('/wechat/teacher/overview')
class TeacherTotal(AcademicData):
    """
    教师汇总数据 每个教师只能看到自己的数据
    """
    def get(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        user_id = self.get_param('user_id')
        if not org_id or not term_id or not user_id:
            self.error_response(502, u'缺少参数') # error
        query = self.es_query(index = 'newcloud_wechat', doc_type = 'teacher_total').filter('term', org_id = org_id).filter('term', term_id = term_id).filter('term', user_id = user_id)
        import json
        print json.dumps(query.to_dict())
        size = self.es_execute(query[:0]).hits.total
        results = self.es_execute(query[:size]).hits
        data = []
        teacher = {}
        if not results:
            teacher = {}
        else:
            for result in results:
                teacher['course_num_total'] = result['course_num_total']
                teacher['term_open_num'] = result['term_open_num']
                teacher['enroll_num_course'] = result['enroll_num_course']
                teacher['course_avg_enroll_num'] = self.round_data(result['course_avg_enroll_num'], 2)
                teacher['discussion_total'] = result['discussion_total']
                teacher['course_avg_discussion_num'] = self.round_data(result['user_avg_discussion_num'], 2)
                teacher['accomplish_percent_course'] = self.round_data(result['accomplish_percent_course'], 4)
                teacher['correct_percent_course'] = self.round_data(result['correct_percent_course'], 4)
                data.append(teacher)

        return self.success_response({'data': data})


@route('/wechat/teacher/course')
class TeacherCourse(AcademicData):
    """
    教师课程数据 教师每门课的详细数据
    """
    @property
    def query(self):
        sort = self.get_argument('sort', 'course_start')
        sort_type = self.get_argument('sort_type', 0)
        sort = '%s' % sort if int(sort_type) else '-%s' % sort

        query = self.teacher_course_query
        query = query.source(COURSE_DETAIL).sort(sort, '-enroll_num_course') #desc
        import json
        print json.dumps(query.to_dict())
        return query[:0]

    def get_result(self, query, page, num):
        total = self.es_execute(query).hits.total
        total_page = self.get_total_page(total, num)
        result = self.es_execute(self.query[(page-1)*num: page*num]).hits

        return result, total, total_page

    def get(self):
        page = int(self.get_argument('page', 1))
        num = int(self.get_argument('num', 5))
        query = self.query

        results, total, total_page = self.get_result(query, page, num)
        datas = self.format_teacher_course_info(results)

        return self.success_response({'data': datas, 'total_page': total_page, 'course_num': total, 'current_page': page})


@route('/wechat/student/overview')
class StudentTotal(AcademicData):
    """
    学生汇总数据  每个学生只能看到自己的汇总数据
    """
    def get(self):
        org_id = self.get_param('org_id')
        term_id = self.get_param('term_id')
        user_id = self.get_param('user_id')
        if not org_id or not term_id or not user_id:
            self.error_response(502, u'缺少参数') # error
        query = self.es_query(index = 'newcloud_wechat', doc_type = 'student_total').filter('term', org_id = org_id).filter('term', term_id = term_id).filter('term', user_id = user_id)
        import json
        print json.dumps(query.to_dict())
        size = self.es_execute(query[:0]).hits.total
        results = self.es_execute(query[:size]).hits
        data = []
        student = {}
        if not results:
            student = {}
        else:
            for result in results:
                student['participate_total_user'] = result['participate_total_user']
                student['passed_num'] = result['passed_num']
                student['study_video_user'] = self.round_data(result['study_video_user'], 2)
                student['discussion_num_user'] = result['discussion_num_user']
                student['avg_grade'] = self.round_data(result['avg_grade'], 2)
                print student
                data.append(student)

        return self.success_response({'data': data})


@route('/wechat/student/course')
class StudentCouse(AcademicData):
    """
    学生课程数据  学生所选课程的详细数据
    """
    @property
    def query(self):
        sort = self.get_argument('sort', 'start')
        sort_type = self.get_argument('sort_type', 0)
        sort = '%s' % sort if int(sort_type) else '-%s' % sort

        query = self.student_course_query
        query = query.source(COURSE_STUDENT).sort(sort, '-study_rate')
        import json
        print json.dumps(query.to_dict())
        return query[:0]

    def get_result(self, query, page, num):
        total = self.es_execute(query).hits.total
        total_page = self.get_total_page(total, num)
        result = self.es_execute(self.query[(page-1)*num: page*num]).hits

        return result, total, total_page

    def get(self):
        page = int(self.get_argument('page', 1))
        num = int(self.get_argument('num', 5))
        query = self.query

        results, total, total_page = self.get_result(query, page, num)
        datas = self.format_student_course_info(results)

        return self.success_response({'data': datas, 'total_page': total_page, 'course_num': total, 'current_page': page})

@route('/wechat/warning/total')
class WarningTotal(AcademicData):
    """
    预警汇总数据数据
    """
    def get(self):
        org_id = self.get_argument('org_id')
        term_id = self.get_argument('term_id')
        service_line = self.get_argument('service_line', None)
        service_line = 'all' if not service_line else service_line
        sort = self.get_argument('sort', 'course_start')
        sort_type = self.get_argument('sort_type', 0)
        sort = '%s' % sort if int(sort_type) else '-%s' % sort
        if not org_id or not term_id or not service_line:
            self.error_response(502, u'缺少参数')  # error
        query = self.es_query(index='newcloud_wechat', doc_type='video_warning_total').filter('term',
        org_id=org_id).filter('term', term_id=term_id)
        if service_line != 'all':
            query = query.filter('term', service_line=service_line)
        query = query.source(WARNING_TOTAL).sort(sort, '-study_warning_num')
        import json
        print json.dumps(query.to_dict())
        size = self.es_execute(query[:0]).hits.total
        results = self.es_execute(query[:size]).hits
        data = []
        # course = {}
        if not results:
            course = {}
        else:
            for result in results:
                course = {}
                course['course_id'] = result['course_id']
                course['course_name'] = result['course_name']
                course['course_status'] = result['course_status']
                course['course_start'] = result['course_start']
                course['study_warning_num'] = result['study_warning_num']
                course['seek_persons_num'] = result['seek_persons_num']
                data.append(course)
        return self.success_response({'data': data})


@route('/wechat/warning/course')
class WarningCourse(AcademicData):
    """
    课程预警数据
    """
    def get(self):
        org_id = self.get_argument('org_id')
        term_id = self.get_argument('term_id')
        course_id = self.get_argument('course_id')
        course_id = fix_course_id(course_id)
        service_line = self.get_argument('service_line', None)
        if not org_id or not term_id or not course_id:
            self.error_response(502, u'缺少参数')  # error
        query = self.es_query(index='newcloud_wechat', doc_type='study_warning').filter('term',
        org_id = org_id).filter('term', term_id = term_id).filter('term', course_id = course_id).filter('term',
        service_line = service_line)
        query = query.source(WARNING_COURSE).sort('-_ut')
        import json
        print json.dumps(query.to_dict())
        result = self.es_execute(query)
        print result
        print result[0].to_dict()
        # course = {}
        # course['course_id'] = result[0]['course_id']
        # course['course_name'] = result[0]['course_name']
        # course['study_week'] = result[0]['study_week']
        # course['time'] = result[0]['time']
        # course['study_warning_num'] = result[0]['study_warning_num']
        # course['study_warning_num_rate'] = result[0]['study_warning_num_rate']
        # course['least_2_week_num'] = result[0]['least_2_week_num']
        # course['least_2_week_num_rate'] = result[0]['least_2_week_num_rate']
        # course['low_video_rate_num'] = result[0]['low_video_rate_num']
        # course['low_video_num_rate'] = result[0]['low_video_num_rate']
        # course['low_grade_num'] = result[0]['low_grade_num']
        # course['low_grade_num_rate'] = result[0]['low_grade_num_rate']

        return self.success_response({'data': result[0].to_dict()})


@route('/wechat/video/course')
class VideoCourse(AcademicData):
    """
    课程预警数据
    """
    def get(self):
        org_id = self.get_argument('org_id')
        term_id = self.get_argument('term_id')
        course_id = self.get_argument('course_id')
        course_id = fix_course_id(course_id)
        service_line = self.get_argument('service_line', None)
        if not org_id or not term_id or not course_id:
            self.error_response(502, u'缺少参数')  # error
        query = self.es_query(index='newcloud_wechat', doc_type='seek_video').filter('term',
        org_id = org_id).filter('term', term_id = term_id).filter('term', course_id = course_id).filter('term',
        service_line = service_line)
        query = query.source(VIDEO_COURSE).sort('-_ut')
        import json
        print json.dumps(query.to_dict())
        result = self.es_execute(query).hits
        print result[0].to_dict()
        # course = {}
        # course['course_id'] = result[0]['course_id']
        # course['course_name'] = result[0]['course_name']
        # course['seek_persons_num'] = result[0]['seek_persons_num']
        # course['seek_persons_num_percent'] = result[0]['seek_persons_num_percent']
        # course['not_seek_persons'] = result[0]['not_seek_persons']
        # course['not_seek_persons_percent'] = result[0]['not_seek_persons_percent']
        # course['person_avg_seek_num'] = result[0]['person_avg_seek_num']
        # course['person_avg_seek_num_percent'] = result[0]['person_avg_seek_num_percent']
        # course['person_avg_not_watch'] = result[0]['person_avg_not_watch']
        # course['person_avg_not_watch_percent'] = result[0]['person_avg_not_watch_percent']

        return self.success_response({'data': result[0].to_dict()})





