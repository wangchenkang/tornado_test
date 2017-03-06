#! -*- coding: utf-8 -*-
from __future__ import division
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
import time
from elasticsearch_dsl import Q
from elasticsearch import Elasticsearch
Log.create('student')

@route('/student/binding_org')
class StudentOrg(BaseHandler):
    """
    获取学堂选修课学生列表
    """
    def get(self):
        org = self.get_param('org')

        query = self.es_query(doc_type='course_student_location') \
                .filter('term', courses=self.course_id) \
                .filter('term', binding_org=org)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        students = []
        for item in data.hits:
            students.append({
                'user_id': int(item.uid),
                'binding_id': getattr(item, 'binding_uid', None),
                'binding_org': getattr(item, 'binding_org', None)
            })

        self.success_response({'students': students})


@route('/student/course_student')
class CourseStudent(BaseHandler):
    """
    获取课程的学生列表
    """
    def get(self):
        students = self.get_users()

        self.success_response({'students': students})


@route('/student/course_student_user_name')
class CourseStudentUerName(BaseHandler):
    """
    获取课程的学生姓名列表    
    """
    def get(self):

        owner = self.get_owner()
        user_names = self.get_user_name(self.get_users(), self.group_key, owner)

        self.success_response({'user_name': user_names})

@route('/student/course_grade')
class StudentCourseGrade(BaseHandler):
    """
    获取课程学生成绩
    """
    def get(self):
        course_id = self.course_id
        user_id = self.get_argument('user_id', None)

        query = self.es_query(doc_type='course_grade') \
                .filter('term', course_id=course_id)

        if user_id:
            students = [u.strip() for u in user_id.split(',') if u.strip()]
            if students:
                query = query.filter('terms', user_id=students)

        default_size = 0
        data = self.es_execute(query[:default_size])
        if data.hits.total > default_size:
            data = self.es_execute(query[:data.hits.total])

        result = []
        for item in data:
            result.append({
                'user_id': item.user_id,
                'grade': item.grade
            })

        self.success_response({
            'data': result,
            'total': data.hits.total
        })


@route('/student/study_student_list')
class StudyStudentList(BaseHandler):
    """
    获取章学生列表
    """
    def get(self):
        chapter_id = self.get_param('chapter_id')
        start = int(self.get_argument('start', 0))
        size = int(self.get_argument('size', 20))

        students = self.get_problem_users()
        if len(students) >= start + size:
            self.success_response({'data': students[start: start+size]})

        video_students = self.get_video_users()
        for item in video_students:
            if item not in students:
                students.append(item)
        if len(students) >= start + size:
            self.success_response({'data': students[start: start+size]})
        all_students = self.get_users()
        for item in all_students:
            if item not in students:
                students.append(item)
        
        self.success_response({'data': students[start: start+size]})


@route('/(student|staff)/periods')
class StudyPeriod(BaseHandler):
    """
    查询学生学习时段
    """
    def get(self, role):
        user_id = self.get_param('user_id')

        if role == 'staff':
            query = self.es_query(doc_type='user_staff_period') \
                .filter('term', user_id=user_id)
        else:
            query = self.es_query(doc_type='user_study_period') \
                .filter('term', user_id=user_id)

        periods = {
            '0': '[00:00~06:00)',
            '1': '[06:00~12:00)',
            '2': '[12:00~18:00)',
            '3': '[18:00~24:00)',
        }

        data = self.es_execute(query)
        try:
            hit = data.hits[0]
        except IndexError:
            hit = {}

        user_periods = {
            '0': getattr(hit, '0', 0),
            '1': getattr(hit, '1', 0),
            '2': getattr(hit, '2', 0),
            '3': getattr(hit, '3', 0),
        }

        self.success_response({'periods': periods, 'user_periods': user_periods})


@route('/student/information')
class StudentInformation(BaseHandler):
    """
    查询学生基本信息
    """
    def get(self):
        user_id = self.get_param('user_id')

        user_sum_query = self.es_query(doc_type='user_sum') \
                .filter('term', user_id=user_id)[:0]
        user_sum_query.aggs.metric('post_total', 'sum', field='post_number')
        user_sum_query.aggs.metric('comment_total', 'sum', field='comment_number')

        data = self.es_execute(user_sum_query)

        post_total = int(data.aggregations.post_total.value)
        comment_total = int(data.aggregations.comment_total.value)

        enrollment_query = self.es_query(doc_type='student_courseenrollment') \
                .filter('term', uid=user_id).filter('term', is_active=True)[:10000]
        enrollment_data = self.es_execute(enrollment_query)

        comment_len_query = self.es_query(doc_type='user_average_comment_length') \
                .filter('term', user_id=user_id)[:1]
        comment_len_data = self.es_execute(comment_len_query)
        try:
            comment_avg_length = comment_len_data.hits[0].comment_average_length
        except IndexError:
            comment_avg_length = 0

        first_course_query = self.es_query(doc_type='user_first_course') \
                .filter('term', user_id=user_id)[:1]
        first_course_data = self.es_execute(first_course_query)
        try:
            first_course = {
                'course_id': first_course_data.hits[0].first_course_id,
                'course_name': first_course_data.hits[0].first_course_name,
                'time_delta': first_course_data.hits[0].first_course_time_delta
            }
        except IndexError:
            first_course = {}

        courses = []
        for item in enrollment_data.hits:
            courses.append(item.course_id)


        # if staff
        staff_query = self.es_query(doc_type='user_staff_statistics') \
                .filter('term', user_id=user_id)[:0]
        staff_data = self.es_execute(staff_query)
        is_staff = True if staff_data.hits.total else False

        # video length per online days
        video_avg_query = self.es_query(doc_type='user_avg_video_time_per_day') \
                .filter('term', user_id=user_id)[:1]
        video_avg_data = self.es_execute(video_avg_query)
        try:
            study_time_per_day = video_avg_data[0].user_avg_video_time_per_day
        except IndexError:
            study_time_per_day = 0

        self.success_response({
            'user_id': user_id,
            'post_total': post_total,
            'comment_total': comment_total,
            'comment_avg_length': comment_avg_length,
            'courses': courses,
            'first_course': first_course,
            'is_staff': is_staff,
            'study_time_per_day': study_time_per_day
        })


@route('/student/courses')
class StudentCourses(BaseHandler):
    def get(self):
        user_id = self.get_param('user_id')

        user_sum_query = self.es_query(doc_type='user_sum') \
                .filter('term', user_id=user_id)[:0]
        user_sum_query.aggs.bucket('course', 'terms', field='course_id') \
                .metric('post_total', 'sum', field='post_number') \
                .metric('comment_total', 'sum', field='comment_number')

        data = self.es_execute(user_sum_query)

        enrollment_query = self.es_query(doc_type='student_courseenrollment') \
                .filter('term', uid=user_id).filter('term', is_active=True)[:10000]
        enrollment_data = self.es_execute(enrollment_query)
        video_rate_query = self.es_query(doc_type='course_video_rate') \
                .filter('term', uid=user_id)[:10000]
        video_rate_data = self.es_execute(video_rate_query)

        video_query = self.es_query(doc_type='course_video_study_length') \
                .filter('term', user_id=user_id)
        video_data = self.es_execute(video_query[:0])
        video_data = self.es_execute(video_query[:video_data.hits.total])

        courses = {}
        for item in enrollment_data.hits:
            courses[item.course_id] = {
                'user_id': user_id,
                'course_id': item.course_id,
                'time': item.event_time,
                'post_total': 0,
                'comment_total': 0,
                'video_rate': 0,
                'video_count': 0,
                'video_length': 0
            }

        for item in data.aggregations.course.buckets:
            if item.key not in courses:
                continue
            courses[item.key].update({
                'post_total': int(item.post_total.value),
                'comment_total': int(item.comment_total.value)
            })

        for item in video_rate_data.hits:
            if item.course_id not in courses:
                continue
            courses[item.course_id].update({
                'video_rate': round(float(item.study_rate_open), 4)
            })

        for item in video_data.hits:
            if item.course_id not in courses:
                continue
            courses[item.course_id]['video_count'] = item.video_count
            courses[item.course_id]['video_length'] = item.study_length

        self.success_response({'courses': courses.values()})


@route('/staff/information')
class StaffInformation(BaseHandler):
    def get(self):
        user_id = self.get_param('user_id')

        staff_query = self.es_query(doc_type='user_staff_statistics') \
                .filter('term', user_id=user_id)[:1]
        staff_data = self.es_execute(staff_query)

        try:
            staff = staff_data.hits[0]
            courses = [str(c) for c in staff.staff_course_list]

            video_query = self.es_query(doc_type='course_video_study_length') \
                    .filter('terms', course_id=courses)
            video_data = self.es_execute(video_query[:0])
            video_data = self.es_execute(video_query[:video_data.hits.total])
            total_study_length = 0
            for item in video_data.hits:
                total_study_length += item.study_length

            comment_query = self.es_query(doc_type='staff_avg_comment_num_per_day') \
                    .filter('term', user_id=user_id)[:1]
            comment_data = self.es_execute(comment_query)
            try:
                staff_comments_per_day = comment_data.hits[0].staff_avg_comment_num_per_day
            except IndexError:
                staff_comments_per_day = 0

            self.success_response({
                'is_staff': True,
                'students_num': staff.staff_student_num,
                'pass_students_num': staff.staff_pass_students_num,
                'user_id': staff.user_id,
                'comment_num': staff.staff_comment_num,
                'comment_avg_length': staff.staff_pass_students_num,
                'days': staff.staff_days,
                'courses': courses,
                'students_study_length': total_study_length,
                'staff_comments_per_day': staff_comments_per_day,
                'forum_total_length': staff.staff_all_courses_comments_len
            })
        except IndexError:
            self.success_response({'is_staff': False})


@route('/user/average')
class UserAverage(BaseHandler):
    def get(self):
        staff_query = self.es_query(doc_type='user_staff_statistics')
        staff_data = self.es_execute(staff_query[:0])
        staff_data = self.es_execute(staff_query[:staff_data.hits.total])
        staff_comments_num = 0
        for item in staff_data.hits:
            staff_comments_num += item.staff_comment_num

        staff_avg_comments_num = staff_comments_num / staff_data.hits.total

        enrollments_query = self.es_query(doc_type='student_courseenrollment') \
                .filter('term', is_active=True)[:0]
        enrollments_data = self.es_execute(enrollments_query)
        students_query = self.es_query(doc_type='course_student_location') \
                .filter('exists', field='courses')[:0]
        students_data = self.es_execute(students_query)

        students_avg_enrollment = enrollments_data.hits.total / students_data.hits.total

        self.success_response({
            'staff_avg_comments_num': staff_avg_comments_num,
            'students_avg_enrollment': students_avg_enrollment
        })


@route('/student/student_info')
class StudentInfo(BaseHandler):
    def get(self):
        student_keyword = self.get_argument('student_keyword')
        if not student_keyword:
            student_keyword = ""
        student_info = []
        query = self.es_query(doc_type='student_enrollment_info')\
                     .filter('term', user_id=self.user_id)
        size = self.es_execute(query[:0]).hits.total
        if student_keyword == "":
            result = self.es_execute(query[:size])
            for i in result.hits.hits:
                student_info.append(i["_source"])
            self.success_response({'data': student_info})
        import settings
        client = Elasticsearch(settings.es_cluster)
        result = client.search(index='tapgo',doc_type='student_enrollment_info',\
                        size=size,\
                        body={\
                            "query": {\
                            "bool": {\
                            "must":[{"match": {"user_id": self.user_id}}],\
                            "should": [\
                            {"wildcard" : { "rname" : { "value" : "%s*" % student_keyword}}},
                            {"wildcard" : { "nickname" : { "value" : "%s*" % student_keyword}}},
                            {"wildcard" : { "binding_uid" : { "value" : "%s*" % student_keyword}}}
                            ]}}})\

        for i in result["hits"]["hits"]:
            student_info.append(i['_source'])
        self.success_response({'data': student_info})


