#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
from utils.tools import date_to_str
from elasticsearch_dsl import F
Log.create('student')

@route('/student/binding_org')
class StudentOrg(BaseHandler):
    """ 
    获取学堂选修课学生列表
    """
    def get(self):
        course_id = self.course_id
        org = self.get_param('org')
    
        default_size = 0
        query = { 
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'courses': course_id}},
                                {'term': {'binding_org': org}},
                            ]   
                        }   
                    }   
                }   
            },  
            'size': default_size
        }   

        data = self.es_search(index='main', doc_type='student', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='student', body=query)

        students = []
        for item in data['hits']['hits']:
            students.append({
                'user_id': int(item['_source']['uid']),
                'binding_id': item['_source'].get('binding_uid', None),
                'binding_org': item['_source'].get('binding_org', None),
            })

        self.success_response({'students': students})


@route('/student/course_grade')
class StudentCourseGrade(BaseHandler):
    """
    获取课程学生成绩
    """
    def get(self):
        course_id = self.course_id
        user_id = self.get_argument('user_id', None)

        query = self.es_query(index='main', doc_type='student_course_grade') \
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
    # TODO: 未完成
    def get(self):
        chapter_id = self.get_param('chapter_id')
        query = self.search(index='main', doc_type='problem_user')\
                .filter('term', course_id=self.course_id)\
                .filter('term', chapter_id=chapter_id)[:0]
        query.aggs.metric('student', 'terms', field='user_id', size=0)
        results = query.execute()
        buckets = results.aggregations.student.buckets
        uids = map(lambda x: x["key"], buckets)
        query = self.search(index='main', doc_type='enrollment')\
                .filter('term', course_id=self.course_id)\
                .filter('term', is_active=True)\
                .filter('terms', uid=uids)[:len(uids)]
        results = query.execute()
        hits = results.hits
        uid_list = map(lambda x: x.uid, hits)
        query = self.search(index='main', doc_type='enrollment')\
                .filter('term', course_id=self.course_id)\
                .filter('term', is_active=True)\
                .filter(~F('terms', uid=uids))
        results = query[:0].execute()
        total = results.hits.total
        results = query[:total].execute()
        hits = results.hits
        _uids = map(lambda x: x.uid, hits)
        uid_list.extend(_uids)
        self.success_response({'data': uid_list})


@route('/(student|staff)/periods')
class StudyPeriod(BaseHandler):
    """
    查询学生学习时段
    """
    def get(self, role):
        user_id = self.get_param('user_id')

        if role == 'staff':
            query = self.es_query(index='rollup', doc_type='user_staff_period') \
                .filter('term', user_id=user_id)
        else:
            query = self.es_query(index='rollup', doc_type='user_study_period') \
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

        user_sum_query = self.es_query(index='main', doc_type='user_sum') \
                .filter('term', user_id=user_id)[:0]
        user_sum_query.aggs.metric('post_total', 'sum', field='post_number')
        user_sum_query.aggs.metric('comment_total', 'sum', field='comment_number')

        data = self.es_execute(user_sum_query)

        post_total = int(data.aggregations.post_total.value)
        comment_total = int(data.aggregations.comment_total.value)

        enrollment_query = self.es_query(index='main', doc_type='enrollment') \
                .filter('term', uid=user_id).filter('term', is_active=True)[:10000]
        enrollment_data = self.es_execute(enrollment_query)

        comment_len_query = self.es_query(index='rollup', doc_type='user_average_comment_length') \
                .filter('term', user_id=user_id)[:1]
        comment_len_data = self.es_execute(comment_len_query)
        try:
            comment_avg_length = comment_len_data.hits[0].comment_average_length
        except IndexError:
            comment_avg_length = 0

        first_course_query = self.es_query(index='rollup', doc_type='user_first_course') \
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

        self.success_response({
            'user_id': user_id,
            'post_total': post_total,
            'comment_total': comment_total,
            'comment_avg_length': comment_avg_length,
            'courses': courses,
            'first_course': first_course
        })


@route('/student/courses')
class StudentCourses(BaseHandler):
    def get(self):
        user_id = self.get_param('user_id')

        user_sum_query = self.es_query(index='main', doc_type='user_sum') \
                .filter('term', user_id=user_id)[:0]
        user_sum_query.aggs.bucket('course', 'terms', field='course_id') \
                .metric('post_total', 'sum', field='post_number') \
                .metric('comment_total', 'sum', field='comment_number')

        data = self.es_execute(user_sum_query)

        enrollment_query = self.es_query(index='main', doc_type='enrollment') \
                .filter('term', uid=user_id).filter('term', is_active=True)[:10000]
        enrollment_data = self.es_execute(enrollment_query)
        video_rate_query = self.es_query(index='rollup', doc_type='course_video_rate') \
                .filter('term', uid=user_id)[:10000]
        video_rate_data = self.es_execute(video_rate_query)

        video_query = self.es_query(index='rollup', doc_type='course_video_study_length') \
                .filter('term', uid=user_id)
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

        staff_query = self.es_query(index='rollup', doc_type='user_staff_statistics') \
                .filter('term', user_id=user_id)[:1]
        staff_data = self.es_execute(staff_query)

        try:
            staff = staff_data.hits[0]
            courses = [str(c) for c in staff.staff_course_list]

            video_query = self.es_query(index='rollup', doc_type='course_video_study_length') \
                    .filter('terms', course_id=courses)
            video_data = self.es_execute(video_query[:0])
            video_data = self.es_execute(video_query[:video_data.hits.total])
            total_study_length = 0
            for item in video_data.hits:
                total_study_length += item.study_length

            self.success_response({
                'is_staff': True,
                'students_num': staff.staff_student_num,
                'pass_students_num': staff.staff_pass_students_num,
                'user_id': staff.user_id,
                'comment_num': staff.staff_comment_num,
                'comment_avg_length': staff.staff_pass_students_num,
                'days': staff.staff_days,
                'courses': courses,
                'students_study_length': total_study_length
            })
        except IndexError:
            self.success_response({'is_staff': False})
