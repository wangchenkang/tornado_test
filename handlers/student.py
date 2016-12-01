#! -*- coding: utf-8 -*-
from __future__ import division
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
import time
from elasticsearch_dsl import Q
Log.create('student')

@route('/student/binding_org')
class StudentOrg(BaseHandler):
    """
    获取学堂选修课学生列表
    """
    def get(self):
        org = self.get_param('org')

        query = self.es_query(index='main', doc_type='student') \
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
    def get(self):
        chapter_id = self.get_param('chapter_id')
        start = int(self.get_argument('start', 0))
        size = int(self.get_argument('size', 20))
        import time
        t1 = time.time()

        students = self.get_problem_users()
        print time.time() - t1
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


        # if staff
        staff_query = self.es_query(index='rollup', doc_type='user_staff_statistics') \
                .filter('term', user_id=user_id)[:0]
        staff_data = self.es_execute(staff_query)
        is_staff = True if staff_data.hits.total else False

        # video length per online days
        video_avg_query = self.es_query(index='rollup', doc_type='user_avg_video_time_per_day') \
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

            comment_query = self.es_query(index='rollup', doc_type='staff_avg_comment_num_per_day') \
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
        staff_query = self.es_query(index='rollup', doc_type='user_staff_statistics')
        staff_data = self.es_execute(staff_query[:0])
        staff_data = self.es_execute(staff_query[:staff_data.hits.total])
        staff_comments_num = 0
        for item in staff_data.hits:
            staff_comments_num += item.staff_comment_num

        staff_avg_comments_num = staff_comments_num / staff_data.hits.total

        enrollments_query = self.es_query(index='main', doc_type='enrollment') \
                .filter('term', is_active=True)[:0]
        enrollments_data = self.es_execute(enrollments_query)
        students_query = self.es_query(index='main',doc_type='student') \
                .filter('exists', field='courses')[:0]
        students_data = self.es_execute(students_query)

        students_avg_enrollment = enrollments_data.hits.total / students_data.hits.total

        self.success_response({
            'staff_avg_comments_num': staff_avg_comments_num,
            'students_avg_enrollment': students_avg_enrollment
        })

@route('/student/grade_detail')
class GradeDetail(BaseHandler):
    def get(self):
        sort_field = self.get_param('sort_field')

        # sort_type = self.get_param('sort_type')
        # fields = self.get_param('fields')
        # elective = self.get_param('elective')
        query = self.es_query(index='tap', doc_type='problem_course') \
            .filter('term', course_id=self.course_id)
        size = self.es_execute(query[:0]).hits.total
        header = ['昵称','学堂号','姓名','学号','院系','专业','成绩','课程_得分','课程_得分率']
        data = self.es_execute(query[:size])
        print len(data.hits)
        grade = {}
        for item in data.hits:
            # print item.to_dict()
            grade[item.user_id] = {
                'user_id': item.user_id,
                'final_grade': item.final_grade,
                'grade_ratio': item.grade_ratio,
                'current_grade': item.current_grade
            }
        headerl = ['nickname','xid','rname','binding_uid','faculty','major','final_grade','current_grade','grade_ratio']
        query = self.es_query(index='tap', doc_type='student') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=grade.keys())
        data = self.es_execute(query[:data.hits.total])
        print len(data.hits)
        for item in data.hits:
            # print type(item.user_id)
            # print item.to_dict()
            u = str(item.user_id)
            grade[u]['xid'] = item.xid
            grade[u]['nickname'] = item.nickname
            grade[u]['rname'] = item.rname
            grade[u]['binding_uid'] = item.binding_uid
            grade[u]['faculty'] = item.faculty
            grade[u]['major'] = item.major
        hDict = dict(zip(header, headerl))
        result = []
        for i in grade:
            print sorted(grade[i].items())
            result.append(grade[i])
        # print result
        result = sorted(result, key=lambda x: x[sort_field], reverse=True)
        self.success_response({'header': header, 'data': result})

@route('/student/overview_detail')
class OverviewDetail(BaseHandler):
    def get(self):
        t_beg = time.time()
        print self.course_id
        pn = int(self.get_argument('page', 0))
        num = int(self.get_argument('num', 10))
        sort_field = self.get_argument('sort', None)
        remove_field = self.get_argument('filter', None)
        users = self.get_users()
        """昵称, 学堂号, 姓名 *, 学号 *, 院系 *, 专业 *, 课程_学习比例, 讨论区_发回帖数, 课程_得分率, 课程_得分, 成绩, nickname, xid, rname, binding_uid, faculty, major, video, discussion, grade_ratio, current_grade, final_grade"""
        result = {}
        struct = self.es_query(index='tap', doc_type='grade_struct') \
            .filter('term', course_id=self.course_id)
        struct_d = self.es_execute(struct)
        header = struct_d.hits[0].to_dict()['doc']
        h1 = ['xid', 'nickname', 'rname', 'binding_uid', 'faculty', 'major', 'is_active', 'enroll_time',
              'unenroll_time', 'video_course', 'post', 'reply', 'discussion', 'grade_ratio', 'current_grade', 'final_grade' ]
        types = header['type_header'].split(',_,')
        seqs = header['seq_names'].split(',_,')
        header = h1 + types + seqs

        query = self.es_query(index='tap', doc_type='student') \
            .filter('term', course_id=self.course_id)\
            .filter('terms', user_id=users)
        size = self.es_execute(query[:0]).hits.total
        data = self.es_execute(query[:size])
        for item in data.hits:
            item = item.to_dict()
            result[str(item['user_id'])] = [item['xid'], item['nickname'], item['rname'], item['binding_uid'], item['faculty'], item['major'],item['is_active'], item['enroll_time'], item['unenroll_time']] + [0] * (7 + len(types + seqs))


        query = self.es_query(index='tap', doc_type='video_course') \
            .filter('term', course_id=self.course_id) \
            .filter('terms', user_id=users)
        chapter_video = self.es_execute(query[:size])
        for item in chapter_video.hits:
            item = item.to_dict()
            result[str(item['user_id'])][9] = item['study_rate']

        query = self.es_query(index='tap', doc_type='discussion_aggs') \
            .filter('term', course_id=self.course_id) \
            .filter('terms', user_id=users)
        discussion = self.es_execute(query[:size])
        for item in discussion.hits:
            item = item.to_dict()
            post = item['post_num'] or 0
            reply = item['reply_num'] or 0
            result[str(item['user_id'])][10:13] = [post, reply, post + reply]

        query = self.es_query(index='tap', doc_type='problem_course') \
            .filter('term', course_id=self.course_id) \
            .filter('terms', user_id=users)
        score = self.es_execute(query[:size])
        for item in score.hits:
            item = item.to_dict()
            result[str(item['user_id'])][13:16] = [item['final_grade'], item['grade_ratio'], item['current_grade']]

        query = self.es_query(index='tap', doc_type='grade_overview') \
            .filter('terms', user_id=users)\
            .filter('term', course_id = self.course_id)
        size = self.es_execute(query[:0]).hits.total
        data = self.es_execute(query[:size])
        for item in data.hits:
            item = item.to_dict()['doc']
            type_grade = item['type_grade'].split(',')
            seq_grade = item['seq_grade'].split(',')
            result[str(item['user_id'])][16:] = type_grade + seq_grade
        #
        print len(result)
        # def fil(l):
        #     f = False
        #     for i in l[9:]:
        #         if i != 0:
        #             f = True
        #             break
        #     return f
        # filtered = filter(lambda x: fil(x), result.values())
        t_beg1 = time.time()
        if sort_field:
            index = header.index(sort_field)
            if index < 0:
                self.error_response(200, 'sort_field error')
            filtered = sorted(result.values(), key=lambda x: x[index], reverse=True)
        else:
            filtered = sorted(result.values(), key=lambda x: x[15], reverse=True)
        print time.time() - t_beg1
        if remove_field:
            index = header.index(remove_field)
            del header[index]
            for li in filtered:
                del li[index]
        final = {}
        if num == -1:
            final['data'] = filtered
        else:
            final['data'] = filtered[num * pn: num * pn + num]
        final['header'] = header
        final['total'] = len(filtered)
        t_elapse = time.time() - t_beg
        final['time'] = "%.0f" % (float(t_elapse) * 1000)
        self.success_response(final)

@route('/student/discussion_detail')
class DiscussDetail(BaseHandler):
    def get(self):
        t_beg = time.time()
        users = self.get_users()
        # print self.course_id, users
        pn = int(self.get_argument('page', 0))
        num = int(self.get_argument('num', 10))
        sort_field = self.get_argument('sort', None)
        if sort_field:
            query = self.es_query(index='tap', doc_type='grade_overview1') \
            .filter('term', course_id=self.course_id).sort(sort_field)
        else:
            query = self.es_query(index='tap', doc_type='grade_overview1')#\
            # .filter('term', course_id=self.course_id)
            q = Q("match", course_id=self.course_id)
            query = query.query(q)
        size = self.es_execute(query[:0]).hits.total
        data = self.es_execute(query[:10000])
        # data = self.es_execute(query)
        final = {}
        result = [item.to_dict() for item in data.hits]
        final['total'] = len(result)
        final['data'] = result[num * pn: num * pn + num]
        t_elapse = time.time() - t_beg
        final['time'] = "%.0f" % (float(t_elapse) * 1000)
        self.success_response(final)

        """昵称
学堂号
姓名 *
学号 *
院系 *
专业 *
成绩
课程_得分
课程_得分率
讨论区发回帖数	发帖数	回帖数
nickname	xid	rname	binding_uid	faculty	major	final_grade	current_grade	grade_ratio	discussion	post	reply"""

@route('/student/video_detail')
class GradeDetail(BaseHandler):
    def get(self):
        """昵称
学堂号
姓名 *
学号 *
院系 *
专业 *
成绩
课程_得分
课程_得分率
章学习比例
nickname	xid	rname	binding_uid	faculty	major	final_grade	current_grade	grade_ratio	chapter_video"""

@route('/student/student_detail')
class GradeDetail(BaseHandler):
    def get(self):
        """昵称
学堂号
姓名 *
学号 *
院系 *
专业 *
成绩
课程_得分
课程_得分率
选课状态	最后选课时间	最后退课时间
nickname	xid	rname	binding_uid	faculty	major	final_grade	current_grade	grade_ratio	status	enroll_time	unenroll_time
"""
