#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Q
from .base import BaseHandler
from utils.routes import route


@route('/exam/examed_students')
class ExamedStudent(BaseHandler):
    """
    获取考试学生列表
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')

        query = self.es_query(index='main', doc_type='seq_stu_grade_exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        students = set()
        for item in data.hits:
            students.add(int(item.user_id))

        self.success_response({'students': list(students)})


@route('/exam/students_stat')
class StudentStat(BaseHandler):
    """
    考试章学生数统计
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')

        # 取没有group的学生数
        query = self.es_query(index='main', doc_type='libc_stu_num_exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id) \
                .filter('term', librayc_id='-1')[:1]
        data = self.es_execute(query)
        try:
            exam_total = data.hits[0].user_num
        except (KeyError, IndexError):
            exam_total = 0

        # 取group后各个分数段的学生数
        query = self.es_query(index='main', doc_type='libc_group_stu_num_exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id) \
                .filter('term', librayc_id='-1')
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        groups = {}
        for item in data.hits:
            groups[item.group_id] = {
                'group_id': item.group_id,
                'user_num': item.user_num
            }

        self.success_response({'exam_student_num': exam_total, 'groups': groups})


@route('/exam/content_students_stat')
class ContentStudentStat(BaseHandler):
    """
    考试章内容(library content)学生统计
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')

        # 获取各个考试章library_content 下学生数
        query = self.es_query(index='main', doc_type='libc_stu_num_exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id) \
                .query(~Q('term', librayc_id='-1'))
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        content = {}
        for item in data.hits:
            content.setdefault(item.librayc_id, {})
            content[item.librayc_id]['student_num'] = item.user_num

        # 获取各个考试章library_content 下各分数段的学生数
        query = self.es_query(index='main', doc_type='libc_group_stu_num_exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id) \
                .query(~Q('term', librayc_id='-1'))
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])
        for item in data.hits:
            content.setdefault(item.librayc_id, {}).setdefault('groups', {})
            content[item.librayc_id]['groups'][item.group_id] = {
                'block_id': item.librayc_id,
                'group_id': item.group_id,
                'user_num': item.user_num
            }

        self.success_response({'content_groups': content})


@route('/exam/students_grade')
class StudentGrade(BaseHandler):
    """
    获取学生考试成绩
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')
        user_id = self.get_argument('user_id', None)

        query = self.es_query(index='main', doc_type='seq_stu_grade_exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id)

        if user_id is not None:
            students = [u.strip() for u in user_id.split(',')]
            query = query.filter('terms', user_id=students)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        students_grade = {}
        for item in data.hits:
            students_grade[item.user_id] = {
                'user_id': int(item.user_id),
                'grade': item.grade,
                'max_score': item.max_score,
                'grade_percent': item.grade_percent
            }

        self.success_response({'students_grade': students_grade})


@route('/exam/students_grade_detail')
class StudentGradeDetail(BaseHandler):
    """
    获取学生考试成绩详细信息
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')
        user_id = self.get_argument('user_id', None)

        query = self.es_query(index='main', doc_type='lib_stu_grade_exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id)

        if user_id is not None:
            students = [u.strip() for u in user_id.split(',')]
            query = query.filter('terms', user_id=students)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        students_grade = {}
        for item in data.hits:
            students_grade.setdefault(item.user_id, {})
            students_grade[item.user_id][item.librayc_id] = {
                'user_id': int(item.user_id),
                'grade': item.grade,
                'max_score': item.max_score,
                'grade_percent': item.grade_percent,
                'seq_format': item.seq_format,
                'content_id': item.librayc_id
            }

        self.success_response({'students_grade': students_grade})
