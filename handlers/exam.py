#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Q
from .base import BaseHandler
from utils.routes import route
from collections import defaultdict


@route('/exam/examed_students')
class ExamedStudent(BaseHandler):
    """
    获取考试学生列表
    """
    def get(self):
        users = self.get_users()
        sequential_id = self.get_param('sequential_id')

        query = self.es_query(index='tap', doc_type='seq_problem') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id) \
                .filter('terms', user_id=users)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total]).hits
        if data.total:
            data.sort(key=lambda x: x.grade_ratio, reverse=True)
        #self.success_response({'students': [int(item.user_id) for item in data]})
        self.success_response({'students': [461955]})

@route('/exam/students_stat')
class StudentStat(BaseHandler):
    """
    考试章学生数统计
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')
        users = self.get_users()
        # 取没有group的学生数
        query = self.es_query(index='tap', doc_type='seq_problem') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id) \
                .filter('terms', user_id=users)[:len(users)]
        data = self.es_execute(query)

        groups = defaultdict(int)
        for item in data.hits:
            group_id = int(item.grade/20)+1
            groups[group_id] += 1
        self.success_response({'groups': groups})


@route('/exam/content_students_stat')
class ContentStudentStat(BaseHandler):
    """
    考试章内容(library content)学生统计
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')
        users = self.get_users()
        # 获取各个考试章library_content 下学生数
        query = self.es_query(index='tap', doc_type='exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id) \
                .filter('terms', user_id=users)
        data = self.es_execute(query[:0]).hits
        data = self.es_execute(query[:data.total])

        content = {}
        for item in data.hits:
            if item.lib_id not in content:
                content[item.lib_id] = defaultdict(int)
            content[item.lib_id][int(item.grade_ratio*5)+1] += 1
        self.success_response({'content_groups': content})


@route('/exam/students_grade')
class StudentGrade(BaseHandler):
    """
    获取学生考试成绩
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')
        user_id = self.get_argument('user_id', None)

        query = self.es_query(index='tap', doc_type='exam') \
                .filter('term', course_id=self.course_id) \
                .filter('term', seq_id=sequential_id)

        if user_id is not None:
            students = [u.strip() for u in user_id.split(',')]
            query = query.filter('terms', user_id=students)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])
        students_grade = {}
        for item in data.hits:
            if item.user_id not in students_grade:
                students_grade[item.user_id] = {}
            students_grade[item.user_id][item.lib_id] = {
                'user_id': int(item.user_id),
                'grade': item.grade,
                'max_grade': item.max_grade,
                'grade_percent': item.grade_ratio,
                'lib_id': item.lib_id
            }

        self.success_response({'students_grade': students_grade})

