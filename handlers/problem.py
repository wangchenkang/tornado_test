#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
import ast


Log.create('problem')

@route('/problem/chapter_stat')
class ChapterProblem(BaseHandler):
    def get(self):
        chapter_id = self.chapter_id
        grade_gte = self.get_argument('grade_gte', 60)

        query = self.es_query(index='main', doc_type='student_grade') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('range', **{'grade_rate': {'gte': grade_gte}})[:0]
        query.aggs.bucket('sequentials', 'terms', field='sequential_id', size=0)
        data = self.es_execute(query)
        problem_stat = {
            'total': data.hits.total,
            'sequentials': {}
        }
        for item in data.aggregations.sequentials.buckets:
            problem_stat['sequentials'][item.key] = {
                'student_num': item.doc_count
            }

        self.success_response(problem_stat)


@route('/problem/chapter_student_stat')
class ChapterStudentProblem(BaseHandler):
    def get(self):
        chapter_id = self.chapter_id
        uid = self.get_param('user_id')
        students = [u.strip() for u in uid.split(',') if u.strip()]

        query = self.es_query(index='main', doc_type='student_grade') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('terms', user_id=students)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        chapter_student_stat = {}
        for item in data.hits:
            sequential_id = item.sequential_id
            student_id = item.user_id

            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id][student_id] = {
                'grade_rate': item.grade_rate
            }
        result = {
            'total': data.hits.total,
            'sequentials': chapter_student_stat
        }

        self.success_response(result)


@route('/problem/detail')
class CourseProblemDetail(BaseHandler):
    """
    获取课程解析后的习题
    """
    def get(self):

        query = self.es_query(index='course', doc_type='problem_detail') \
                .filter('term', course_id=self.course_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        problems = {}
        for item in data.hits:
            problem_id = item.problem_id
            problem_num = item.problem_num
            problems.setdefault(problem_id, {})
            problems[problem_id][problem_num] = {
                'detail': item.detail,
                'answer': item.answer.to_dict(),
                'problem_type': item.problem_type,
                'problem_id': item.problem_id,
                'problem_num': item.problem_num,
            }

        self.success_response({'problems': problems})


@route('/problem/chapter_grade_stat')
class ChapterGradeStat(BaseHandler):
    def get(self):
        chapter_id = self.chapter_id

        query = self.es_query(index='main', doc_type='grade_group_stu_num') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('term', seq_id='-1')
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        groups = {}
        for item in data.hits:
            groups[item.group_id] = {
                'group_id': item.group_id,
                'user_num': item.user_num
            }

        query = self.es_query(index='main', doc_type='grade_stu_num') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('term', seq_id='-1')
        data = self.es_execute(query[:1])
        try:
            graded_num = data.hits[0].user_num
        except (KeyError, IndexError, AttributeError):
            graded_num = 0

        self.success_response({'graded_student_num': graded_num, 'groups': groups})


@route('/problem/chapter_problem_detail')
class ChapterProblemDetail(BaseHandler):
    def get(self):
        result = []
        query = self.es_query(index='main', doc_type='problem_user')\
                .filter('term', course_id=self.course_id)\
                .filter('term', chapter_id=self.chapter_id)[:0]
        query.aggs.bucket("pid_dim", "terms", field="pid", size=0)\
                .bucket("count", "terms", field="answer_right", size=0)
        results = self.es_execute(query)
        aggs = results.aggregations
        buckets = aggs["pid_dim"]["buckets"]
        for bucket in buckets:
            pid = bucket["key"]
            count = bucket["count"]["buckets"]
            correct = 0
            incorrect = 0
            for item in count:
                if item["key"] == "1":
                    correct = item["doc_count"]
                if item["key"] == "0":
                    incorrect = item["doc_count"]
            result.append({
                "subproblem": pid,
                "correct": correct,
                "incorrect": incorrect
                })
        self.success_response({"data": result})


@route('/problem/chapter_student_detail_stat')
class ChapterProblemDetailStat(BaseHandler):
    def get(self):
        result = []
        uid_str = self.get_argument('uid', "")
        uid = uid_str.split(',')
        query = self.es_query(index='main', doc_type='problem_user')\
                .filter("term", course_id=self.course_id)\
                .filter("term", chapter_id=self.chapter_id)\
                .filter("terms", user_id=uid)[:0]
        results = self.es_execute(query)
        total = results.hits.total
        query = self.es_query(index='main', doc_type='problem_user')\
                .filter("term", course_id=self.course_id)\
                .filter("term", chapter_id=self.chapter_id)\
                .filter("terms", user_id=uid)[:total]
        results = self.es_execute(query)
        for hit in results.hits:
            correct = 'correct' if hit.answer_right == "1" else "uncorrect"
            answer = hit.answer
            try:
                answer = ast.literal_eval(answer)
            except:
                pass
            result.append({
                'uid': hit.user_id,
                'grade': float(hit.grade),
                'pid': hit.pid,
                'correctness': correct,
                'value': answer,
                'last_modified': hit.answer_time
                })
        self.success_response({'data': result})
