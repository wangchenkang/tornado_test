#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
import json
import ast


@route('/problem/chapter_stat')
class ChapterProblem(BaseHandler):
    """

    """
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        grade_gte = self.get_argument('grade_gte', 60)
        query = { 
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'range': {'grade_rate': {'gte': grade_gte}}}
                            ]
                        }
                    }
                }
            },
            'aggs': {
                'sequentials': {
                    'terms': {
                        'field': 'sequential_id',
                        'size': 0
                    }
                }
            },
            'size': 0
        }

        data = self.es_search(index='main', doc_type='student_grade', search_type='count', body=query)
        problem_stat = {
            'total': data['hits']['total'],
            'sequentials': {}
        }
        
        for item in data['aggregations']['sequentials']['buckets']:
            problem_stat['sequentials'][item['key']] = {
                'student_num': item['doc_count']
            }

        self.success_response(problem_stat)

@route('/problem/chapter_student_stat')
class ChapterStudentProblem(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        uid = self.get_param('user_id')
        students = [u.strip() for u in uid.split(',') if u.strip()]

        default_size = 0
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'terms': {'user_id': students}},
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='main', doc_type='student_grade', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='student_grade', body=query)

        chapter_student_stat = {}
        for item in data['hits']['hits']:
            sequential_id = item['_source']['sequential_id']
            student_id = item['_source']['user_id']

            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id][student_id] = {
                'grade_rate': item['_source']['grade_rate']
            }

        result = { 
            'total': data['hits']['total'],
            'sequentials': chapter_student_stat
        }

        self.success_response(result)

@route('/problem/detail')
class CourseProblemDetail(BaseHandler):
    """
    获取课程解析后的习题
    """
    def get(self):
        course_id = self.course_id

        default_size = 0
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='course', doc_type='problem_detail', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='course', doc_type='problem_detail', body=query)

        problems = {}
        for item in data['hits']['hits']:
            problem_id = item['_source']['problem_id']
            problem_num = item['_source']['problem_num']
            problems.setdefault(problem_id, {})
            problems[problem_id][problem_num] = {
                'detail': item['_source']['detail'],
                'answer': item['_source']['answer'],
                'problem_type': item['_source']['problem_type'],
                'problem_id': item['_source']['problem_id'],
                'problem_num': item['_source']['problem_num'],
            }

        self.success_response({'problems': problems})


@route('/problem/chapter_grade_stat')
class ChapterGradeStat(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        group_max_number = 10000

        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'term': {'seq_id': '-1'}}
                            ]
                        }
                    }
                }
            },
            'size': group_max_number
        }
        data = self.es_search(index='main', doc_type='grade_group_stu_num', body=query)

        groups = {}
        for item in data['hits']['hits']:
            groups[item['_source']['group_id']] = {
                'group_id': item['_source']['group_id'],
                'user_num': item['_source']['user_num']
            }

        data = self.es_search(index='main', doc_type='grade_stu_num', body=query)
        try:
            graded_num = data['hits']['hits'][0]['_source']['user_num']
        except (KeyError, IndexError):
            graded_num = 0

        self.success_response({'graded_student_num': graded_num, 'groups': groups})

@route('/problem/chapter_problem_detail')
class ChapterProblemDetail(BaseHandler):
    def get(self):
        result = []
        query = self.search(index='main', doc_type='problem_user')\
                .filter('term', course_id=self.course_id, chapter_id=self.chapter_id)[:0]
        query.aggs.bucket("pid_dim", "terms", field="pid", size=0)\
                .metric("count", "terms", field="answer_right", size=0)
        results = query.execute()
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
        query = self.search(index='main', doc_type='problem_user')\
                .filter("term", course_id=self.course_id,
                        chapter_id=self.chapter_id
                        )\
                .filter("terms", user_id=uid)[:0]
        results = query.execute()
        total = results.hits.total
        query = self.search(index='main', doc_type='problem_user')\
                .filter("term", course_id=self.course_id, 
                        chapter_id=self.chapter_id
                        )\
                .filter("terms", user_id=uid)[:total]
        results = query.execute()
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
