#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/problem/chapter_stat')
class ChapterProblem(BaseHandler):
    """

    """
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        grade_gte = self.get_param('grade_gte', 60)
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

        default_size = 100000
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

        default_size = 100000
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
