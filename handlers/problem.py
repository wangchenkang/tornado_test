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
            'aggs': {
                'sequentials': {
                    'terms': {
                        'field': 'sequential_id',
                        'size': 0
                    },
                    'aggs': {
                        'students': {
                            'terms': {
                                'field': 'user_id',
                                'size': 0
                            },
                            'aggs': {
                                'record': {
                                    'top_hits': {'size': 1}
                                }
                            }
                        }
                    }
                }
            },
            'size': 0
        }

        data = self.es_search(index='main', doc_type='student_grade', body=query)

        chapter_student_stat = {}
        for sequential in data['aggregations']['sequentials']['buckets']:
            sequential_id = sequential['key']
            chapter_student_stat.setdefault(sequential_id, {}) 
            for student in sequential['students']['buckets']:
                if student['doc_count'] == 0:
                    continue
                student_id = student['key']
                student_record = student['record']['hits']['hits'][0]
                chapter_student_stat[sequential_id][student_id] = { 
                    'grade_rate': student_record['_source']['grade_rate'],
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
            'aggs': {
                'problems': {
                    'terms': {
                        'field': 'problem_id',
                        'size': 0
                    },
                    'aggs': {
                        'items': {
                            'terms': {
                                'field': 'problem_num',
                                'size': 0
                            },
                            'aggs': {
                                'record': {
                                    'top_hits': {'size': 1}
                                }
                            }
                        }
                    }
                }
            },
            'size': 0
        }

        data = self.es_search(index='course', doc_type='problem_detail', body=query)
        problems = {}
        for problem in data['aggregations']['problems']['buckets']:
            problems.setdefault(problem['key'], {})
            for item in problem['items']['buckets']:
                item_detail = item['record']['hits']['hits'][0]
                problems[problem['key']][item['key']] = {
                    'detail': item_detail['_source']['detail'],
                    'answer': item_detail['_source']['answer'],
                    'problem_type': item_detail['_source']['problem_type'],
                    'problem_id': item_detail['_source']['problem_id'],
                    'problem_num': item_detail['_source']['problem_num'],
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
