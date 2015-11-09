#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/problem/chapter_stat')
class ChapterProblem(BaseHandler):
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

        data = self.es.search(index='main', doc_type='student_grade', search_type='count', body=query)
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
        uid = self.get_param('uid')
        students = [u.strip() for u in uid.split(',') if u.strip()]

        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'terms': {'uid': students}},
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
                                'field': 'uid',
                                'size': 0
                            },
                            'aggs': {
                                'problems': {
                                    'terms': {
                                        'field': 'id',
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
                    }
                }
            },
            'size': 0
        }

        data = self.es.search(index='main', doc_type='student_grade', body=query)

        chapter_student_stat = {}
        for sequential in data['aggregations']['sequentials']['buckets']:
            sequential_id = sequential['key']
            chapter_student_stat.setdefault(sequential_id, {}) 
            for student in sequential['students']['buckets']:
                student_id = student['key']
                chapter_student_stat[sequential_id].setdefault(student_id, []) 
                for problem_item in student['problems']['buckets']:
                    if problem_item['doc_count'] == 0:
                        continue
                    student_record = problem_item['record']['hits']['hits'][0]
                    student_problem_item = { 
                        'problem_id': student_record['_source']['id'],
                        'grade_rate': student_record['_source']['grade_rate'],
                    }
                    chapter_student_stat[sequential_id][student_id].append(student_problem_item)

        result = { 
            'total': data['hits']['total'],
            'sequentials': chapter_student_stat
        }

        self.success_response(result)

