#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/problem/chapter_stat')
class ChapterProblem(BaseHandler):
    def get(self):
        course_id = self.get_argument('course_id')
        chapter_id = self.get_argument('chapter_id')
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
