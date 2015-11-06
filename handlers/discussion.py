#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/discussion/chapter_stat')
class ChapterDiscussion(BaseHandler):
    def get(self):
        course_id = self.get_argument('course_id')
        chapter_id = self.get_argument('chapter_id')
        query = { 
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'exists': {'field': 'uid'}}
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

        data = self.es.search(index='main', doc_type='discussion', search_type='count', body=query)
        discussion_stat = {
            'total': data['hits']['total'],
            'sequentials': {}
        }
        
        for item in data['aggregations']['sequentials']['buckets']:
            discussion_stat['sequentials'][item['key']] = {
                'student_num': item['doc_count']
            }

        self.success_response(discussion_stat)
