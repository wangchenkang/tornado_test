#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/video/chapter_stat')
class ChapterVideo(BaseHandler):
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
                                {'range': {'study_rage': {'gte': 0}}}
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

        data = self.es.search(index='main', doc_type='video', search_type='count', body=query)
        video_stat = {
            'total': data['hits']['total'],
            'sequentials': {}
        }
        for item in data['aggregations']['sequentials']['buckets']:
            video_stat['sequentials'][item['key']] = {
                'student_num': item['doc_count'],
            }

        self.success_response(video_stat)
