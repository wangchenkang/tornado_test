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


@route('/discussion/chapter_student_stat')
class ChapterStudentDiscussion(BaseHandler):
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
                } }, 'aggs': { 'sequentials': { 'terms': { 'field': 'sequential_id',
                        'size': 0
                    },
                    'aggs': {
                        'students': {
                            'terms': {
                                'field': 'uid',
                                'size': 0
                            },
                            'aggs': {
                                'discussions': {
                                    'terms': {
                                        'field': 'item_id',
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

        data = self.es.search(index='main', doc_type='discussion', body=query)

        chapter_student_stat = {}
        for sequential in data['aggregations']['sequentials']['buckets']:
            sequential_id = sequential['key']
            chapter_student_stat.setdefault(sequential_id, {}) 
            for student in sequential['students']['buckets']:
                student_id = student['key']
                chapter_student_stat[sequential_id].setdefault(student_id, []) 
                for discussion_item in student['discussions']['buckets']:
                    if discussion_item['doc_count'] == 0:
                        continue
                    student_record = discussion_item['record']['hits']['hits'][0]
                    student_discussion_item = { 
                        'item_id': student_record['_source']['item_id'],
                        'post_num': student_record['_source']['post_num'],
                        'reply_num': student_record['_source']['reply_num'],
                        'time': student_record['_source']['time'],
                    }
                    chapter_student_stat[sequential_id][student_id].append(student_discussion_item)

        result = { 
            'total': data['hits']['total'],
            'sequentials': chapter_student_stat
        }

        self.success_response(result)
