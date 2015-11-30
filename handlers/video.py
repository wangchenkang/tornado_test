#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
from utils import es_utils

Log.create('video')

@route('/video/chapter_stat')
class ChapterVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'range': {'study_rate': {'gte': 0}}}
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


        data = self.es_search(index='main', doc_type='video', search_type='count', body=query)
        video_stat = {
            'total': data['hits']['total'],
            'sequentials': {}
        }
        for item in data['aggregations']['sequentials']['buckets']:
            video_stat['sequentials'][item['key']] = {
                'student_num': item['doc_count'],
            }

        self.success_response(video_stat)


@route('/video/chapter_student_stat')
class ChapterStudentVideo(BaseHandler):
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
                                {'terms': {'uid': students}},
                                {'exists': {'field': 'duration'}},
                                {'exists': {'field': 'study_rate'}}
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='main', doc_type='video', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='video', body=query)

        chapter_student_stat = {}
        for item in data['hits']['hits']:
            sequential_id = item['_source']['sequential_id']
            student_id = item['_source']['uid']
            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id].setdefault(student_id, [])

            chapter_student_stat[sequential_id][student_id].append({
                'video_id': item['_source']['vid'],
                'review': item['_source']['review'],
                'review_rate': item['_source']['review_rate'],
                'watch_num': item['_source']['watch_num'],
                'duration': item['_source']['duration'],
                'study_rate': item['_source']['study_rate'],
                'la_access': item['_source']['la_access']
            })

        result = {
            'total': data['hits']['total'],
            'sequentials': chapter_student_stat
        }

        self.success_response(result)


@route('/video/course_study_rate')
class CourseVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        user_id = self.get_argument('user_id', None)

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
            'size': 0
        }

        if user_id is not None:
            max_length = 1000
            user_id = [int(u.strip()) for u in user_id.split(',') if u.strip()][0:max_length]
            query['query']['filtered']['filter']['bool']['must'].append({
                'terms': {'uid': user_id}
            })
            query['size'] = max_length
            data = self.es_search(index='rollup', doc_type='course_video_rate', body=query) 
        else:
            default_size = 100000
            query['size'] = default_size
            data = self.es_search(index='rollup', doc_type='course_video_rate', body=query) 
            if data['hits']['total'] > default_size:
                query['size'] = data['hits']['total']
                data = self.es_search(index='rollup', doc_type='course_video_rate', body=query) 

        result = []
        users_has_study = set()
        for item in data['hits']['hits']:
            item_uid = int(item['_source']['uid'])
            result.append({
                'user_id': item_uid,
                'study_rate': float(item['_source']['study_rate_open'])
            })
            users_has_study.add(item_uid)

        if user_id:
            users_not_study = set(user_id).difference(users_has_study)
            for item in users_not_study:
                result.append({
                    'user_id': item,
                    'study_rate': 0.000
                })

        self.success_response({'data': result})
