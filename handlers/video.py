#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.es_utils import *


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
                                'videos': {
                                    'terms': {
                                        'field': 'vid',
                                        'size': 0
                                    },
                                    'aggs': {
                                        'record': {
                                            'top_hits': {
                                                'size': 1
                                            }
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

        data = self.es_search(index='main', doc_type='video', body=query)
        chapter_student_stat = {}
        for sequential in data['aggregations']['sequentials']['buckets']:
            sequential_id = sequential['key']
            chapter_student_stat.setdefault(sequential_id, {})
            for student in sequential['students']['buckets']:
                student_id = student['key']
                chapter_student_stat[sequential_id].setdefault(student_id, [])
                for video_item in student['videos']['buckets']:
                    if video_item['doc_count'] == 0:
                        continue
                    student_record = video_item['record']['hits']['hits'][0]
                    student_video_item = {
                        'video_id': student_record['_source']['vid'],
                        'review': student_record['_source']['review'],
                        'review_rate': student_record['_source']['review_rate'],
                        'watch_num': student_record['_source']['watch_num'],
                        'duration': student_record['_source']['duration'],
                        'study_rate': student_record['_source']['study_rate'],
                        'la_access': student_record['_source']['la_access']
                    }
                    chapter_student_stat[sequential_id][student_id].append(student_video_item)

        result = {
            'total': data['hits']['total'],
            'sequentials': chapter_student_stat
        }

        self.success_response(result)


@route('/video/course_study_rate')
class CourseVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        uid_list = self.get_param('user_id')

        max_length = 1000
        user_id = [u.strip() for u in uid_list.split(',') if u.strip()][0:max_length]

        filter_args = [filter_course(course_id)]
        filter_args.append(filter_op('terms', 'uid', user_id))
        query = get_base(filter_args)
        query.update({'size': max_length})

        response = self.es_search(index='rollup', doc_type='course_video_rate', body=query) 
        data = []
        users_has_study = set()
        for doc in response['hits']['hits']:
            data.append({
                'user_id': int(doc['_source']['uid']),
                'study_rate': float(doc['_source']['study_rate_open'])
            })
            users_has_study.add(doc['_source']['uid'])

        users_not_study = set(user_id).difference(users_has_study)
        for item in users_not_study:
            data.append({
                'user_id': int(item),
                'study_rate': 0.000
            })

        self.success_response({'data': data})
