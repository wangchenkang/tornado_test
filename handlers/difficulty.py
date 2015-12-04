#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
from utils import es_utils

Log.create('difficulty')


@route('/video/chapter_review_detail')
class ChapterReviewDetail(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.get_param('chapter_id')
        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='rollup', doc_type='video_review', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='rollup', doc_type='video_review', body=query)

        result = {}
        for item in data['hits']['hits']:
            result[item['_source']['video_id']] = item['_source']

        self.success_response({'data': result})


@route('/difficulty/video_problem_wrong')
class VideoProblemWrong(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.get_param('chapter_id')

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='rollup', doc_type='video_problem_wrong', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='rollup', doc_type='video_problem_wrong', body=query)

        result = {}
        for item in data['hits']['hits']:
            result[item['_source']['video_id']] = item['_source']

        self.success_response({'data': result})


@route('/difficulty/difficulty_detail')
class VideoProblemWrong(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.get_param('chapter_id')

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='rollup', doc_type='difficulty_detail', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='rollup', doc_type='difficulty_detail', body=query)

        result = {}
        for item in data['hits']['hits']:
            result.setdefault(item['_source']['video_id'], []).append(item['_source'])

        self.success_response({'data': result})


@route('/difficulty/chapter_video_duration_stat')
class ChapterVideoDurationStat(BaseHandler):
    def get(self):
        course_id = self.course_id

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}}
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='rollup', doc_type='difficulty_detail_aggs', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='rollup', doc_type='difficulty_detail_aggs', body=query)

        result = {}
        for item in data['hits']['hits']:
            result.setdefault(item['_source']['chapter_id'], {
                'chapter_id': item['_source']['chapter_id'],
                'duration': item['_source']['total_video_len'],
                'count': item['_source']['total_stu_num']
            })

        self.success_response({'data': result})


@route('/difficulty/chapter_review_duration_stat')
class ChapterReviewDurationStat(BaseHandler):
    def get(self):
        course_id = self.course_id

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}}
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='rollup', doc_type='video_review_aggs', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='rollup', doc_type='video_review_aggs', body=query)

        result = {}
        for item in data['hits']['hits']:
            result.setdefault(item['_source']['chapter_id'], {
                'chapter_id': item['_source']['chapter_id'],
                'total': item['_source']['total'],
                'avg': item['_source']['avg']
            })

        self.success_response({'data': result})


@route('/difficulty/chapter_problem_stat')
class ChapterProblemStat(BaseHandler):
    def get(self):
        course_id = self.course_id

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}}
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='rollup', doc_type='video_problem_stats', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='rollup', doc_type='video_problem_stats', body=query)

        result = {}
        for item in data['hits']['hits']:
            result.setdefault(item['_source']['chapter_id'], {
                'chapter_id': item['_source']['chapter_id'],
                'total': item['_source']['total'],
                'avg': item['_source']['avg']
            })

        self.success_response({'data': result})

