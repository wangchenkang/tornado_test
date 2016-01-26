#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log

Log.create('difficulty')


@route('/video/chapter_review_detail')
class ChapterReviewDetail(BaseHandler):
    def get(self):
        chapter_id = self.get_param('chapter_id')

        query = self.es_query(index='rollup', doc_type='video_review') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {}
        for item in data.hits:
            result[item.video_id] = item.to_dict()

        self.success_response({'data': result})


@route('/difficulty/video_problem_wrong')
class VideoProblemWrong(BaseHandler):
    def get(self):
        chapter_id = self.get_param('chapter_id')

        query = self.es_query(index='rollup', doc_type='video_problem_wrong') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {}
        for item in data.hits:
            result[item.video_id] = item.to_dict()

        self.success_response({'data': result})


@route('/difficulty/chapter_difficulty_detail')
class ChapterDifficultyDetail(BaseHandler):
    def get(self):
        chapter_id = self.get_param('chapter_id')

        query = self.es_query(index='rollup', doc_type='difficulty_detail') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {}
        for item in data.hits:
            result.setdefault(item.video_id, []).append(item.to_dict())

        self.success_response({'data': result})


@route('/difficulty/chapter_video_duration_stat')
class ChapterVideoDurationStat(BaseHandler):
    def get(self):

        query = self.es_query(index='rollup', doc_type='difficulty_detail_aggs') \
                .filter('term', course_id=self.course_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {}
        for item in data.hits:
            result[item.chpater_id] = {
                'chapter_id': item.chapter_id,
                'duration': item.total_video_len,
                'count': item.total_stu_num
            }

        self.success_response({'data': result})


@route('/difficulty/chapter_review_duration_stat')
class ChapterReviewDurationStat(BaseHandler):
    def get(self):

        query = self.es_query(index='rollup', doc_type='video_review_aggs') \
                .filter('term', course_id=self.course_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {}
        for item in data.hits:
            result[item.chapter_id] = {
                'chapter_id': item.chapter_id,
                'total': item.total,
                'avg': item.avg
            }

        self.success_response({'data': result})


@route('/difficulty/chapter_problem_stat')
class ChapterProblemStat(BaseHandler):
    def get(self):

        query = self.es_query(index='rollup', doc_type='video_problem_stats') \
                .filter('term', course_id=self.course_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {}
        for item in data.hits:
            result[item.chapter_id] = {
                'chapter_id': item.chapter_id,
                'total': item.total,
                'avg': item.avg
            }

        self.success_response({'data': result})
