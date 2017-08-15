#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/difficulty/chapter_review_detail')
class ChapterReviewDetail(BaseHandler):

    def get(self):

        query = self.es_query(index='rollup', doc_type='video_review') \
                    .filter('term', course_id=self.get_param('course_id')) \
                    .filter('term', chapter_id=self.get_param('chapter_id'))
        total = self.es_execute(query).hits.total
        data = self.es_execute(query[:total])

        result = {}
        for item in data.hits:
            result[item.video_id] = item.to_dict()
    
        self.success_response({'data': result})


@route('/difficulty/video_problem_wrong')
class VideoProblemWrong(BaseHandler):

    def get(self):
        
        query = self.es_query(index='rollup', doc_type='video_problem_wrong') \
                    .filter('term', course_id=self.get_param('course_id')) \
                    .filter('term', chapter_id=self.get_param('chapter_id'))
        total = self.es_execute(query).hits.total
        data = self.es_execute(query[:total])

        result = {}
        for item in data.hits:
            result[item.video_id] = item.to_dict()

        self.success_response({'data': result})


@route('/difficulty/chapter_difficulty_detail')
class ChapterDifficultyDetail(BaseHandler):

    def get(self):

        query = self.es_query(index='rollup', doc_type='difficulty_detail') \
                    .filter('term', course_id=self.get_param('course_id')) \
                    .filter('term', chapter_id=self.get_param('chapter_id'))
        total = self.es_execute(query).hits.total
        data = self.es_execute(query[:total])

        result = {}
        for item in data.hits:
            result.setdefault(item.video_id, []).append(item.to_dict())

        self.success_response({'data': result})


@route('/difficulty/chapter_video_duration_stat')
class ChapterVideoDurationStat(BaseHandler):
    def get(self):

        query = self.es_query(index='rollup', doc_type='difficulty_detail_aggs') \
                    .filter('term', course_id=self.get_param('course_id'))
        total = self.es_execute(query).hits.total
        data = self.es_execute(query[:total])

        result = {}
        for item in data.hits:
            result[item.chapter_id] = {
                'chapter_id': item.chapter_id,
                'duration': item.total_video_len,
                'count': item.total_stu_num
            }

        self.success_response({'data': result})


@route('/difficulty/chapter_review_duration_stat')
class ChapterReviewDurationStat(BaseHandler):
    def get(self):

        query = self.es_query(index='rollup', doc_type='video_review_aggs') \
                    .filter('term', course_id=self.get_param('course_id'))
        total = self.es_execute(query).hits.total
        data = self.es_execute(query[:total])

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
                    .filter('term', course_id=self.get_param('course_id'))
        total = self.es_execute(query).hits.total
        data = self.es_execute(query[:total])

        result = {}
        for item in data.hits:
            result[item.chapter_id] = {
                'chapter_id': item.chapter_id,
                'total': item.total,
                'avg': item.avg
            }

        self.success_response({'data': result})

@route('/difficulty/course_status')
class Courses(BaseHandler):
    
    def get(self):

        course_id = self.get_param('course_id')
        query = self.es_query(index='rollup', doc_type='course_keywords')\
                    .filter('term', course_id=course_id)
        
        total = self.es_execute(query).hits.total
        status = True if total != 0 else False
    
        self.success_response({'status': status})
