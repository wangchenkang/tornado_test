#! -*- coding: utf-8 -*-
import time
from utils.log import Log
from utils.routes import route
from .base import BaseHandler

Log.create('student')

class TableHandler(BaseHandler):

    def get_query(self, course_id, chapter_id,  page, num, sort, sort_type, fields):
        pass

    def post(self):
        time_begin = time.time()
        page = int(self.get_argument('page', 0))
        num = int(self.get_argument('num', 10))
        sort = self.get_argument('sort', None)
        sort_type = int(self.get_argument('sort_type', 0))
        fields = self.get_argument('fields', '')
        fields = fields.split(',') if fields else []
        course_id = self.course_id
        chapter_id = self.chapter_id
        print chapter_id
        user_ids = self.get_users()

        query = self.get_query(course_id, chapter_id, user_ids, page, num, sort, sort_type, fields)
        if fields:
            query = query.source(fields)
        
        
        size = self.es_execute(query[:0]).hits.total
        data = self.es_execute(query[:size])

        final = {}
        result = [item.to_dict() for item in data.hits]
    
        for i in result:
            for j in i.keys():
                try:
                     i[i[j]["block_id"]]= i[j]["study_rate"]
                except Exception as e:
                    pass

        final['total'] = size
        final['data'] = result
        time_elapse = time.time() - time_begin
        final['time'] = "%.0f" % (float(time_elapse) * 1000)

        self.success_response(final)

@route('/table/grade_overview')
class GradeDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tap2_test', doc_type='grade_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)[num*page:num*page+num]
        else:
            query = self.es_query(index='tap2_test', doc_type='grade_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)[num*page:num*page+num]
        return query

@route('/table/question_overview')
class QuestionDetail(TableHandler):
    def get_query(self, course_id, chapter_id, user_ids, page, num, sort, sort_type, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tap5_test', doc_type='question_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('term', chapter_id=chapter_id)\
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)[num*page:num*page+num]
        else:
            query = self.es_query(index='tap5_test', doc_type='question_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('term', chapter_id=chapter_id)\
                        .filter('terms', user_id=user_ids)[num*page:num*page+num]

        return query

@route('/table/video_overview')
class VideoDetail(TableHandler):
    def get_query(self, course_id, chapter_id, user_ids, page, num, sort, sort_type, fields):
        print "************"
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tap5_test', doc_type='video_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)[num*page:num*page+num]
        else:
            query = self.es_query(index='tap5_test', doc_type='video_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)[num*page:num*page+num]

        return query

@route('/table/discussion_overview')
class DiscussionDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tap2_test', doc_type='discussion_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)[num*page:num*page+num]
        else:
            query = self.es_query(index='tap2_test', doc_type='discussion_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)[num*page:num*page+num]

        return query

@route('/table/enroll_overview')
class EnrollDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tap2_test', doc_type='enroll_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)[num*page:num*page+num]
        else:
            query = self.es_query(index='tap2_test', doc_type='enroll_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)[num*page:num*page+num]

        return query
