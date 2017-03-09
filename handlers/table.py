#! -*- coding: utf-8 -*-
import time
from utils.log import Log
from utils.routes import route
from elasticsearch_dsl import Q
from .base import BaseHandler

Log.create('student')

class TableHandler(BaseHandler):

    def get_query(self, course_id, page, num, sort, sort_type, student_keyword,fields):
        pass

    def post(self):
        student_keyword = self.get_argument('student_keyword')
        time_begin = time.time()
        page = int(self.get_argument('page', 0))
        num = int(self.get_argument('num', 10))
        sort = self.get_argument('sort', None)
        sort_type = int(self.get_argument('sort_type', 0))
        fields = self.get_argument('fields', '')
        fields = fields.split(',') if fields else []
        course_id = self.course_id
        user_ids = self.get_users()
        
        
        #查询改课程的owner
        query_owner = self.es_query(doc_type='course_community')\
                            .filter('term', course_id=course_id)
        owner = self.es_execute(query_owner[:1]).hits[0].owner

        query = self.get_query(course_id, user_ids, page, num, sort, sort_type, student_keyword, fields)
        if fields:
            query = query.source(fields)
        
        if student_keyword != "":
            query = query.filter(Q('bool', should=[Q('wildcard', rname='*%s*' % student_keyword)\
                                                  | Q('wildcard', binding_uid='*%s*'% student_keyword) \
                                                  | Q('wildcard', nickname='*%s*' % student_keyword)\
                                                  | Q('wildcard', xid='*%s*' % student_keyword)\
                                                  ]))
        size = self.es_execute(query[:0]).hits.total

        if num == -1:
            times = size/10000
            data = self.es_execute(query[:10000])
            result = [item.to_dict() for item in data.hits]
            for i in range(times+1):
                if i != 0:
                    result = result + [item.to_dict() for item in self.es_execute(query[i*10000:(i+1)*10000]).hits]
        else:
            data = self.es_execute(query[page*num:(page+1)*num])
            result = [item.to_dict() for item in data.hits]

        final = {}
        final['total'] = size
        final['data'] = result
        time_elapse = time.time() - time_begin
        final['time'] = "%.0f" % (float(time_elapse) * 1000)
        final['owner'] = owner

        self.success_response(final)

@route('/table/grade_overview')
class GradeDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, student_keyword, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tapgrade', doc_type='grade_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)
        else:
            query = self.es_query(index='tapgrade', doc_type='grade_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)
        return query

@route('/table/question_overview')
class QuestionDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, student_keyword, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tapgrade', doc_type='question_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)
        else:
            query = self.es_query(index='tapgrade', doc_type='question_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)

        return query

@route('/table/video_overview')
class VideoDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, student_keyword, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tapvideo', doc_type='video_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)
        else:
            query = self.es_query(index='tapvideo', doc_type='video_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)

        return query

@route('/table/discussion_overview')
class DiscussionDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, student_keyword,fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tapforum', doc_type='discussion_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)
        else:
            query = self.es_query(index='tapforum', doc_type='discussion_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)

        return query

@route('/table/enroll_overview')
class EnrollDetail(TableHandler):
    def get_query(self, course_id, user_ids, page, num, sort, sort_type, student_keyword,fields):
        user_ids.extend(self.get_users(is_active=False))

        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tapforum', doc_type='enroll_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .sort(sort)
        else:
            query = self.es_query(index='tapforum', doc_type='enroll_overview') \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)

        return query
