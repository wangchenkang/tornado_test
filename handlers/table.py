#! -*- coding: utf-8 -*-
import time
from utils.log import Log
from utils.routes import route
from .base import BaseHandler

Log.create('student')

class TableHandler(BaseHandler):

    def get_query(self, page, num, sort, sort_type, fields):
        pass

    def post(self):
        time_begin = time.time()
        page = int(self.get_argument('page', 0))
        num = int(self.get_argument('num', 10))
        sort = self.get_argument('sort', None)
        sort_type = int(self.get_argument('sort_type', 0))
        fields = self.get_argument('fields', '')
        fields = fields.split(',') if fields else []

        query = self.get_query(page, num, sort, sort_type, fields)
        if fields:
            query = query.source(fields)

        size = self.es_execute(query[:0]).hits.total
        data = self.es_execute(query)

        final = {}
        result = [item.to_dict() for item in data.hits]
        final['total'] = size
        final['data'] = result
        time_elapse = time.time() - time_begin
        final['time'] = "%.0f" % (float(time_elapse) * 1000)

        self.success_response(final)


@route('/table/grade_detail')
class GradeDetail(TableHandler):
    def get_query(self, page, num, sort, sort_type, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tap', doc_type='grade_overview') \
            .filter('term', course_id=self.course_id).sort(sort)[num*page:num*page+num]
        else:
            query = self.es_query(index='tap', doc_type='grade_overview') \
            .filter('term', course_id=self.course_id)[num*page:num*page+num]

        return query


@route('/table/video_detail')
class ProblemDetail(TableHandler):
    def get_query(self, page, num, sort, sort_type, fields):
        if sort:
            reverse = True if sort_type else False
            sort = '-' + sort if reverse else sort
            query = self.es_query(index='tap', doc_type='video_study') \
            .filter('term', course_id=self.course_id).sort(sort)[num*page:num*page+num]
        else:
            query = self.es_query(index='tap', doc_type='video_study') \
            .filter('term', course_id=self.course_id)[num*page:num*page+num]

        return query

