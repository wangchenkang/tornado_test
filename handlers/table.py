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

    #def post(self):
    def get(self):
        student_keyword = self.get_argument('student_keyword', '')
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
        #query_owner = self.es_query(doc_type='course_community')\
        #                    .filter('term', course_id=course_id)
        #owner = self.es_execute(query_owner[:1]).hits[0].owner

        result, size = self.get_query(course_id, user_ids, page, num, sort, sort_type, student_keyword, fields)
        #if fields:
        #    query = query.source(fields)

        # student search
        #if student_keyword != "":
        #    query = query.filter(Q('bool', should=[Q('wildcard', rname='*%s*' % student_keyword)\
        #                                          | Q('wildcard', binding_uid='*%s*'% student_keyword) \
        #                                          | Q('wildcard', nickname='*%s*' % student_keyword)\
        #                                          | Q('wildcard', xid='*%s*' % student_keyword)\
        #                                          ]))

        #size = self.es_execute(query[:0]).hits.total

        # for download
        #if num == -1:
        #    times = size/10000
        #    data = self.es_execute(query[:10000])
        #    result = [item.to_dict() for item in data.hits]
        #    for i in range(times+1):
        #        if i != 0:
        #            result = result + [item.to_dict() for item in self.es_execute(query[i*10000:(i+1)*10000]).hits]
        #else:
        #    data = self.es_execute(query[page*num:(page+1)*num])
        #    result = [item.to_dict() for item in data.hits]

        final = {}
        final['total'] = size
        final['data'] = result
        time_elapse = time.time() - time_begin
        final['time'] = "%.0f" % (float(time_elapse) * 1000)
        #final['owner'] = owner

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

    def get_es_type(self, sort_field):
        if 'answer' in sort_field or 'correct' in sort_field:
            return 'small_question/small_question'
        return 'tap7_test/question_overview'

    def get_query_plan(self, sort):
        es_types = ['tap7_test/question_overview', 'small_question/small_question']
        first_es_type = self.get_es_type(sort)
        es_types.remove(first_es_type)
        es_types.insert(0, first_es_type)
        return es_types

    def get_query(self, course_id, user_ids, page, num, sort, sort_type, student_keyword, fields):
        import ipdb
        ipdb.set_trace()
        if not sort:
            sort = 'grade'

        es_index_types = self.get_query_plan(sort)

        reverse = True if sort_type else False
        sort = '-' + sort if reverse else sort

        result = []
        size = 0
        for idx, es_index_type in enumerate(es_index_types):
            es_index, es_type = es_index_type.split('/')
            query = self.es_query(index=es_index, doc_type=es_type) \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids)
            # 如果是第一个查询，需要把size查出来，前端用于分页
            if idx == 1:
                query = query.sort(sort)
                size = self.es_execute(query[:0]).hits.total
                query = query[page*num:(page+1)*num]

            data = self.es_execute(query)
            data_result = [item.to_dict() for item in data.hits]

            # 如果是第一个查询，在查询后更新user_ids列表，后续查询只查这些学生
            if idx == 1:
                result = data_result
                user_ids = [r['user_id'] for r in result]
            else:
                for r in result:
                    for dr in data_result:
                        if r['user_id'] == dr['user_id']:
                            r.extends(dr)

        ipdb.set_trace()
        return result, size

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

class SmallQuestionStructure(BaseHandler):
    """
    返回课程的小题顺序，描述
    """
    def get(self):
        query = self.es_query(index='question_desc', doc_type='question_desc') \
                    .filter('term', course_id=self.course_id)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = [{item.compact_order_id: item.to_dict()} for item in data.hits]
        self.success_response({'data': result})

