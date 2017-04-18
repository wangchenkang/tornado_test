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

    def student_search(self, course_id, student_keyword):
        query = self.es_query(index='tapgo', doc_type='student_enrollment_info') \
                    .filter('term', course_id=course_id) \
                    .filter(Q('bool', should=[Q('wildcard', rname='*%s*' % student_keyword)\
                                              | Q('wildcard', binding_uid='*%s*'% student_keyword) \
                                              | Q('wildcard', nickname='*%s*' % student_keyword)\
                                              | Q('wildcard', xid='*%s*' % student_keyword)\
                                              ]))

        size = self.es_execute(query[:0]).hits.total
        result = self.es_execute(query[:size]).hits
        user_ids = [r.user_id for r in result]
        return user_ids

    def search_es(self, course_id, user_ids, page, num, sort, sort_type, student_keyword, fields):
        query = self.get_query(course_id, user_ids, page, num, sort, sort_type, student_keyword, fields)
        if fields:
            query = query.source(fields)

        if num == -1:
            result = self.download(query, len(user_ids))
        else:
            data = self.es_execute(query[page*num:(page+1)*num])
            result = [item.to_dict() for item in data.hits]

        return result

    def download(self, query, size, part_num=10000):
        times = size/part_num
        if size % part_num:
            times += 1
        result = []
        for i in range(times):
            items = self.es_execute(query[i*part_num:(i+1)*part_num]).hits
            result.extend([item.to_dict() for item in items])
        return result

    #def post(self):
    def get(self):
        student_keyword = self.get_argument('student_keyword', '')
        time_begin = time.time()
        page = int(self.get_argument('page', 0))
        num = int(self.get_argument('num', 10))
        sort = self.get_argument('sort', 'grade')
        sort_type = int(self.get_argument('sort_type', 0))
        fields = self.get_argument('fields', '')
        fields = fields.split(',') if fields else []
        course_id = self.course_id

        if student_keyword:
            user_ids = self.student_search(course_id, student_keyword)
        else:
            user_ids = self.get_users()

        result = self.search_es(course_id, user_ids, page, num, sort, sort_type, student_keyword, fields)

        final = {}
        final['total'] = len(user_ids)
        final['data'] = result

        self.success_response(final)


class NewTableHandler(TableHandler):

    def get_es_type(self, sort_field):
        pass

    def get_query_plan(self, sort):
        first_es_type = self.get_es_type(sort)
        self.es_types.remove(first_es_type)
        self.es_types.insert(0, first_es_type)
        return self.es_types

    def iterate_search(self, es_index_types, course_id, user_ids, page, num, sort, fields):
        if 'user_id' not in fields:
            fields.append('user_id')
        for idx, es_index_type in enumerate(es_index_types):
            es_index, es_type = es_index_type.split('/')
            query = self.es_query(index=es_index, doc_type=es_type) \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .source(fields)

            # 如果是第一个查询，需要排序，查询后更新学生列表
            if idx == 0:
                query = query.sort(sort)
                query = query[page*num:(page+1)*num]
            else:
                query = query[:len(user_ids)]

            data = self.es_execute(query)
            data_result = [item.to_dict() for item in data.hits]

            # 如果是第一个查询，在查询后更新user_ids列表，后续查询只查这些学生
            if idx == 0:
                result = data_result
                user_ids = [r['user_id'] for r in result]
            else:
                for r in result:
                    for dr in data_result:
                        if r['user_id'] == dr['user_id']:
                            r.update(dr)
        return result

    def iterate_download(self, es_index_types, course_id, user_ids, sort, fields, part_num=10000):
        num = len(user_ids)
        times = num / part_num
        if num % part_num:
            times += 1
        result = []
        for i in range(times):
            result.extend(self.iterate_search(es_index_types, course_id, user_ids, i, part_num, sort, fields))
        return result 

    def search_es(self, course_id, user_ids, page, num, sort, sort_type, student_keyword, fields):
        es_index_types = self.get_query_plan(sort)

        reverse = True if sort_type else False
        sort = '-' + sort if reverse else sort

        if num == -1:
            result = self.iterate_download(es_index_types, course_id, user_ids, sort, fields)
        else:
            result = self.iterate_search(es_index_types, course_id, user_ids, page, num, sort, fields)

        return result


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
class QuestionDetail(NewTableHandler):

    es_types = ['tap7_test/question_overview', 'small_question/small_question', 'course_grade/course_grade', 'tapgo/student_enrollment_info']

    def get_es_type(self, sort_field):
        if 'grade' in sort_field:
            return 'course_grade/course_grade'
        elif 'answer' in sort_field or 'correct' in sort_field:
            return 'small_question/small_question'
        return 'tapgo/student_enrollment_info'


@route('/table/video_overview')
class VideoDetail(NewTableHandler):

    es_types = ['video_overview/video_overview', 'item_video/item_video', 'course_grade/course_grade', 'tapgo/student_enrollment_info']

    def get_es_type(self, sort_field):
        if 'grade' in sort_field:
            return 'course_grade/course_grade'
        if 'item' in sort_field:
            return 'item_video/item_video'
        return 'tapgo/student_enrollment_info'

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

@route('/small_question_structure')
class SmallQuestionStructure(BaseHandler):
    """
    返回课程的小题顺序，描述
    """
    def get(self):
        query = self.es_query(index='question_desc', doc_type='question_desc') \
                    .filter('term', course_id=self.course_id)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {item.compact_order_id: item.to_dict() for item in data.hits}
        self.success_response({'data': result})

