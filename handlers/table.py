#! -*- coding: utf-8 -*-
import time
from utils.log import Log
from utils.routes import route
from elasticsearch_dsl import Q
from .base import BaseHandler
import settings

Log.create('student')

class TableHandler(BaseHandler):

    def student_search(self, course_id, student_keyword):
        query = self.es_query(index=settings.ES_INDEX, doc_type='student_enrollment_info') \
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
    
    def get_study_warning_num(self):
        query = self.es_query(doc_type='study_warning_person')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .filter('terms', user_id=self.get_users())
        total = self.es_execute(query).hits.total
        return total

    def post(self):
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
        total = len(user_ids)
        #NEED
        if 'warning_date' in fields:
            total = self.get_study_warning_num()
        final = {}
        final['total'] = total
        final['data'] = result

        self.success_response(final)


class TableJoinHandler(TableHandler):

    GRADE_FIELDS = ['grade', 'letter_grade', 'current_grade_rate', 'total_grade_rate']
    USER_FIELDS = ['user_id', 'nickname', 'xid', 'rname', 'binding_uid', 'faculty', 'major']
    SEEK_FIELDS = ['is_watch', 'is_seek', 'not_watch_total', 'seek_total', 'video_open_num']
    WARNING_FIELDS = ['warning_date', 'study_week', 'least_2_week', 'low_video_rate', 'low_grade_rate']

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
        result = []
        for idx, es_index_type in enumerate(es_index_types):
            es_index, es_type = es_index_type.split('/')
            if idx == 0 and es_type == 'study_warning_person':
                query = self.es_query(index=es_index, doc_type=es_type)\
                            .filter('term', course_id=course_id)\
                            .filter('term', group_key=self.group_key)
                query.aggs.bucket('user_ids', 'terms', field='user_id', size=len(user_ids))
                aggs = self.es_execute(query).aggregations
                buckets = aggs.user_ids.buckets 
                user_ids = [bucket.key for bucket in buckets]
            query = self.es_query(index=es_index, doc_type=es_type) \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .source(fields)

            if es_type == 'student_enrollment_info' or es_type == 'study_warning_person':
                query = query.filter('term', group_key=self.group_key)
            
            # 如果是第一个查询，需要排序，查询后更新学生列表
            if idx == 0:
                query = query.sort(sort)
                query = query[page*num:(page+1)*num]
            else:
                query = query[:len(user_ids)]

            data = self.es_execute(query)
            data_result = [item.to_dict() for item in data.hits]
            
            if es_type == 'study_warning_person' and len(data_result) == 0:
                result = []
                continue
            # 如果是第一个查询，在查询后更新user_ids列表，后续查询只查这些学生
            if idx == 0:
                result.extend(data_result)
                user_ids = [r['user_id'] for r in result]
            else:
                data_result_dict = {}
                for row in data_result:
                    data_result_dict[row['user_id']] = row
                for r in result:
                    dr = data_result_dict.get(r['user_id'], {})
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

        result = self.postprocess(result)

        return result

    def postprocess(self, result):
        return result


@route('/table/grade_overview')
class GradeDetail(TableJoinHandler):

    es_types = ['tap_table_grade/grade_summary', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field):
        if sort_field in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return 'tap_table_grade/grade_summary'


@route('/table/question_overview')
class QuestionDetail(TableJoinHandler):

    es_types = ['tap_table_question/chapter_question', 'tap_table_small_question/small_question', \
                '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field):
        if sort_field in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        elif '_answer' in sort_field or '_correct' in sort_field:
            return 'tap_table_small_question/small_question'
        return 'tap_table_question/chapter_question'

    def postprocess(self, result):
        for record in result:
            for key in record:
                if key.endswith('_answer') and len(record[key]) > 20:
                    record[key] = record[key][:20] + '...' 
        return result


@route('/table/video_overview')
class VideoDetail(TableJoinHandler):

    es_types = ['tap_table_video/chapter_seq_video', 'tap_table_video/item_video', \
                '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field):
        if sort_field in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        elif 'item_' in sort_field:
            return 'tap_table_video/item_video'
        return 'tap_table_video/chapter_seq_video'


@route('/table/discussion_overview')
class DiscussionDetail(TableJoinHandler):

    es_types = ['tap_table_discussion/discussion_summary', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field):
        if sort_field in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return 'tap_table_discussion/discussion_summary'


@route('/table/enroll_overview')
class EnrollDetail(TableJoinHandler):

    es_types = ['tap_table_enroll/enroll_summary', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field):
        if sort_field in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return 'tap_table_enroll/enroll_summary'

    def postprocess(self, result):
        for record in result:
            if 'enroll_time' in record and record['enroll_time']:
                record['enroll_time'] = record['enroll_time'][:10]
            if 'unenroll_time' in record and record['unenroll_time']:
                record['unenroll_time'] = record['unenroll_time'][:10]
        return result

@route('/table/seek_video')
class SeekVideoTable(TableJoinHandler):

    es_types = ['%s/video_seek_summary' % settings.ES_INDEX, '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort):
        if sort in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return '%s/video_seek_summary' % settings.ES_INDEX
                            
@route('/table/study_warning')
class Studywarning(TableJoinHandler):
                                
    es_types = ['%s/study_warning_person' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]
    
    def get_es_type(self, sort_field):
        if sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return '%s/study_warning_person' % settings.ES_INDEX

@route('/small_question_structure')
class SmallQuestionStructure(BaseHandler):
    """
    返回课程的小题顺序，描述
    """
    def get(self):
        query = self.es_query(index='tap_table_small_question', doc_type='small_question_desc') \
                    .filter('term', course_id=self.course_id)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        result = {item.compact_order_id: item.to_dict() for item in data.hits}
        self.success_response({'data': result})

