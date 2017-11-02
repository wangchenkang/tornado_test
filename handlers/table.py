#! -*- coding: utf-8 -*-

import time
from tornado.web import gen
from utils.routes import route
from utils.tools import is_ended
from elasticsearch_dsl import Q
from .base import BaseHandler
import settings
import json


class TableHandler(BaseHandler):

    def student_search(self, course_id, group_key, student_keyword, data_type, status):
        # enroll表格临时逻辑
        if data_type in ('enroll', 'video', 'discussion') and not status:
            index = settings.TAP_REALTIME_INDEX
        else:
            index = settings.ES_INDEX

        query = self.es_query(index=index, doc_type='student_enrollment_info') \
                    .filter('term', course_id=course_id) \
                    .filter('term', group_key=group_key)
        if data_type != 'enroll':
            query=query.filter('term', is_active=1)

        if student_keyword:
                    query = query.filter(Q('bool', should=[Q('wildcard', rname='*%s*' % student_keyword)\
                                              | Q('wildcard', binding_uid='*%s*'% student_keyword) \
                                              | Q('wildcard', nickname='*%s*' % student_keyword)\
                                              | Q('wildcard', xid='*%s*' % student_keyword)\
                                              | Q('wildcard', faculty='*%s*' % student_keyword)\
                                              | Q('wildcard', major='*%s*' % student_keyword)\
                                              | Q('wildcard', the_class='*%s*' % student_keyword)\
                                              | Q('wildcard', entrance_year='*%s*' % student_keyword)
                                              ]))
        size = self.es_execute(query[:0]).hits.total
        result = self.es_execute(query[:size]).hits
        user_ids = [r.user_id for r in result]
        return user_ids

    def get_warning_total(self):
        query = self.es_query(index='problems_focused',doc_type='study_warning_person')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .filter('terms', user_id=self.get_users())
        total = self.es_execute(query).hits.total
        return total

    @gen.coroutine
    def post(self):
        course_detail = yield self.course_detail(self.course_id)
        end_time = course_detail['end']
        status = is_ended(end_time)

        student_keyword = self.get_argument('student_keyword', None)
        time_begin = time.time()
        page = int(self.get_argument('page', 0))
        num = int(self.get_argument('num', 10))
        sort = self.get_argument('sort', None)
        sort = 'grade' if not sort else sort
        sort_type = int(self.get_argument('sort_type', 0))
        data_type = self.get_argument('data_type')
        fields = self.get_argument('fields', '')
        screen_index = self.get_argument('screen_index', '')
        fields = json.loads(fields) if fields else []
        screen_index = json.loads(screen_index) if screen_index else []
        user_ids = self.student_search(self.course_id, self.group_key, student_keyword, data_type, status)
        # enroll表临时逻辑
        if data_type == 'enroll' and sort == 'grade':
            sort = 'enroll_time'
        if sort == 'grade' and data_type == 'warning':
            sort = 'study_week'
        if sort == 'grade' and data_type == 'newcloud_grade':
            sort = 'final_score'
        result = yield self.search_es(self.course_id, user_ids, page, num, sort, sort_type, fields, screen_index, data_type)
        if screen_index:
            user_ids = self.get_filter_user_ids(self.course_id, user_ids, data_type, screen_index)
        total = len(user_ids)
        if data_type == 'warning':
            total = self.get_warning_total() if not screen_index else total
        final = {}
        final['total'] = total
        final['data'] = result

        self.success_response(final)


class TableJoinHandler(TableHandler):

    GRADE_FIELDS = ['grade', 'letter_grade', 'current_grade_rate', 'total_grade_rate']
    USER_FIELDS = ['user_id', 'nickname', 'xid', 'rname', 'binding_uid', 'faculty', 'major', 'the_class', 'entrance_year']
    SEEK_FIELDS = ['is_watch', 'is_seek', 'not_watch_total', 'seek_total', 'video_open_num']
    WARNING_FIELDS = ['warning_date', 'study_week', 'least_2_week', 'low_video_rate', 'low_grade_rate']
    NEWCLOUD_GRADE_FIELDS = ['has_passed', 'final_score', 'edx_score', 'video_score', 'post_score', 'import_score']

    def get_es_type(self, sort_field, status):
        pass

    @gen.coroutine
    def get_query_plan(self, sort):
        course_detail = yield self.course_detail(self.course_id)
        end_time = course_detail['end']
        status = is_ended(end_time)

        first_es_type = self.get_es_type(sort, status)
        self.es_types.remove(first_es_type)
        self.es_types.insert(0, first_es_type)

        raise gen.Return(self.es_types)

    def iterate_search(self, es_index_types, course_id, user_ids, page, num, sort, fields, screen_index, data_type):
        if 'user_id' not in fields:
            fields.append('user_id')
        result = []
        if screen_index:
            user_ids = self.get_filter_user_ids(course_id, user_ids, data_type, screen_index)
        for idx, es_index_type in enumerate(es_index_types):
            es_index, es_type = es_index_type.split('/')
            if idx == 0 and es_type == 'study_warning_person':
                user_ids = self.get_warning_user_ids(course_id, es_index, es_type, user_ids)

            query = self.es_query(index=es_index, doc_type=es_type) \
                        .filter('term', course_id=course_id) \
                        .filter('terms', user_id=user_ids) \
                        .source(fields)
           
            #不同的group_key下，user_id是一样的
            if es_type in ('study_warning_person', 'student_enrollment_info'):
                query = query.filter('term', group_key=self.group_key)
            # 如果是第一个查询，需要排序，查询后更新学生列表
            if idx == 0:
                query = query.sort(sort)
                query = query[page*num:(page+1)*num]
            else:
                query = query[:len(user_ids)]
            data = self.es_execute(query)
            data_result = [item.to_dict() for item in data.hits]
            
            if es_type == 'score_realtime':
                for item in data_result:
                    item['user_id'] = str(item.pop('user_id'))

            if es_type == 'score_realtime' and (len(data_result) < num):
                data_result_user_ids = []
                for item in data_result:
                    data_result_user_ids.append(item['user_id'])
                for user_id in user_ids[page*num:(page+1)*num]:
                    if user_id not in data_result_user_ids:
                        data_result.append({'user_id': user_id})

            if es_type == 'score_realtime' and num == 10000:
                data_result_user_ids = []
                for item in data_result:
                    data_result_user_ids.append(item['user_id'])
                for user_id in user_ids:
                    if user_id not in data_result_user_ids:
                        data_result.append({'user_id': user_id})

            # 如果是第一个查询，在查询后更新user_ids列表，后续查询只查这些学生
            if idx == 0:
                result.extend(data_result)
                user_ids = [r['user_id'] for r in data_result]
            else:
                data_result_dict = {}    
                for row in data_result:
                    data_result_dict[row['user_id']] = row
                for r in result:
                    dr = data_result_dict.get(r['user_id'], {})
                    r.update(dr)
        
        return result


    def iterate_download(self, es_index_types, course_id, user_ids, sort, fields, screen_index, data_type, part_num=10000):
        num = len(user_ids)
        times = num / part_num
        if num % part_num:
            times += 1
        result = []
        for i in range(times):
            result.extend(self.iterate_search(es_index_types, course_id, user_ids, i, part_num, sort, fields, screen_index, data_type))
        return result 

    @gen.coroutine
    def search_es(self, course_id, user_ids, page, num, sort, sort_type, fields, screen_index, data_type):
        es_index_types = yield self.get_query_plan(sort)

        reverse = True if sort_type else False
        sort = '-' + sort if reverse else sort
        if num == -1:
            result = self.iterate_download(es_index_types, course_id, user_ids, sort, fields, screen_index, data_type)
        else:
            result = self.iterate_search(es_index_types, course_id, user_ids, page, num, sort, fields, screen_index, data_type)

        result = self.postprocess(result)

        raise gen.Return(result)

    def postprocess(self, result):
        return result
 
    def newcloud_grade_filter(self, course_id, user_ids, screen_index, total):
        """
        新学堂云成绩过滤
        """
        query = self.es_query(index=settings.NEWCLOUD_ES_INDEX, doc_type='score_realtime')\
                    .filter('term', course_id=course_id)\
                    .filter('terms', user_id=user_ids)
        for item in screen_index:
            query = query.filter('range', **{item['field']: {'gte': item['min'] or 0, 'lte': item['max'] or 100}})
        user_ids = [item.user_id for item in self.es_execute(query[:total]).hits]
        return user_ids

    def course_grade_filter(self, course_id, user_ids, screen_index, total):
        """
        共同点得分，得分率
        """
        query = self.es_query(doc_type='course_grade')\
                    .filter('term', course_id=course_id)\
                    .filter('terms', user_id=user_ids)
        for item in screen_index:
            if item['field'] == 'grade':
                query = query.filter('range', **{item['field']: {'gte': item['min'] or 0, 'lte': item['max'] or 100}})
            else:
                query = query.filter('range', **{item['field']: {'gte': float(item['min'] or 0 )/100, 'lte': float(item['max'] or 100 )/100}})
        user_ids = [item.user_id for item in self.es_execute(query[:total]).hits]
        return user_ids

    def row_filter(self, course_id, user_ids, screen_index, total, query):
        """
        各个独立指标过滤相应学生
        """
        course_grade_fields = []
        status = False

        for i in screen_index:
            if i['field'] in self.GRADE_FIELDS:
                course_grade_fields.append(i)
            else:
                status = True
                if i['field'] in ['low_grade_rate', 'low_video_rate', 'study_rate']:
                    query = query.filter('range', **{i['field']: {'gte': float(i['min'] or 0) /100, 'lte': float(i['max'] or 100) /100}})
                else:
                    query = query.filter('range', **{i['field']: {'gte': i['min'] or 0, 'lte': i['max'] or 100}})

        return course_grade_fields, status, query

    def get_filter_user_ids(self, course_id, user_ids, data_type, screen_index):
        """
        根据不同的type,过滤相应学生
        """
        total = 50000 if len(user_ids) > 50000 else len(user_ids)
        es = settings.ROW_FILTER.get(data_type, None)
        if not es:
            es_index = settings.ES_INDEX
            es_doc_type = 'course_grade'
        else:
            es_index = es['index']
            es_doc_type = es['doc_type']
        if data_type == 'newcloud_grade':
            user_ids = self.newcloud_grade_filter(course_id, user_ids, screen_index, total)
        else:
            query = self.es_query(index=es_index, doc_type=es_doc_type)\
                    .filter('term', course_id=course_id)\
                    .filter('terms', user_id=user_ids)
            course_grade_fields, status, query = self.row_filter(course_id, user_ids, screen_index, total, query)
            course_grade_filter_user_ids =  self.course_grade_filter(course_id, user_ids, course_grade_fields, total) if course_grade_fields else user_ids
            other_filter_user_ids = [item.user_id for item in self.es_execute(query[:total]).hits] if status else user_ids
            user_ids = list(set(course_grade_filter_user_ids).intersection(set(other_filter_user_ids)))
        return user_ids

    def get_warning_user_ids(self, course_id, es_index, es_type, user_ids):
        """
        学业预警单拿出来筛选学生
        """
        query = self.es_query(index=es_index, doc_type=es_type)\
                    .filter('term', course_id=course_id)\
                    .filter('term', group_key=self.group_key)\
                    .filter('terms', user_id=user_ids)
        query.aggs.bucket('user_ids', 'terms', field='user_id', size=len(user_ids) or 1)
        aggs = self.es_execute(query).aggregations
        buckets = aggs.user_ids.buckets 
        user_ids = [bucket.key for bucket in buckets]
        return user_ids

@route('/table/grade_overview')
class GradeDetail(TableJoinHandler):

    es_types = ['tap_table_grade/grade_summary', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field, status):
        if status:
            self.es_types = ['tap_table_grade_lock/grade_summary', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]
        if sort_field in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX

        if status:
            return 'tap_table_grade_lock/grade_summary'
        else:
            return 'tap_table_grade/grade_summary'


@route('/table/question_overview')
class QuestionDetail(TableJoinHandler):

    es_types = ['tap_table_question/chapter_question', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]
    #es_types = ['tap_table_question/chapter_question', 'tap_table_small_question/small_question', \
    #            '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field, status):
        if status:
            self.es_types = ['tap_table_question_lock/chapter_question', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]
        if sort_field in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        elif '_answer' in sort_field or '_correct' in sort_field:
            return 'tap_table_small_question/small_question'

        if status:
            return 'tap_table_question_lock/chapter_question'
        else:
            return 'tap_table_question/chapter_question'

    def postprocess(self, result):
        for record in result:
            for key in record:
                if key.endswith('_answer') and len(record[key]) > 20:
                    record[key] = record[key][:20] + '...' 
        return result


@route('/table/video_overview')
class VideoDetail(TableJoinHandler):
    es_types = ['tap_table_video_realtime/chapter_seq_video', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.TAP_REALTIME_INDEX]
    #es_types = ['tap_table_video/chapter_seq_video', 'tap_table_video/item_video', \
    #            '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort_field, status):
        if status:
            self.es_types = ['tap_table_video/chapter_seq_video', '%s/course_grade' % settings.ES_INDEX_LOCK, '%s/student_enrollment_info' % settings.ES_INDEX_LOCK]
     
        if sort_field in self.GRADE_FIELDS:
            if status:
                return '%s/course_grade' % settings.ES_INDEX_LOCK
            else:
                return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            if status:
                return '%s/student_enrollment_info' % settings.ES_INDEX_LOCK
            else:
                return '%s/student_enrollment_info' % settings.TAP_REALTIME_INDEX

        if 'item_' in sort_field:
            return 'tap_table_video/item_video'
        
        if status:
            return 'tap_table_video/chapter_seq_video'
        else:
            return 'tap_table_video_realtime/chapter_seq_video'


@route('/table/discussion_overview')
class DiscussionDetail(TableJoinHandler):

    es_types = ['realtime_discussion_table/realtime_discussion_data', '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.TAP_REALTIME_INDEX]

    def get_es_type(self, sort_field, status):
        if status:
            self.es_types = ['tap_table_discussion_lock/discussion_summary', '%s/course_grade' % settings.ES_INDEX_LOCK, '%s/student_enrollment_info' % settings.ES_INDEX_LOCK]
        
        if sort_field in self.GRADE_FIELDS:
            if status:
                return '%s/course_grade' % settings.ES_INDEX_LOCK
            else:
                return '%s/course_grade' % settings.ES_INDEX
        elif sort_field in self.USER_FIELDS:
            if status:
                return '%s/student_enrollment_info' % settings.ES_INDEX_LOCK
            else:
                return '%s/student_enrollment_info' % settings.TAP_REALTIME_INDEX

        if status:
            return 'tap_table_discussion_lock/discussion_summary'
        else: 
            return 'realtime_discussion_table/realtime_discussion_data'


@route('/table/enroll_overview')
class EnrollDetail(TableJoinHandler):

    es_types = ['realtime/student_enrollment_info']

    def get_es_type(self, sort_field, status):
        if status:
            self.es_types = ['tap_lock/student_enrollment_info']
            return 'tap_lock/student_enrollment_info'
        else:
            return 'realtime/student_enrollment_info'

    def postprocess(self, result):
        for record in result:
            if 'enroll_time' in record and record['enroll_time']:
                record['enroll_time'] = record['enroll_time'][:10]
            if 'unenroll_time' in record and record['unenroll_time']:
                record['unenroll_time'] = record['unenroll_time'][:10]
        return result

@route('/table/seek_video')
class SeekVideoTable(TableJoinHandler):

    es_types = ['%s/video_seek_summary' % ('problems_focused'), '%s/course_grade' % settings.ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort, status):
        if sort in self.GRADE_FIELDS:
            return '%s/course_grade' % settings.ES_INDEX
        elif sort in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return '%s/video_seek_summary' % ('problems_focused')
                            
@route('/table/study_warning')
class Studywarning(TableJoinHandler):
                                
    es_types = ['%s/study_warning_person' %('problems_focused'), '%s/student_enrollment_info' % settings.ES_INDEX]
    
    def get_es_type(self, sort, status):
        if sort in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return '%s/study_warning_person' % ('problems_focused')

@route('/table/newcloud_grade')
class NewcloudGrade(TableJoinHandler):
    es_types = ['%s/score_realtime' % settings.NEWCLOUD_ES_INDEX, '%s/student_enrollment_info' % settings.ES_INDEX]

    def get_es_type(self, sort, status):
        if sort in self.USER_FIELDS:
            return '%s/student_enrollment_info' % settings.ES_INDEX
        return '%s/score_realtime' % settings.NEWCLOUD_ES_INDEX

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


@route('/data/update_time')
class UpdateTime(BaseHandler):
    """
    """
    def get(self):
        data_type = self.get_argument('data_type')
        if data_type == 'enroll':
            query = self.es_query(index='processstate', doc_type='processstate') \
                        .filter('term', topic='edxapp')
            data = self.es_execute(query[:1])
            update_time = data[0].current_time.replace('T', ' ')
        elif data_type == 'video':
            query = self.es_query(index = 'processstate', doc_type = 'processstate')\
                        .filter('term', data_type = 'realtime_table_video')
            data = self.es_execute(query[:1])
            update_time = data[0].current_time.replace('T', ' ').split('+')[0]

        elif data_type == 'discussion':
            query = self.es_query(index = 'processstate', doc_type = 'processstate')\
                        .filter('term', data_type = 'realtime_discussion_data')
            data = self.es_execute(query[:1])
            update_time = data[0].current_time.replace('T', ' ').split('+')[0]
        elif data_type in ('grade', 'question'):
            query = self.es_query(index = 'tap', doc_type = 'data_conf')
            data = self.es_execute(query[:1])
            update_time = '%s 23:59:59' % data[0].latest_data_date

        self.success_response({'data': {'update_time': update_time}})
