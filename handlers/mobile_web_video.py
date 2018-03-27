#! -*- coding: utf-8 -*-
from tornado import gen
from utils.log import Log
from utils.routes import route
from utils import study_progress 
from utils.tools import fix_course_id
from .base import BaseHandler
import MySQLdb
from MySQLdb.cursors import DictCursor
import json
import datetime 
import time
from time import sleep
import settings

Log.create('mobile_web_video')


class MysqlConnect(object):
    
    def __init__(self):
        self.host= '10.0.0.247'
        self.db= 'vms'
        self.user= 'vmsselect'
        self.password= 'readonly'

    def get_db(self):
        db = MySQLdb.connect(self.host, self.user, self.password, self.db)
        db.ping(True)
        return db

    def convert_decimal_int(self, decimal_num):
        if not decimal_num:
            return 0
        return int(decimal_num)
    
    def get_db_cursor(self):
        db = MysqlConnect().get_db()
        cursor = db.cursor(cursorclass=DictCursor)
        return db,cursor

@route('/mobile/mobile_web_video_course')
class MobileWebCourse(BaseHandler):
    
    def get(self):
        db,cursor= MysqlConnect().get_db_cursor()
        course_id = self.get_argument("course_id", None)
        if not course_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        query = """
                select count(vi.id) as video_num, sum(vi.duration) as total_duration from course_video cv join video_info vi on cv.video_id = vi.id where cv.course_id='{0}' and cv.item_id != 'intro_video'
                """.format(course_id)
        cursor.execute(query)
        results = cursor.fetchall()
        results = [{'video_num': result['video_num'], 'total_durtaion': MysqlConnect().convert_decimal_int(result['total_duration'])} for result in results]
        db.close()
        self.success_response({"data": results[0]})

@route('/mobile/mobile_web_video_chapter')
class MobileWebVideoChapter(BaseHandler):
    
    def get(self):
        db,cursor= MysqlConnect().get_db_cursor()
        course_id = self.get_argument('course_id', None)
        chapter_id = self.get_argument('chapter_id', None)
        if not course_id or not chapter_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        query = """
                select count(vi.id) as video_num, sum(vi.duration) as total_duration from course_video cv join video_info vi on cv.video_id = vi.id where cv.course_id='{0}' and chapter_id='{1}'
                """.format(course_id, chapter_id)
        cursor.execute(query)
        results = cursor.fetchall()
        results = [{'video_num': result['video_num'], 'total_durtaion': MysqlConnect().convert_decimal_int(result['total_duration'])} for result in results]
        db.close()
        self.success_response({"data": results[0]})

@route('/mobile/mobile_web_video_vertical')
class MobileWebVideoVertical(BaseHandler):

    def get(self):
        db,cursor= MysqlConnect().get_db_cursor()
        course_id = self.get_argument('course_id', None)
        vertical_id = self.get_argument('vertical_id',None)
        if not course_id or not vertical_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        query = """
                select count(vi.id) as video_num, sum(vi.duration) as total_duration from course_video cv join video_info vi on cv.video_id = vi.id where cv.course_id='{0}' and vertical_id= '{1}'
                """.format(course_id, vertical_id)
        cursor.execute(query)
        results = cursor.fetchall()
        results = [{'video_num': result['video_num'], 'total_durtaion': MysqlConnect().convert_decimal_int(result['total_duration'])} for result in results]
        db.close()
        self.success_response({"data": results[0]})

@route('/mobile/mobile_web_video_sequential')
class MobileWebVideoSequencial(BaseHandler):

    def get(self):
        db,cursor= MysqlConnect().get_db_cursor()
        course_id = self.get_argument('course_id', None)
        sequential_id = self.get_argument('sequential_id', None)
        if not course_id or not sequential_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        query = """
                select count(vi.id) as video_num, sum(vi.duration) as total_duration from course_video cv join video_info vi on cv.video_id = vi.id where cv.course_id='{0}' and sequential_id= '{1}'
                """.format(course_id, sequential_id)
        cursor.execute(query)
        results = cursor.fetchall()
        results = [{'video_num': result['video_num'], 'total_durtaion': MysqlConnect().convert_decimal_int(result['total_duration'])} for result in results]
        db.close()
        self.success_response({"data": results[0]})


@route('/mobile/mobile_web_video_item')
class MobileWebVideoItem(BaseHandler):

    def get(self):
        db,cursor= MysqlConnect().get_db_cursor()
        course_id = self.get_argument('course_id', None)
        item_id = self.get_argument('item_id', None)
        if not course_id or not item_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        query = """
                select count(vi.id) as video_num, sum(vi.duration) as total_duration from course_video cv join video_info vi on cv.video_id = vi.id where cv.course_id='{0}' and item_id= '{1}'
                """.format(course_id, item_id)
        cursor.execute(query)
        results = cursor.fetchall()
        results = [{'video_num': result['video_num'], 'total_durtaion': MysqlConnect().convert_decimal_int(result['total_duration'])} for result in results]
        db.close()
        self.success_response({"data": results[0]})


@route('/mobile/mobile_web_study_progress')
class MobileWebStudyProgress(BaseHandler):

    def get(self): 
        course_id = self.get_argument('course_id', None)
        user_id = self.get_argument('user_id', None)
        chapter_id = self.get_argument('chapter_id', None)
        sequential_id = self.get_argument('sequential_id', None)
        vertical_id = self.get_argument('vertical_id', None)
        day = self.get_argument('day', '')
        if not course_id or not user_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        db,cursor= MysqlConnect().get_db_cursor()
        
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        query_items = None
        items = None
        if chapter_id:
            query_items = """
                            select item_id as items from course_video where course_id='{0}' and chapter_id='{1}'
                          """.format(course_id, chapter_id)
        elif sequential_id:
            query_items = """
                            select item_id as items from course_video where course_id='{0}' and sequential_id='{1}'
                          """.format(course_id, sequential_id)
        elif vertical_id:
            query_items = """
                            select item_id as items from course_video where course_id='{0}' and vertical_id='{1}'
                         """.format(course_id, vertical_id)
        else:
            query_items = """
                            select item_id as items from course_video where course_id='{0}'
                         """.format(course_id)
        if query_items:
            results = cursor.execute(query_items)
            results = cursor.fetchall()
            items = [result['items'] for result in results]
        watched_num, watched_duration = sp.get_study_progress(user_id, course_id, items=items, day=day)
        sp.close()
        db.close()
        data = {'watched_num': watched_num, 'watched_duration': watched_duration}
        self.success_response({'data': data})


@route('/mobile/mobile_study_progress')
class MobileStudyProgress(BaseHandler):

    def get(self): 
        course_id = self.get_argument('course_id', None)
        user_id = self.get_argument('user_id', None)
        chapter_id = self.get_argument('chapter_id', None)
        sequential_id = self.get_argument('sequential_id', None)
        vertical_id = self.get_argument('vertical_id', None)
        day = self.get_argument('day', '')
        if not course_id or not user_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        
        query = self.moocnd_es_query(index=settings.MOOCND_ES_INDEX, doc_type='chapter_seq_video') \
                    .filter('term', course_id = course_id) \
                    .filter('term', user_id = user_id)  \
                    .filter('range', **{'watched_duration': {'gt': 0}}) 
        if day:
            query = query.filter('range', **{'date': {'lte': day}})
        size = self.es_execute(query[:0]).hits.total
        if size == 0: size=1 
        if chapter_id:
            query = query.filter('term', chapter_id = chapter_id)
        elif sequential_id:
            query = query.filter('term', sequential_id = sequential_id)
        elif vertical_id:
            query = query.filter('term', vertical_id = vertical_id)
        
        query.aggs.bucket('video_ids', 'terms', field='video_id', size=size)
        query.aggs.metric('watched_durations', 'sum', field='watched_duration')
        query.aggs.metric('video_lengths', 'sum', field='video_length')

        response = self.es_execute(query)
        aggs = response.aggregations
        buckets = aggs.video_ids.buckets
        watched_num = len(buckets)
        watched_duration = int(aggs.watched_durations.value)
        video_lengths = int(aggs.video_lengths.value)
        if watched_duration > video_lengths:watched_duration=video_lengths
        update_time = self.get_moocnd_update_time()
        data = {'watched_num': watched_num, 'watched_duration': watched_duration, 'update_time': update_time}
        
        self.success_response({'data': data})

@route('/mobile/mobile_user_rank')
class MobileUserRank(BaseHandler):

    def get(self):
        user_id = self.get_argument('user_id', None)
        day = self.get_argument('day', '')
        query = self.moocnd_es_query(index=settings.MOOCND_ES_INDEX, doc_type='chapter_seq_video') \
                    .filter('term', user_id = user_id) \
                    .filter('range', **{'watched_duration': {'gt': 0}})
        if day:
            query = query.filter('range', **{'date': {'lte': day}})
        size = self.es_execute(query[:0]).hits.total 
        if size ==0: size=1
        query.aggs.bucket('course_ids', 'terms', field='course_id', size=size)
        query.aggs.metric('watched_durations', 'sum', field='watched_duration')
        query.aggs.metric('video_lengths', 'sum', field='video_length')
        response = self.es_execute(query)
        aggs = response.aggregations
        buckets = aggs.course_ids.buckets
        watched_courses = len(buckets)
        watched_duration = int(aggs.watched_durations.value)
        video_lengths = int(aggs.video_lengths.value)
        if watched_duration > video_lengths:watched_duration=video_lengths
        update_time = self.get_moocnd_update_time()
        result = {'watched_duration': watched_duration, 'watched_courses': watched_courses, 'update_time': update_time} 
        
        self.success_response({'data': result})

@route('/mobile/mobile_user_study')
class MobileUserStudy(BaseHandler):

    def get(self):
        user_id = self.get_argument('user_id', None)
        day = self.get_argument('day', '')
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        result = sp.get_user_watched_video(user_id, day)
        
        self.success_response({'data': result})


@route('/mobile/mobile_user_study_by_course')
class MobileUserStudyByCourse(BaseHandler):

    def get(self):
        user_id = self.get_argument('user_id', None)
        final = []
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        course_study = sp.get_user_watched_video_by_course(user_id)
        course_ids = course_study.keys()

        # get all enroll course
        query = self.es_query(index='enrollment', doc_type='enroll_student') \
            .filter('term', user_id=user_id)
        result = self.es_execute(query)
        result = self.es_execute(query[:result.hits.total])
        for r in result:
            if r['course_id'] not in course_ids:
                course_ids.append(r['course_id'])
        if not course_ids:
            self.success_response({'data': final})

        db,cursor= MysqlConnect().get_db_cursor()
        # get sequential durations
        sql = """
             SELECT course_id, sum(vi.duration) AS course_duration 
             FROM course_video cv 
             JOIN video_info vi ON cv.video_id = vi.id 
             WHERE cv.course_id in (%s)
             GROUP BY cv.course_id
              """ % ','.join(['"%s"' % course_id for course_id in course_ids])
        cursor.execute(sql)
        course_durations = cursor.fetchall()
        course_durations_d = {}
        for row in course_durations:
            course_durations_d[row['course_id']] = row['course_duration']

        for course_id in course_ids:
            record = {}
            record['course_id'] = course_id
            record['total_length'] = int(course_durations_d.get(course_id, 0))
            record['watched_length'] = int(course_study.get(course_id, 0))
            final.append(record)

        self.success_response({'data': final})


@route('/mobile/mobile_web_study_progress_item_detail')
class MobileWebStudyProgressItemDetail(BaseHandler):

    @gen.coroutine
    def get(self):
        user_id = self.get_argument('user_id', None)
        course_id = self.get_argument('course_id', None)
        course_id = fix_course_id(course_id)
        if not course_id or not user_id:
            self.error_response({100, u'error'})

        # get video durations
        sql = """
             SELECT cv.item_id as vid, vi.duration as dur
             FROM course_video cv
             JOIN video_info vi ON cv.video_id = vi.id
             WHERE cv.course_id='%s'
              """ % course_id
        db,cursor= MysqlConnect().get_db_cursor()
        cursor.execute(sql)
        video_durations = cursor.fetchall()
        video_durations_d = {}
        for video in video_durations:
            video_durations_d[video['vid']] = video['dur']
        
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        result = sp.get_video_progress_detail(user_id, course_id, video_durations_d)

        self.success_response({'data': result})


@route('/mobile/mobile_web_study_progress_item')
class MobileWebStudyProgressItem(BaseHandler):

    @gen.coroutine
    def get(self):
        user_id = self.get_argument('user_id', None)
        course_id = self.get_argument('course_id', None)
        if not course_id or not user_id:
            self.error_response({'data': []})
        course_id = fix_course_id(course_id)
        db,cursor= MysqlConnect().get_db_cursor()

        # get all video
        sql = """
              SELECT item_id
              FROM course_video
              WHERE course_id='%s'
              """ % (course_id)
        cursor.execute(sql)
        video_items = cursor.fetchall()
        video_items = [video['item_id'] for video in video_items]
        video_items_d = {video:video for video in video_items}

        # get video durations
        sql = """
             SELECT cv.item_id as vid, vi.duration as dur
             FROM course_video cv
             JOIN video_info vi ON cv.video_id = vi.id
             WHERE cv.course_id='%s'
              """ % course_id
        cursor.execute(sql)
        video_durations = cursor.fetchall()
        video_durations_d = {}
        for video in video_durations:
            video_durations_d[video['vid']] = video['dur']

        # get student seq video duration
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        video_rate_dict = sp.get_study_progress_not_class_level(user_id, course_id, video_items_d)

        # combine all these info
        result = []
        for video_id in video_items:
            video = {}
            video['video_id'] = video_id
            video['video_length'] = video_durations_d.get(video_id, 0)
            video['watched_length'] = video_rate_dict.get(video_id, {}).get('watched_duration', 0)
            video['watched_percent'] =  round(video['watched_length'] / float(video_durations_d.get(video_id, 1e-10)), 4)
            if video['watched_percent'] > 1:
                video['watched_percent'] = 1.
            result.append(video)

        self.success_response({'data': result})

@route('/mobile/mobile_web_study_progress_detail')
class MobileWebStudyProgressDetail(BaseHandler):

    def get(self): 
        user_id = self.get_argument('user_id', None)
        course_id = self.get_argument('course_id', None)
        if not course_id or not user_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        db,cursor= MysqlConnect().get_db_cursor()

        # get course structure
        sql = """
              SELECT sequential_id, item_id
              FROM course_video 
              WHERE course_id='%s' 
              """ % (course_id)
        cursor.execute(sql)
        course_structure = cursor.fetchall()

        video_rate = {}
        seq_items = {}
        for row in course_structure:
            seq_id = row['sequential_id']
            item_id = row['item_id']
            video_rate[seq_id] = 0
            seq_items[item_id] = seq_id

        # get sequential durations
        sql = """
             SELECT sequential_id, count(vi.id) AS seq_video_num, sum(vi.duration) AS seq_duration 
             FROM course_video cv 
             JOIN video_info vi ON cv.video_id = vi.id
             WHERE cv.course_id='%s'
             GROUP BY cv.sequential_id
              """ % course_id
        cursor.execute(sql)
        sequential_durations = cursor.fetchall()
        video_durations = {}
        for row in sequential_durations:
            video_durations[row['sequential_id']] = row['seq_duration']

        # get student seq video duration
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        seq_video_rate_dict = sp.get_study_progress_not_class_level(user_id, course_id, seq_items)

        # combine all these info
        result = []
        for seq_id in set(seq_items.values()):
            seq = {}
            seq['id'] = seq_id
            watched_percent = seq_video_rate_dict.get(seq_id, {}).get('watched_duration', 0) / float(video_durations.get(seq_id, 1e-10))
            if watched_percent >= 0.95:
                watched_percent = 1.
            seq['watched_percent'] = round(watched_percent, 2)
            seq['watched_num'] = seq_video_rate_dict.get(seq_id, {}).get('watched_num', 0)
            seq['video_length'] = int(video_durations.get(seq_id, 0))
            result.append(seq)

        self.success_response({'data': result})

@route('/mobile/mobile_web_study_progress_chapter_detail')
class MobileWebStudyProgressChapterDetail(BaseHandler):

    def get(self): 
        user_id = self.get_argument('user_id', None)
        course_id = self.get_argument('course_id', None)
        if not course_id or not user_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        db,cursor= MysqlConnect().get_db_cursor()

        # get course structure
        sql = """
              SELECT chapter_id, item_id
              FROM course_video
              WHERE course_id='%s'
              """ % (course_id)
        cursor.execute(sql)
        course_structure = cursor.fetchall()

        video_rate = {}
        chapter_items = {}
        for row in course_structure:
            chapter_id = row['chapter_id']
            item_id = row['item_id']
            video_rate[chapter_id] = 0
            chapter_items[item_id] = chapter_id

        # get chapter durations
        sql = """
             SELECT chapter_id, count(vi.id) AS chapter_video_num, sum(vi.duration) AS chapter_duration 
             FROM course_video cv
             JOIN video_info vi ON cv.video_id = vi.id
             WHERE cv.course_id='%s'
             GROUP BY cv.chapter_id
              """ % course_id
        cursor.execute(sql)
        chapter_durations = cursor.fetchall()
        video_durations = {}
        for row in chapter_durations:
            video_durations[row['chapter_id']] = row['chapter_duration']

        # get student chapter video duration
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        chapter_video_rate_dict = sp.get_study_progress_not_class_level(user_id, course_id, chapter_items)

        # combine all these info
        result = []
        for chapter_id in set(chapter_items.values()):
            chapter = {}
            chapter['id'] = chapter_id
            watched_percent = chapter_video_rate_dict.get(chapter_id, {}).get('watched_duration', 0) / float(video_durations.get(chapter_id, 1e-10))
            if watched_percent >= 0.95:
                watched_percent = 1.
            chapter['watched_percent'] = round(watched_percent, 2)
            chapter['watched_num'] = chapter_video_rate_dict.get(chapter_id, {}).get('watched_num', 0)
            chapter['video_length'] = int(video_durations.get(chapter_id, 0))
            result.append(chapter)

        self.success_response({'data': result})



@route('/mobile/mobile_web_video_last_pos')
class MobileWebVideoLastPos(BaseHandler):

    def get(self): 
        course_id = self.get_argument('course_id', None)
        user_id = self.get_argument('user_id', None)
        item_id = self.get_argument('item_id', None)
        if not course_id or not user_id or not item_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        current_point = sp.get_video_last_pos(user_id, course_id, item_id)
        sp.close()
        self.success_response({'data': {'current_point': current_point}})


@route('/mobile/mobile_web_course_last_pos')
class MobileWebCourseLastPos(BaseHandler):

    def get(self): 
    
        course_id = self.get_argument('course_id', None)
        user_id = self.get_argument('user_id', None)
        if not course_id or not user_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        result = {}
        result['course_id'] = course_id
        result['item'] = ''
        result['cur_pos'] = ''
        if not sp.get_course_last_pos(user_id, course_id):
            self.success_response(result)
            return
        else:
            course_id, item, cur_pos = sp.get_course_last_pos(user_id, course_id)
            result['item'] = item
            result['cur_pos'] = cur_pos
        sp.close()
        self.success_response({'data': result})


@route('/mobile/mobile_web_user_last_pos')
class MobileWebUserLastPos(BaseHandler):

    def get(self): 
        user_id = self.get_argument('user_id', None)
        if not user_id:
            self.error_response(502, u'缺少参数')
        sp = study_progress.StudyProgress(thrift_server_list=settings.THRIFT_SERVER, namespace='heartbeat')
        result = {}
        result['course_id'] = ''
        result['item'] = ''
        result['cur_pos'] = ''
        if not sp.get_user_last_pos(user_id):
            self.success_response(result)
            return
        else:
            course_id, item, cur_pos = sp.get_user_last_pos(user_id)
            result['course_id'] = course_id
            result['item'] = item
            result['cur_pos'] = cur_pos
        sp.close()
        self.success_response({'data': result})


@route('/mobile/learning_guide') # url
class LearningGuide(BaseHandler): 

    def get(self): 

        user_id = self.get_argument('user_id', None) # get parameter
        course_ids = self.get_argument('course_id', None)# get parameter
        if not user_id or not course_ids: 
            self.error_response(502, u'缺少参数') # error
        course_id_list = course_ids.split(',')

        #课程结构查询
        str_result = {}
        for course_id in course_id_list:
            course_id  = fix_course_id(course_id)
            result = self.course_structure(course_id, 'course', depth=4)
            course_end = result['end'] if result else ''
            chapters = result['children'] if result else []
            for chapter in chapters:
                metadata = chapter['metadata']
                is_exam = metadata['is_exam']
                seqs = chapter['children']
                for seq in seqs:
                    seq_type = seq['block_type'] #sequential
                    me = seq['metadata']
                    seq_end = me.get('due',course_end)
                    exam_end = me.get('exam_end',course_end)
                    verticals = seq['children']
                    str_result_one={}
                    str_result_one['course_id'] = course_id
                    str_result_one['chapter_id'] = chapter['block_id']
                    str_result_one['seq_id'] = seq['block_id']
                    str_result_one['chapter_start'] =chapter['start']
                    if seq_type=='sequential' and is_exam==True :
                        if seq_end is not None:
                            end = datetime.datetime.strptime(seq_end, "%Y-%m-%dT%H:%M:%S")
                            stri = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                            interval = end - datetime.datetime.strptime(stri,'%Y-%m-%dT%H:%M:%S')
                            if interval.days<=2 and interval.days>=0:
                                str_result_one['is_exam'] = True
                                str_result_one['seq_end'] = exam_end
                                str_result[seq['block_id']] = str_result_one
                    else:
                        for vertical in verticals:
                            items = vertical['children']
                            for item in items:
                                item_id = item['block_id']
                                item_type = item['block_type']
                                if seq_end is not None:
                                    end = datetime.datetime.strptime(seq_end, "%Y-%m-%dT%H:%M:%S")
                                    stri = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                                    interval = end - datetime.datetime.strptime(stri,'%Y-%m-%dT%H:%M:%S')

                                    if interval.days <= 2 and interval.days >= 0 and item_type == 'problem':
                                        str_result_one['is_exam'] = False
                                        str_result_one['seq_end'] = seq_end
                                        str_result[item_id] = str_result_one
        course_ids = []
        for course_id in course_id_list:
            course_ids.append(course_id)

        query = self.es_query(index = 'learning_records',doc_type = 'learning_guide')\
                    .filter('term',student_id=user_id) \
                    .filter('terms',course_id=course_ids)
        total = self.es_execute(query).hits.total
        result = self.es_execute(query[:total]).hits
        item_ids = []
        for row in result:
            item_ids.append(row.item_id)

        result_final = {}
        time_list = []
        if len(str_result) != 0:
            for key,value in str_result.items(): #str_result是查询课程结构的结果{item_id:{course_id:,chapter_id:,chapter_name:,seq_id:,seq_name:,vertical_id:,seq_end:,is_exam:,chapter_start:}}
                result_d = {}
                result_d['chapter_id'] = value['chapter_id']
                result_d['seq_id'] = value['seq_id']
                result_d['seq_end'] = value['seq_end']
                time_list.append(value['seq_end'])
                result_d['chapter_start'] = value['chapter_start']
                result_d['is_exam'] = value['is_exam']
                if key not in item_ids: #未提交的记录
                    if result_final.has_key(value['course_id']):
                        k = value['course_id']
                        data = result_final[k]#已经存在的
                        if value['is_exam'] == False and data['is_exam'] == True:#如果已经存在的记录是考试，后来的记录是作业
                            result_final[k]['is_exam'] = value['is_exam']
                            result_final[k]['seq_end'] = value['seq_end']
                        if  value['is_exam'] == False and data['is_exam'] == False:#如果已经存在的是作业，后来的记录是作业
                            s_end = datetime.datetime.strptime(value['seq_end'], '%Y-%m-%dT%H:%M:%S')
                            seq_end = datetime.datetime.strftime(s_end,'%Y-%m-%d %H:%M:%S')
                            ss = time.mktime(time.strptime(seq_end,'%Y-%m-%d %H:%M:%S'))
                            d_end = datetime.datetime.strptime(data['seq_end'],'%Y-%m-%dT%H:%M:%S')
                            ss_end = datetime.datetime.strftime(d_end,'%Y-%m-%d %H:%M:%S')
                            dd = time.mktime(time.strptime(ss_end,'%Y-%m-%d %H:%M:%S'))
                            if ss<dd:
                                result_final[k]['seq_end'] = value['seq_end']
                        if  value['is_exam'] == True and data['is_exam'] == True:#如果已经存在的是考试，后来的记录是考试
                            s_end = datetime.datetime.strptime(value['seq_end'], '%Y-%m-%dT%H:%M:%S')
                            seq_end = datetime.datetime.strftime(s_end,'%Y-%m-%d %H:%M:%S')
                            ss = time.mktime(time.strptime(seq_end,'%Y-%m-%d %H:%M:%S'))
                            d_end = datetime.datetime.strptime(data['seq_end'],'%Y-%m-%dT%H:%M:%S')
                            ss_end = datetime.datetime.strftime(d_end,'%Y-%m-%d %H:%M:%S')
                            dd = time.mktime(time.strptime(ss_end,'%Y-%m-%d %H:%M:%S'))
                            if ss<dd:
                                result_final[k]['seq_end'] = value['seq_end']


                    else:
                        c_id = value['course_id']
                        result_final[c_id] = result_d
        result_list = []
        for k,v in result_final.items():
            one = {}
            one['id'] = k
            if v['is_exam'] == False:
                one['remind_type'] = 'homework'
            elif v['is_exam'] == True:
                one['remind_type'] = 'exam'
            else:
                one['remind_type'] = -1
            one['deadline'] = v['seq_end']
            result_list.append(one)
        result_course_list = []#提示考试或者作业的课程id
        for key,value in result_final.items():
            result_course_list.append(key)
        #course_id_list请求传进来的课
        for co in course_id_list:
            if fix_course_id(co) not in result_course_list:
                result = self.course_structure(fix_course_id(co), 'course', depth=4)
                course = result['children']
                max_start = None
                result_one = {'id': fix_course_id(co),'remind_type':'chapter'}
                for c in course:#针对一门课的每一章
                    name = c['display_name']
                    result_one['name'] = name
                    chapter_id = c['block_id']
                    chapter_start =  c['start']
                    if chapter_start > max_start :
                        max_start = chapter_start
                    result_one['deadline'] = max_start
                result_list.append(result_one)
        self.success_response({'data': result_list})

 #答题记录
@route('/mobile/learning_history')
class LearningHistory(BaseHandler):

    def get(self):
        user_id = self.get_argument('user_id', None)  # get parameter
        course_ids = self.get_argument('course_id', None) # get parameter
        #参数校验
        if not user_id or not course_ids:
            self.error_response(502, u'缺少参数')  # error
        course_id_list = course_ids.split(',')
        # 课程结构查询
        str_result = {}
        for course_id in course_id_list:
            course_id = fix_course_id(course_id)
            item_list = []
            result = self.course_structure(course_id, 'course', depth=4)
            chapters = result['children']
            #print 'course%s',course
            for chapter in chapters:
                chapter_id = chapter['block_id']
                chapter_name = chapter['display_name']
                seqs = chapter['children']
                for seq in seqs:
                    seq_id = seq['block_id']
                    seq_name = seq['display_name']
                    verticals = seq['children']
                    for vertical in verticals:
                        vertical_id = vertical['block_id']
                        items = vertical['children']
                        #item_list = []
                        for item in items:
                            item_id = item['block_id']
                            #item_list = []
                            item_list.append(item_id)
            str_result[course_id] = item_list
        # es查询
        course_ids = []
        for course_id in course_id_list:
            course_ids.append(fix_course_id(course_id))
        query = self.es_query(index='learning_records', doc_type='learning_guide') \
                    .filter('term', student_id=user_id) \
                    .filter('terms', course_id=course_ids)
        total = self.es_execute(query).hits.total
        result = self.es_execute(query[:total]).hits
        info = []
        for row in result:
            info.append(row.item_id)
        result_list = []#[{course_id:,chapter_id:,chapter_name:,seq_id:,seq_name},{},...,{}]
        for key, value in str_result.items():#key：course_id value:[item_id,item_id,...]
            #对于每一门课
            result = {'has_submitted': False, 'id': key}
            for v in value:#课程结构   中的item
                if v in info:#提交过
                    result['has_submitted'] = True
                    continue
            result_list.append(result)

        self.success_response({'data': result_list})


#课程about页访问量
@route('/mobile/page_about')
class PageAbout(BaseHandler):
    """
    """
    def post(self):
        course_ids = json.loads(self.get_argument('course_id'))
        start_date = json.loads(self.get_argument('start_date'))
        times = 2
        retry_times = 0
        sleep_time = 0.5
        if not course_ids and start_date:
            self.error_respnse(100, u'参数错误')
        pathes = []
        for course_id in course_ids:
            path = "/courses/%s/about" % fix_course_id(course_id)
            pathes.append(path.encode('utf-8'))
        from elasticsearch import Elasticsearch
        es = Elasticsearch(['10.0.0.227', '10.0.0.228', '10.0.0.236'], timeout=5)
        body = {\
              "query":{\
                "filtered":{\
                "query":{\
                  "match_all":{}\
                  },\
                  "filter":{\
                    "and":{\
                      "filters":\
                      [\
                        {\
                          "term":{\
                            "value_type":"visits"\
                            }\
                          },\
                        {\
                          "term":{\
                            "key":"L8"\
                            }\
                          },\
                        {\
                          "terms":{\
                            "path": pathes\
                            }\
                          },\
                        {\
                          "term":{\
                            "cycle":"daily"\
                            }\
                          },\
                        {\
                          "range":{\
                            "start_date":{\
                              "gte":start_date\
                              }\
                            }\
                          }\
                        ]\
                      }\
                    }\
                  }\
                },\
                "aggs":{\
                  "courses":{\
                    "terms":{\
                      "field":"path"\
                      },\
                    "aggs":{\
                      "visits":{\
                        "sum":{\
                          "field":"value"\
                          }\
                        }\
                      }\
                    }\
                  }\
                }
        while True:
            try:
                response = es.search(index='opkpi', doc_type='page_access_pv_uv', search_type= 'count', body = body)
                break
            except Exception as e:
                if retry_times <= times:
                  if retry_times == times:
                        break
                retry_times += 1
                sleep(sleep_time)
                continue
        data = []
        for bucket in response['aggregations']['courses']['buckets']:
            course_about = {}
            course_about['course_id'] = bucket['key'].rsplit('/')[2]
            course_about['num'] = int(bucket['visits']['value'] or 0)
            course_about['date'] = start_date
            data.append(course_about)
        self.success_response({'data': data})
