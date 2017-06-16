#! -*- coding: utf-8 -*-
from tornado import gen
from utils.log import Log
from utils.routes import route
from utils import study_progress 
from utils.tools import fix_course_id
from .base import BaseHandler
import MySQLdb
from MySQLdb.cursors import DictCursor


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
        if not course_id or not user_id:
            self.error_response(502, u'缺少参数')
        course_id = fix_course_id(course_id)
        db,cursor= MysqlConnect().get_db_cursor()
        
        sp = study_progress.StudyProgress(thrift_server='10.0.2.132', namespace='heartbeat')
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
        watched_num, watched_duration = sp.get_study_progress(user_id, course_id, items)
        sp.close()
        db.close()
        data = {'watched_num': watched_num, 'watched_duration': watched_duration}
        self.success_response({'data': data})


@route('/mobile/mobile_user_study')
class MobileUserStudy(BaseHandler):

    def get(self):
        user_id = self.get_argument('user_id', None)

        sp = study_progress.StudyProgress(thrift_server='10.0.2.132', namespace='heartbeat')
        result = sp.get_user_watched_video(user_id)
        
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
        sp = study_progress.StudyProgress(thrift_server='10.0.2.132', namespace='heartbeat')
        seq_video_rate_dict = sp.get_study_progress_not_class_level(user_id, course_id, seq_items)

        # combine all these info
        result = []
        for seq_id in set(seq_items.values()):
            seq = {}
            seq['id'] = seq_id
            watched_percent = seq_video_rate_dict.get(seq_id, {}).get('watched_duration', 0) / float(video_durations.get(seq_id, 1e-10))
            if watched_percent >= 0.975:
                watched_percent = 1.
            seq['watched_percent'] = round(watched_percent, 2)
            seq['watched_num'] = seq_video_rate_dict.get(seq_id, {}).get('watched_num', 0)
            seq['video_length'] = int(video_durations.get(seq_id, 0))
            result.append(seq)
            
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
        sp = study_progress.StudyProgress(thrift_server='10.0.2.132', namespace='heartbeat')
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
        sp = study_progress.StudyProgress(thrift_server='10.0.2.132', namespace='heartbeat')
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
        sp = study_progress.StudyProgress(thrift_server='10.0.2.132', namespace='heartbeat')
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

