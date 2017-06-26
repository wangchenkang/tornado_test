#! -*- coding: utf-8 -*-
from tornado import gen
from utils.log import Log
from utils.routes import route
from utils import study_progress 
from utils.tools import fix_course_id
from utils.service import CourseService
from .base import BaseHandler
import MySQLdb
from MySQLdb.cursors import DictCursor
<<<<<<< HEAD
import json
import datetime

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


@route('/learning/guide') # url
class MobileDemo(BaseHandler): 

    def get(self): 
        user_id = self.get_argument('user_id', None) # get parameter
        course_id_list = fix_course_id(self.get_argument('course_id', None)) # get parameter
        if not user_id: 
            self.error_response(502, u'缺少参数') # error
        #course_id_list = self.get_argument('course_id', None)
        if not course_id_list:
            self.error_response(502,u'缺少参数')
        course_id_ll = course_id_list.split(',')
        #课程结构查询
        str_result = {}
        for course_id in course_id_ll:
            course_id = course_id.encode('utf-8')
            result = self.course_structure(course_id, 'course', depth=4)
            course = result['children']
            for c in course:
                chapter_id = c['block_id'].encode('utf-8')
                ##chapter_name = c['display_name']
                seq = c['children']
                for s in seq:
                    seq_id = s['block_id'].encode('utf-8')
                    ##seq_name = s['display_name']
                    ver = s['children']
                    for v in ver:
                        vertical_id = v['block_id'].encode('utf-8')
                        item = v['children']
                        for i in item:
                            item_id = i['block_id'].encode('utf-8')
                            #is_exam = i['is_exam']
                            #chapter_start = i['chpater_start']
                            if i['seq_end'] is not None:
                                end_str = i['seq_end'].encode('utf-8')
                                end = datetime.datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S")
                            stri = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                            interval = end - datetime.datetime.strptime(stri,'%Y-%m-%dT%H:%M:%S')
                            if i['seq_end'] is not None:
                                seq_end = i['seq_end'].encode('utf-8')
                            else:
                                seq_end = ''
                           # interval=1
                            if interval.days < 2 and interval.days > 0:
                            #if interval < 2 and interval>0:
                                str_result_one = {}
                                str_result_one['course_id'] = course_id
                                str_result_one['chapter_id'] = chapter_id
                                ##str_result_one['chapter_name'] = chapter_name
                                str_result_one['seq_id'] = seq_id
                                ##str_result_one['seq_name'] = seq_name
                                str_result_one['vertical_id'] = vertical_id
                                #str_result_one['seq_end'] = seq_end
                                #str_result_one['is_exam'] = is_exam
                                # str_result_one['chapter_start'] =chapter_start
                                #不要时间差，只要到截止时间还有未提交的作业类型就是homework，没有未提交的作业还有未提交的考试类型就是exam，要不然就是章（是发布时间）
                                str_result[item_id] = str_result_one
                                #print str_result
       
        #print str_result                     
        #es查询        
        cid = []
        for course_id in course_id_ll:
            cid.append(course_id)
        print user_id   #url传过来的用户
        print cid       #url传过来的课程id
        query = self.es_query(index = 'learning_guide',doc_type = 'learning_guide')\
               .filter('term',student_id=user_id) \
               .filter('terms',course_id=cid) 
        result = self.es_execute(query)
        #print result.hits
        info = []
        for row in result.hits:
            info.append(row.item_id)
        print info  #es里面提交过的item_id
        result_final = {}
        #history = {}
        for key,value in str_result.items(): #str_result是查询课程结构的结果{item_id:{course_id:,chapter_id:,chapter_name:,seq_id:,seq_name:,vertical_id:,seq_end:,is_exam:,chapter_start:}}
            result_d = {}
            if key not in info:
                if result_final.has_key(value['course_id']):
                    k = value['course_id']
                    data = result_final[k]#原来就有的
                    
                    if value['interval'] < data['interval']:
                        
                        ch_id = value['chapter_id']
                        ##ch_name = value['chapter_name']
                        s_id = value['seq_id']
                        ##s_name = value['seq_name']
                        seq_end = value['seq_end']
                        #ch_start = value['chapter_start']
                        #is_exam = value['is_exam']
                        result_d['chapter_id'] = ch_id
                        ##result_d['chapter_name'] = ch_name
                        result_d['seq_id'] = s_id
                        ##result_d['seq_name'] = s_name
                        result_d['seq_end'] = seq_end
                        #result_d['chapter_start'] = ch_start
                        #result_d['is_exam'] = is_exam
                        result_final[k] = result_d
                    else:
                        pass

                else:
                    c_id = value['course_id']
                    ch_id = value['chapter_id']
                    ##ch_name = value['chapter_name']
                    s_id = value['seq_id']
                    ##s_name = value['seq_name']
                    seq_end = value['end']
                    inte = value['interval']
                    #ch_start = value['chapter_start']
                    #is_exam = value['is_exam']
                    result_d['chapter_id'] = ch_id
                    ##result_d['chapter_name'] = ch_name
                    result_d['seq_id'] = s_id
                    ##result_d['seq_name'] = s_name
                    result_d['seq_end'] = seq_end
                    result_d['interval'] = inte
                    #result_d['chapter_start'] = ch_start
                    #result_d['is_exam'] = is_exam
                    result_final[c_id] = result_d
            #else:
                #答过题的记录
                # history[key] = value #答过题的记录

        if len(result_final) == 0:  #全部考试作业都提交过
            for course in cid:
                pass
                 #对每一门课调接口，拿出章的发布时间，根据章的发布时间作排序

            # chapter_list = []
            # for k,v in history.items():
            #     chapter_list.append(v)
            # #course_id没有做区分
            # so = lambda s:s['chapter_start']
            # chapter_list.sort(so)
            # ch_d = chapter_list[-1]
            # c_id = ch_d['course_id']
            # result_final[c_id] = ch_d

            #章发布的时间最近的一章
        ##if not user_id: 
        ##    self.error_response(502, u'缺少参数') # error
     
        ##if not course_id:
        ##    self.error_response(502,u'缺少参数')
        result = self.course_structure(course_id, block_id='course', depth=4)

        # mysql
        #MysqlConnect

        # ES
        # self.es_query
        # return data
        self.success_response({'data': result_final})

 #答题记录
@route('/learning/history')
class LearningHistory(BaseHandler):

    def get(self):
        user_id = self.get_argument('user_id', None)  # get parameter
        course_id_list = fix_course_id(self.get_argument('course_id', None)) # get parameter
        #参数校验
        if not user_id:
            self.error_response(502, u'缺少参数')  # error
        if not course_id_list:
            self.error_response(502, u'缺少参数')
        course_id_ll = course_id_list.split(',')
        # 课程结构查询
        str_result = {}
        for course_id in course_id_ll:
            course_id = course_id.encode('utf-8')
            item_list = []
            result = self.course_structure(course_id, 'course', depth=4)
            course = result['children']
            for c in course:
                chapter_id = c['block_id'].encode('utf-8')
                chapter_name = c['display_name']
                seq = c['children']
                for s in seq:
                    seq_id = s['block_id'].encode('utf-8')
                    seq_name = s['display_name']
                    ver = s['children']
                    for v in ver:
                        vertical_id = v['block_id'].encode('utf-8')
                        item = v['children']
                        #item_list = []
                        for i in item:
                            item_id = i['block_id'].encode('utf-8')
                            #item_list = []
                            item_list.append(item_id)
            str_result[course_id] = item_list
        print str_result
        # es查询
        cid = []
        for course_id in course_id_ll:
            cid.append(fix_course_id(course_id))
        print user_id
        print cid
        query = self.es_query(index='learning_guide', doc_type='learning_guide') \
            .filter('term', student_id=user_id) \
            .filter('terms', course_id=cid)
        result = self.es_execute(query)
        info = []
        for row in result.hits:
            info.append(row.item_id)
        print info
        result_list = []#[{course_id:,chapter_id:,chapter_name:,seq_id:,seq_name},{},...,{}]
        for key, value in str_result.items():#key：course_id value:[item_id,item_id,...]
            #对于每一门课
            result = {'has_submitted': False, 'id': key}
            for v in value:#课程结构   中的item
                if v in info:#提交过
                    result['has_submitted'] = True
                    continue
            result_list.append(result)

        #if len(result_list) < len(cid):
        #    for c in course_id_ll:
        #        result_false = {}
        #        sub_list = []
        #        print len(result_list)
        #        for result_one in result_list:   
        #            course_id = result_one['id']
        #            sub_list.append(course_id)
        #            if c not in sub_list:
        #                result_false['id'] = c
        #                result_false['has_submitted'] = False
        #    result_list.append(result_false) 

        self.success_response({'data': result_list})
                # c_id = value['course_id']
                # ch_id = value['chapter_id']
                # ch_name = value['chapter_name']
                # seq_id = value['seq_id']
                # seq_name = value['seq_name']
                # #is_exam = value['is_exam']
                # k = c_id + seq_id
                # one['chapter_id'] = ch_id
                # one['chapter_name'] = ch_name
                # one['seq_id'] = seq_id
                # one['seq_name'] = seq_name
                # one['is_exam']
                # one['course_id'] = c_id
                # result[k] = one
                # print one
        # print result
        # for key,value in result.items():
        #     #d = {}
        #     course_id = value['course_id']
        #     chapter_id = value['chapter_id']
        #     chapter_name = value['chapter_name']
        #     seq_id = value['seq_id']
        #     seq_name = value['seq_name']
            # d['course_id'] = course_id
            # d['chapter_id'] = chapter_id
            # d['chapter_name'] = chapter_name
            # d['seq_id'] = seq_id
            # d['seq_name'] = seq_name
            # for k,v in result_final.items():
            #     seq_list = []
            #     if k in result_final:
            #         seq_info = {}
            #         seq_info['chapter_id'] = chapter_id
            #         seq_info['chapter_name'] = chapter_name
            #         seq_info['seq_id'] = seq_id
            #         seq_info['seq_name'] = seq_name
            #         seq_list.append()
            #         pass #加在这个key对应的值的list里面
            #     else:
            #         pass #如果没有这个key，也就是没有这门课的记录，就加到result_final
            #result_list.append(d)


