#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Q
from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler
import settings
import datetime
import time

Log.create('education')
COURSE_STATUS = {'process': '开课中', 'close': '已结课', 'unopen': '即将开课'}

@route('/education/course_overview')
class EducationCourseOverview(BaseHandler):
    """
    教务数据课程概览关键参数
    """
    def get(self):
        
        #TODO doc_type
        #service_line(application_id)
        #org_id
        query = self.es_query(doc_type='academics_summary')\
                .filter('term', host=self.host)\
                .filter('term', user_id=self.user_id)\
                .filter('term', service_line=self.service_line)\
                .filter('term', course_status=COURSE_STATUS.get(self.course_status))
        course_result = {}
        length = self.es_execute(query).hits.total
        if length == 0:
            course_result['course_num'] = 0
            course_result['active_num'] = 0
            course_result['video_length'] = 0
            course_result['enrollment_num'] = 0
            self.success_response({'data': course_result})
            return 
        result = self.es_execute(query[:1]).hits[0]
            
        course_result['course_num'] = result.course_num
        course_result['active_num'] = result.active_num
        course_result['video_length'] = result.video_length
        course_result['enrollment_num'] = result.enrollment_num
        
        self.success_response({'data': course_result})


@route('/education/course_study')
class EducationCourseStudy(BaseHandler):
    """
    教务数据气泡图数据
    """
    def get(self):

        #TODO host 学校标识需要主站传给我
        query = self.es_query(doc_type='academics_course_statics')\
                .filter('term', user_id=self.user_id) \
                .filter('term', host=self.host)\
                .filter('term', service_line=self.service_line)\
                .filter('term', course_status=COURSE_STATUS.get(self.course_status))
    
        total = self.es_execute(query).hits.total
        query_result = self.es_execute(query[:total])

        result = []
        if self.course_status == 'process':
            result.extend([{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'active_rate': hits.active_rate, 'course_name': hits.course_name } for hits in query_result.hits])
        if self.course_status == 'close':
            result.extend([{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'pass_rate': hits.pass_rate, 'course_name': hits.course_name } for hits in query_result.hits])
        if self.course_status == 'unopen':
            result.extend([{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'course_name': hits.course_name } for hits in query_result.hits])
        self.success_response({'data': result})

@route('/education/course_name_search')
class EducationCourseNameSearch(BaseHandler):
    """
    教务数据课程概览课程名称搜索数据
    """
    def get(self):

        #TODO
        page = int(self.get_argument('page'))
        size = int(self.get_argument('size'))
        course_name = self.get_argument('course_name')
        query_statics = self.es_query(doc_type='academics_course_statics')\
                    .filter('term', user_id=self.user_id)\
                    .filter('term', host=self.host)\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', course_status=COURSE_STATUS.get(self.course_status)).sort('-start_time')
        query_course_name =query_statics.filter(Q('bool', should=[Q('wildcard', course_name='%s*' % course_name)]))
        results = self.es_execute(query_statics[(page-1)*size:page*size])
        if course_name == ' ':
            pass
        else:
            results = self.es_execute(query_course_name)
        load_more = 0
        if results.hits:
            #TODO健康度
            if results.hits.total > page*size:
                load_more = 1
            course_names = []
            for name in [hit.course_name for hit in results.hits]:
                if name not in course_names:
                    course_names.append(name)
            # 没开课就是几天后开课，自主课程开课就是上线几个月 
            data = [{'course_id': hits.course_id,'course_type': hits.course_type,'chapter': hits.chapter_issue_num, 'chapter_total': hits.chapter_num,'chapter_avg': hits.chapter_avg_length,'course_status': self.course_status, 'course_time': '%s-%s' %(hits.start_time.replace('-', '.'),hits.end_time.replace('-', '.')), 'course_name': hits.course_name, 'start_time': hits.start_time } for hits in results.hits]
            for i in data:
                if i['course_type'] == 1 and self.course_status == 'process':
                    month = int(time.mktime(time.localtime())-time.mktime(time.strptime(i['start_time'],'%Y-%m-%d')))/60/60/24/30
                    i['month'] = month
                if self.course_status == 'unopen':
                    month = int(time.mktime(time.strptime(i['start_time'],'%Y-%m-%d'))-time.localtime())/60/60/24
                    i['month'] = month
                if self.course_status == 'process':
                    certification_status = 0
                    i['certification_status'] = certification_status
            course_ids = list(set([ hits.course_id for hits in results.hits]))
            #查teacher_power
            query_teacher_power = self.es_query(index='tapgo',doc_type='teacher_power')\
                                          .filter('term', user_id=self.user_id)\
                                          .filter('term', host=self.host)\
                                          .filter('terms',course_id=course_ids)
            total = self.es_execute(query_teacher_power).hits.total
            result_teacher_power = self.es_execute(query_teacher_power[:total])
            course_id = [hits.course_id for hits in result_teacher_power.hits]
            teacher_power = [{hits.course_id:[]} for hits in result_teacher_power.hits]
            for i in teacher_power:
                for j in result_teacher_power.hits:
                    if i.keys()[0] == j.course_id and j.group_key not in i.values():
                        i[i.keys()[0]].append(j.group_key)
            #查健康度以及相关数据
            result = []
            for i in teacher_power:
                for course_id, group_key in i.items():
                    query_health = self.es_query(index='test_health', doc_type='course_health')\
                                        .filter('term',course_id=course_id)\
                                        .filter('terms',group_key=group_key).source(['course_id','activate_rate', 'active_user_num', 'active_rank', 'enroll_num','enroll_rank', 'reply_rate', 'noreply_num','reply_rank','interactive_per','interactive_rank',\
                                                'comment_num', 'comment_rank', 'accomplish_num','avg_grade','post_per', 'group_key', 'accomplish_rate'])
                    result_health = self.es_execute(query_health)
                    #TODO
                    result.extend([hits.to_dict() for hits in result_health.hits])
            for i in data:
                i['dynamics'] = []
            for i in result:
                for j in data:
                    if i['course_id'] == j['course_id']:
                        j['dynamics'].append(i)
            self.success_response({'data': data, 'load_more': load_more})
        self.success_response({'data': [], 'load_more': load_more})

@route('/education/course_download')
class EducationCourseDownload(BaseHandler):
    """
    教务数据课程下载数据
    """
    def get(self):
        #TODO

        query = self.es_query(doc_type='academics_course_statics')\
                    .filter('term', user_id=self.user_id)\
                    .filter('term', host=self.host)\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', course_status=COURSE_STATUS.get(self.course_status))
        total = self.es_execute(query).hits.total
        query_result = self.es_execute(query[:total])
        import json
        print json.dumps(query_result.to_dict())
        data = []
        result_data = []
        if query_result.hits:
            print "************"
            course_ids = list(set([hits.course_id for hits in query_result.hits]))
            #TODO先查user_id,下对应的course_id:[group_key]
            #暂时先从teacher_power中查course_id:[group_key]
            #index暂时用tapgo
            data = [{'course_id': hits.course_id, 'chapter_num': hits.chapter_num,'video_length': hits.video_length, 'end_time': hits.end_time,'course_type': hits.course_type,'chapter_issue_num': hits.chapter_issue_num, 'chapter_total': hits.chapter_num,'chapter_avg': hits.chapter_avg_length,'course_status': self.course_status, 'course_time': '%s-%s' %(hits.start_time    .replace('-', '.'),hits.end_time.replace('-', '.')), 'course_name': hits.course_name, 'start_time': hits.start_time } for hits in query_result.hits]
            for i in data:
                if i['course_type'] == 1 and self.course_status == 'process':
                    month = int(time.mktime(time.localtime())-time.mktime(time.strptime(i['start_time'],'%Y-%m-%d')))/60/60/24/30
                    i['month'] = month
                if self.course_status == 'unopen':
                    month = int(time.mktime(time.strptime(i['start_time'],'%Y-%m-%d'))-time.localtime())/60/60/24
                    i['month'] = month
                if self.course_status == 'process':
                    certification_status = 0
                    i['certification_status'] = certification_status

            query_teacher_power = self.es_query(index='tapgo',doc_type='teacher_power')\
                                        .filter('term', user_id=self.user_id)\
                                        .filter('term', host=self.host)\
                                        .filter('terms', course_id=course_ids)
            total = self.es_execute(query_teacher_power).hits.total
            result_teacher_power = self.es_execute(query_teacher_power[:total])
            course_id = [hits.course_id for hits in result_teacher_power.hits]
            teacher_power = [{hits.course_id:[]} for hits in result_teacher_power.hits]
            for i in teacher_power:
                for j in result_teacher_power.hits:
                    if i.keys()[0] == j.course_id and j.group_key not in i.values():
                        i[i.keys()[0]].append(j.group_key)
            #print teacher_power
            #查健康度以及相关数据
            result = []
            for i in teacher_power:
                for course_id, group_key in i.items():
                    query_health = self.es_query(index='test_health', doc_type='course_health')\
                                        .filter('term',course_id=course_id)\
                                        .filter('terms',group_key=group_key)
                    result_health = self.es_execute(query_health)
                    #TODO
                    #print json.dumps(query_health.to_dict())
                    result.extend([hits.to_dict() for hits in result_health.hits])
                    #result.append({k:v for hits in result_health.hits for k,v in hits.to_dict().items()})
            for i in result:
                for j in data:
                    if i['course_id'] == j['course_id']:
                        i.update(j)
            #print result
            #TODO下载header
            #print result
            header = ['course_id', 'course_name','group_key', 'course_status', 'course_type', 'start_time', 'end_time', 'video_length', 'chapter_num', 'chapter_avg', 'chapter_issue_num', 'active_user_num', 'active_rate', 'avg_grade', 'post_per']
            for i in result:
                result_ = []
                if isinstance(i,dict):
                    for j in header:
                        result_.append(i[j])
                result_data.append(result_)
            self.success_response({'data': result_data})
        self.success_response({'data': []})
 
