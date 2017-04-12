#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Q
from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler
import settings
import datetime

Log.create('education')


@route('/education/course_overview')
class EducationCourseOverview(BaseHandler):
    """
    教务数据课程概览关键参数
    """
    def get(self):
        
        #TODO doc_type
        user_id = '8005'
        query = self.es_query(doc_type='academics_summary')\
                .filter('term', host=self.host)\
                .filter('term', user_id=user_id)\
                .filter('term', service_line=self.service_line)\
                .filter('term', course_status=self.course_status)
        
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
        course_result['active_num'] = result.active_nums
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
        user_id = '104'
        host = 'suzhoudaxue.xuetangx.com'
        service_line = 'credit'
        course_status = self.course_status
        query = self.es_query(doc_type='academics_course_dynamics')\
                .filter('term', user_id=user_id) \
                .filter('term', host=host)\
                .filter('term', service_line=service_line)\
                .filter('term', course_status=course_status)
        query_result = self.es_execute(query)
        result = []
        if course_status == '开课中':
            result = [{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'active_rate': hits.active_rate, 'course_name': hits.course_name } for hits in query_result.hits]
        if course_status == '已结课':
            result = [{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'pass_rate': hits.pass_rate, 'course_name': hits.course_name } for hits in query_result.hits]
        if course_status == '即将开课':
            result = [{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'course_name': hits.course_name } for hits in query_result.hits]

        self.success_response({'data': result})

@route('/education/course_name_search')
class EducationCourseNameSearch(BaseHandler):
    """
    教务数据课程概览课程名称搜索数据
    """
    def get(self):

        #TODO
        user_id = '5446'
        page = int(self.get_argument('page'))
        size = int(self.get_argument('size'))
        course_name = self.get_argument('course_name')
        query_statics = self.es_query(doc_type='academics_course_statics')\
                    .filter('term', user_id=user_id)\
                    .filter('term', host=self.host)\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', course_status=self.course_status).sort('-start_time')
        query_course_name =query_statics.filter(Q('bool', should=[Q('wildcard', course_name='%s*' % course_name)]))
        query_dynamics = self.es_query(doc_type='academics_course_dynamics')\
                         .filter('term', user_id=user_id)\
                         .filter('term', host=self.host)\
                         .filter('term', service_line=self.service_line)\
                         .filter('term', course_status=self.course_status)
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
            query_dynamics = query_dynamics.filter('terms', course_name=course_names)
            result_dynamics = self.es_execute(query_dynamics)
            data = [{'chapter': hits.chapter_issue_num, 'chapter_avg': hits.chapter_avg_length,'course_status': self.course_status, 'course_time': '%s-%s' %(hits.start_time.replace('-', '.'),hits.end_time.replace('-', '.')), 'course_name': hits.course_name} for hits in results.hits]
            data_dynamics = [{'enrollment_num':query_dynamic.enrollment_num, 'active_rate': query_dynamic.active_rate, \
                            'pass_num': query_dynamic.pass_num, 'avg_grade': query_dynamic.avg_grade, 'avg_comment_num': query_dynamic.avg_comment_num,\
                            'pass_rate': query_dynamic.pass_rate, 'course_name': query_dynamic.course_name, 'school': '%s.%s' %(query_dynamic.school, query_dynamic.service_line)} for query_dynamic in query_dynamics]
            for i in data:
                i['dynamics'] = []
            for i in data_dynamics:
                for j in data:
                    if i['course_name'] == j['course_name']:
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
        user_id = '5446'
        query = self.es_query(doc_type='academics_course_statics')\
                    .filter('term', user_id=user_id)\
                    .filter('term', host=self.host)\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', course_status=self.course_status)
        query_result = self.es_execute(query)
        import json
        print json.dumps(query.to_dict())
        if query_result.hits:
            #TODO
            temp_data = []
            for hits in query_result.hits:
                if hits.school not in temp_data:
                    temp_data.append(hits.school)
                if hits.user_id not in temp_data:
                    temp_data.append(hits.user_id)
            print set(temp_data)
            query_dynamics = self.es_query(doc_type='academics_course_dynamics')\
                                 .filter('term', user_id=user_id)\
                                 .filter('term', host=self.host)\
                                 .filter('term', service_line=self.service_line)\
                                 .filter('term', course_status=self.course_status)
            query_dynamic_result = self.es_execute(query_dynamics)
            #TODO
            print query_dynamics.to_dict()
            data = [[hits.course_status, hits.course_id, hits.course_name, hits.service_line, hits.course_type, hits.start_time, hits.end_time, hits.video_length, hits.chapter_num, hits.chapter_avg_length, hits.chapter_issue_num] for hits in query_result.hits for dynamic_hits in query_dynamic_result if hits.course_id == dynamic_hits.course_id and hits.service_line == dynamic_hits.service_line and hits.user_id == dynamic_hits.user_id]
            self.success_response({'data': data})
        self.success_response({'data': []})
 
