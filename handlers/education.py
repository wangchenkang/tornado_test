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
COURSE_TYPE = {1: '自主模式', 0: '随堂模式'}
FIELD_COURSE_NAME_SEARCH = ['course_id','group_key', 'active_rank', 'enroll_rank', 'reply_rank', 'interactive_rank', 'comment_rank', 'enroll_num', 'active_rate', 'accomplish_num', 'avg_grade', 'post_per', 'accomplish_rate']
FIELD_COMMON = ['course_id', 'course_name', 'group_key', 'course_status', 'course_type', 'start_time', 'end_time', 'video_length', 'chapter_num', 'chapter_avg', 'chapter_issue_num']
FIELD_DOWNLOAD_PROCESS = FIELD_COMMON + ['active_user_num', 'active_rate', 'avg_grade', 'post_num', 'post_per']
FIELD_DOWNLOAD_CLOSE = FIELD_COMMON + ['accomplish_num','accomplish_rate', 'avg_grade', 'post_num', 'post_per']
FIELD_DOWNLOAD_UNOPEN = FIELD_COMMON
FIELD_DOWNLOAD = {'process': FIELD_DOWNLOAD_PROCESS, 'close': FIELD_DOWNLOAD_CLOSE, 'unopen': FIELD_DOWNLOAD_UNOPEN}
#TODO
EDUCATION_GROUP = {settings.MOOC_GROUP_KEY: '全部学生', settings.SPOC_GROUP_KEY: '全部学生'}

class Academic(BaseHandler):
    
    @property
    def summary_query(self):
        query = self.es_query(index='tap',doc_type='academics_summary')\
                    .filter('term', host=self.host)\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', user_id=self.user_id)\
                    .filter('term', course_status=COURSE_STATUS.get(self.course_status))
        return query
    
    @property
    def statics_query(self):
        #为了测试即将开课所以暂时当course_status == 'unopen'时取course_status = 'close'
        course_status = 'close' if self.course_status == 'unopen' else self.course_status
        query = self.es_query(index='tap',doc_type='academics_course_statics')\
                    .filter('term', host=self.host)\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', user_id=self.user_id)\
                    .filter('term', course_status=COURSE_STATUS.get(course_status))
        return query

    #TODO service_line(application_id)
    def get_summary(self):
        total = self.es_execute(self.summary_query).hits.total
        result = self.es_execute(self.summary_query[:total]).hits
        return result

    def get_statics(self):
        total = self.es_execute(self.statics_query).hits.total
        result = self.es_execute(self.statics_query[:total]).hits
        return result
    

    def get_course_name_search(self, page, size, course_name=None):
        query = self.statics_query
        if course_name:
            query =self.statics_query.filter(Q('bool', should=[Q('wildcard', course_name='%s*' % course_name)]))
        result = self.es_execute(query[(page-1)*size:page*size]).hits
        return result

    @property
    def teacher_power_query(self):
        query = self.es_query(index='tapgo', doc_type='teacher_power')\
                    .filter('term', user_id=self.user_id)\
                    .filter('term', host=self.host)
        return query
        
    #何琪在搞教学权限，到时直接查mysql
    def get_teacher_power(self, course_ids):
        #查teacher_power)
        query = self.teacher_power_query.filter('terms', course_id=course_ids)
        total = self.es_execute(query).hits.total
        result = self.es_execute(query[:total])
        teacher_power = [] 
        for i in [{hits.course_id:[]} for hits in result.hits]:
            if i not in teacher_power:
                teacher_power.append(i)
        for i in teacher_power:
            for j in result.hits:
                if j.course_id in i and j.group_key not in i:
                    i[j.course_id].append(j.group_key)
        return teacher_power

    def get_health(self, teacher_power, field=None):
        if not field:
            field = FIELD_COURSE_NAME_SEARCH 
        result = []
        for i in teacher_power:
            for course_id,group_key in i.items():
                query = self.es_query(index='test_health', doc_type='course_health')\
                            .filter('term',course_id=course_id)\
                            .filter('terms',group_key=group_key).source(field)
                result.extend([hits.to_dict() for hits in self.es_execute(query).hits])
        return result


@route('/education/course_overview')
class EducationCourseOverview(Academic):
    """
    教务数据课程概览关键参数
    """
    def get(self):
        
        result = self.get_summary()
        
        overview_result = {}
        overview_result['course_num'] = 0
        overview_result['active_num'] = 0
        overview_result['video_length'] = 0
        overview_result['enrollment_num'] = 0
        overview_result['pass_num'] = 0
        overview_result['no_num'] = 0
         
        if len(result) != 0:
            overview_result['course_num'] = result[0].course_num
            overview_result['active_num'] = result[0].active_num
            overview_result['video_length'] = result[0].video_length
            overview_result['enrollment_num'] = result[0].enrollment_num
            overview_result['pass_num'] = result[0].pass_num
        
        if self.course_status == 'process':
            del overview_result['pass_num']
            del overview_result['no_num']
        if self.course_status == 'close':
            del overview_result['active_num']
            del overview_result['no_num']
        if self.course_status == 'unopen':
            del overview_result['active_num']
            del overview_result['pass_num']

        self.success_response({'data': overview_result})


@route('/education/course_study')
class EducationCourseStudy(Academic):
    """
    教务数据气泡图数据
    """
    def get(self):

        result = self.get_statics()
        study_result = []
        if len(result) != 0:
            if self.course_status == 'process':
                study_result.extend([{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'active_rate': hits.active_rate, 'course_name': hits.course_name } for hits in result])
            if self.course_status == 'close':
                study_result.extend([{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'pass_rate': hits.pass_rate, 'course_name': hits.course_name } for hits in result])
            if self.course_status == 'unopen':
                study_result.extend([{'week_course_duration': hits.week_course_duration, 'enrollment_num': hits.enrollment_num, 'course_name': hits.course_name ,'no_rate': 0} for hits in result])
        
        self.success_response({'data': study_result})

@route('/education/course_name_search')
class EducationCourseNameSearch(Academic):
    """
    教务数据课程概览课程名称搜索数据
    """
    def get(self):

        #TODO
        page = int(self.get_argument('page'))
        size = int(self.get_argument('size'))
        course_name = self.get_argument('course_name', None)
        if not course_name:
            statics_result = self.get_statics()
        else:
            statics_result = self.get_course_name_search(page, size, course_name)

        load_more = 0
        if statics_result:
            #是否有下一页
            if statics_result.total > page*size:
                load_more = 1

            #TODO 没开课就是几天后开课，自主课程开课就是上线几个月,证书状态 
            data = [{'course_id': result.course_id,'course_type': result.course_type,'chapter': result.chapter_issue_num, 'chapter_total': result.chapter_num,\
                    'chapter_avg': result.chapter_avg_length,'course_status': self.course_status, 'course_time': '%s-%s' %(result.start_time.replace('-', '.'),result.end_time.replace('-', '.')), \
                    'course_name': result.course_name, 'start_time': result.start_time, 'month':0, 'certification_status':0, 'school': '%s.%s' %(result.school, '学分课') } for result in statics_result]
            
            for i in data:
                if i['course_type'] == 1 and self.course_status == 'process':
                    month = int(time.mktime(time.localtime())-time.mktime(time.strptime(i['start_time'],'%Y-%m-%d')))/60/60/24/30
                    i['month'] = month
                if self.course_status == 'unopen':
                    month = int(time.mktime(time.strptime(i['start_time'],'%Y-%m-%d'))-time.mktime(time.localtime()))/60/60/24
                    i['month'] = month
                
            course_ids = list(set([ result.course_id for result in statics_result]))
            #查teacher_power
            teacher_power = self.get_teacher_power(course_ids)
            
            #查健康度以及相关数据
            result = self.get_health(teacher_power)
            for i in data:
                i['dynamics'] = []
            for i in result:
                for j in data:
                    if i['course_id'] == j['course_id'] and i not in j['dynamics']:
                        i['school'] = EDUCATION_GROUP.get(i['group_key'], j['school'])
                        j['dynamics'].append(i)
            for j in data:
                del j['school']
            self.success_response({'data': data, 'load_more': load_more})
        self.success_response({'data': [], 'load_more': load_more})

@route('/education/course_download')
class EducationCourseDownload(Academic):
    """
    教务数据课程下载数据
    """
    def get(self):
        #TODO
        statics_result = self.get_statics()
        data = []
        result_data = []
        if statics_result:
            course_ids = list(set([hits.course_id for hits in statics_result]))
            #TODO先查user_id,下对应的course_id:[group_key]
            #暂时先从teacher_power中查course_id:[group_key]
            data = [{'course_id': result.course_id, 'chapter_num': result.chapter_num, 'video_length': result.video_length,\
                    'end_time': result.end_time, 'course_type': result.course_type, 'chapter_issue_num': result.chapter_issue_num,\
                    'chapter_total': result.chapter_num, 'chapter_avg': result.chapter_avg_length, 'course_status': self.course_status, \
                    'course_name': result.course_name, 'start_time': result.start_time, 'school': '%s.%s' %(result.school, '学分课') } for result in statics_result]
                
            teacher_power = self.get_teacher_power(course_ids)
            #查健康度以及相关数据
            field = FIELD_DOWNLOAD.get(self.course_status)
            result = self.get_health(teacher_power, field)
            for i in result:
                for j in data:
                    if i['course_id'] == j['course_id']:
                        i['group_key'] = EDUCATION_GROUP.get(i['group_key'], j['school'])
                        i.update(j)
            for j in data:
                del j['school']
            #TODO
            for i in result:
                result_ = []
                if isinstance(i,dict):
                    for j in field:
                        if j in ['active_rate', 'accomplish_rate']:
                            i[j] = "%.2f%%" %(float(i[j] or 0)*100)
                        if j == 'course_status':
                            i[j] = COURSE_STATUS[i[j]]
                        if j == 'course_type':
                            i[j] = COURSE_TYPE[i[j]]
                        result_.append(i[j])
                result_data.append(result_)
            self.success_response({'data': result_data})
        self.success_response({'data': []})
 
