#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Q
from utils.routes import route
from utils.tools import var
from utils.log import Log
from utils import mysql_connect
from .base import BaseHandler
import settings
import datetime
import time

Log.create('education')

COURSE_STATUS = {'process': '开课中', 'close': '已结课', 'unopen': '即将开课'}
COURSE_TYPE = {1: '自主模式', 0: '随堂模式'}
FIELD_COURSE_NAME_SEARCH = ['course_id', 'group_key', 'group_name', 'active_rank', 'enroll_rank', 'reply_rank', 'interactive_rank', 'comment_rank', 'enroll_num', 'active_rate', 'accomplish_num', 'avg_grade', 'post_per', 'accomplish_rate']
FIELD_COMMON = ['course_id', 'course_name', 'group_key','group_name', 'course_status', 'course_type', 'start_time', 'end_time', 'video_length', 'chapter_num', 'chapter_avg', 'chapter_issue_num']
FIELD_DOWNLOAD_PROCESS = FIELD_COMMON + ['active_user_num', 'active_rate', 'avg_grade', 'post_num', 'post_per']
FIELD_DOWNLOAD_CLOSE = FIELD_COMMON + ['accomplish_num','accomplish_rate', 'avg_grade', 'post_num', 'post_per']
FIELD_DOWNLOAD_UNOPEN = FIELD_COMMON
FIELD_DOWNLOAD = {'process': FIELD_DOWNLOAD_PROCESS, 'close': FIELD_DOWNLOAD_CLOSE, 'unopen': FIELD_DOWNLOAD_UNOPEN}
EDUCATION_GROUP = {settings.MOOC_GROUP_KEY: '全部学生', settings.SPOC_GROUP_KEY: '全部学生'}

class Academic(BaseHandler):
    
    @property
    def orgid_or_host(self):
        orgid_or_host = self.get_argument('orgid_or_host', None)
        if not orgid_or_host:
            self.error_response(u'参数错误')
        return orgid_or_host

    @property
    def role(self):
        results = mysql_connect.MysqlConnect('10.0.0.206', 'heqi_test', 'heqi', 'heqi').get_role(self.user_id, self.host)
        role = []
        role.extend([result['mode'] for result in results])
        for i in ['staff', 'vpc_admin', 'plateducaion']:
            if i in role:
                return 1
        return 0

    @property
    def summary_query(self):
        query = self.es_query(index='academics',doc_type='tap_academics_summary')\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', course_status=COURSE_STATUS.get(self.course_status))
        if self.service_line != 'mooc':
            query = query.filter('term', orgid_or_host=self.orgid_or_host)
        return query
    
    @property
    def statics_query(self):
        query = self.es_query(index='academics',doc_type='tap_academics_statics')\
                    .filter('term', service_line=self.service_line)\
                    .filter('term', course_status=COURSE_STATUS.get(self.course_status))\
                    .sort('-start_time')
        if self.service_line != 'mooc':
            query = query.filter('term', orgid_or_host=self.orgid_or_host)
        return query

    def get_summary(self, course_ids=None):
        if course_ids:
            query = self.statics_query.filter('terms', course_id=course_ids)
            query.aggs.metric('enrollment_num', 'sum', field='enrollment_num')
            query.aggs.metric('pass_num', 'sum', field='pass_num')
            query.aggs.metric('video_length', 'sum', field='video_length')
            query.aggs.metric('active_num', 'sum', field='active_num')
            query.aggs.metric('no_num', 'sum', field='no_num')
            results = self.es_execute(query)
            total = results.hits.total
            aggs = results.aggregations
            result = [{'course_num': total, 'enrollment_num': int(aggs.enrollment_num.value or 0), 'pass_num': int(aggs.pass_num.value or 0), 'video_length': round(int(aggs.video_length.value or 0)/3600,2), \
                           'active_num': int(aggs.active_num.value or 0), 'no_num': int(aggs.no_num.value or 0)}]
        else:
            total = self.es_execute(self.summary_query).hits.total
            result = self.es_execute(self.summary_query[:total]).hits
        return result

    def get_statics(self, course_ids=None):
        query = self.statics_query
        if course_ids:
            query = query.filter('terms', course_id=course_ids)
        total = self.es_execute(query).hits.total
        result = self.es_execute(query[:total]).hits
        return result
    

    def get_course_name_search(self, page, size, course_ids, course_name=None):
        query = self.statics_query.filter('terms', course_id=course_ids) if course_ids != 'all_' else self.statics_query
        if course_name:
            query =query.filter(Q('bool', should=[Q('wildcard', course_name='%s*' % course_name)]))
        result = self.es_execute(query[(page-1)*size:page*size]).hits
        return result

    @property
    def get_teacher_power(self):
        
        teacher_power, course_ids= mysql_connect.MysqlConnect('10.0.0.206', 'heqi_test', 'heqi', 'heqi').get_course_group_keys(self.user_id, self.host)
        return teacher_power, course_ids

    def get_health(self, teacher_power, field=None):
        if not field:
            field = FIELD_COURSE_NAME_SEARCH 
        result = []
        group_key_list = []
        course_ids = teacher_power.keys()
        for group_keys in teacher_power.values():
            for group_key in group_keys:
                if group_key not in group_key_list:
                    group_key_list.append(group_key)
        query = self.es_query(index='test_health', doc_type='course_health')\
                    .filter('terms',course_id=course_ids)\
                    .filter('terms',group_key=group_key_list).source(field)
        total = self.es_execute(query).hits.total
        result.extend([hits.to_dict() for hits in self.es_execute(query[:total]).hits])
        return result


@route('/education/course_overview')
class EducationCourseOverview(Academic):
    """
    教务数据课程概览关键参数
    """
    def get(self):
    
        teachter_power, course_ids = self.get_teacher_power
        result = self.get_summary() if self.role == 1 else self.get_summary(course_ids)   
        overview_result = {}
        overview_result['course_num'] = 0
        overview_result['active_num'] = 0
        overview_result['video_length'] = 0
        overview_result['enrollment_num'] = 0
        overview_result['pass_num'] = 0
        overview_result['no_num'] = 0
         
        if len(result) != 0:
            overview_result['course_num'] = result[0]['course_num']
            overview_result['active_num'] = result[0]['active_num']
            overview_result['video_length'] = round(result[0]['video_length']/3600, 2)
            overview_result['enrollment_num'] = result[0]['enrollment_num'] or 0
            overview_result['pass_num'] = result[0]['pass_num']
        
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

        teacher_power, course_ids = self.get_teacher_power
        result = self.get_statics() if self.role == 1 else self.get_statics(course_ids)
    
        study_result = []
        if len(result) != 0:
            if self.course_status == 'process':
                study_result.extend([{'week_course_duration': round((hits.week_course_duration or 0)/3600, 2), 'enrollment_num': hits.enrollment_num or 0, 'active_rate': round(hits.study_active_rate or 0, 4), 'course_name': hits.course_name } for hits in result])
            if self.course_status == 'close':
                study_result.extend([{'week_course_duration': round((hits.week_course_duration or 0)/3600, 2), 'enrollment_num': hits.enrollment_num or 0, 'pass_rate': round(hits.pass_rate or 0, 4), 'course_name': hits.course_name } for hits in result])
            if self.course_status == 'unopen':
                study_result.extend([{'week_course_duration': round((hits.week_course_duration or 0)/3600, 2), 'enrollment_num': hits.enrollment_num or 0, 'no_rate': 0, 'course_name': hits.course_name} for hits in result])
        
        self.success_response({'data': study_result})

@route('/education/course_name_search')
class EducationCourseNameSearch(Academic):
    """
    教务数据课程概览课程名称搜索数据
    """
    def get(self):

        page = int(self.get_argument('page'))
        size = int(self.get_argument('size'))
        course_name = self.get_argument('course_name', None)

        teacher_power, course_ids = self.get_teacher_power
        statics_result = self.get_course_name_search(page, size, 'all_', course_name) if self.role == 1 else self.get_course_name_search(page, size, course_ids, course_name)
        load_more = 0
        result_data = []
        if statics_result:

            #是否有下一页
            if statics_result.total > page*size:
                load_more = 1

            #TODO 没开课就是几天后开课，自主课程开课就是上线几个月,证书状态 
            data = [{'course_id': result.course_id,'course_type': int(result.course_type),'chapter': result.chapter_issue_num, 'chapter_total': result.chapter_num,\
                    'chapter_avg': round((result.chapter_avg_length or 0)/3600,2),'course_status': self.course_status, 'course_time': '%s-%s' %(result.start_time.replace('-', '.'),result.end_time.replace('-', '.')), \
                    'course_name': result.course_name, 'start_time': result.start_time.split(' ')[0], 'month':0, 'certification_status': result.certification_status } for result in statics_result]
            
            for i in data:
                if i['course_type'] == 1 and self.course_status == 'process':
                    month = int(time.mktime(time.localtime())-time.mktime(time.strptime(i['start_time'],'%Y-%m-%d')))/3600/24/30
                    i['month'] = month
                if self.course_status == 'unopen':
                    if i['start_time']:
                        month = int(time.mktime(time.strptime(i['start_time'],'%Y-%m-%d'))-time.mktime(time.localtime()))/3600/24
                        i['month'] = month
                    else:
                        i['month'] = -1
                del i['start_time']

            #course_id:[group_key]
            course_ids = [result.course_id for result in statics_result]
            course_id_group_key = {}
            for course_id in course_ids:
                try:
                    course_id_group_key[course_id] = teacher_power[course_id]
                except KeyError:
                    continue
            #查健康度以及相关数据
            result = self.get_health(course_id_group_key)
            teacher_data = []
            for i in data:
                i['dynamics'] = []
            for i in result:
                for j in data:
                    if i['course_id'] == j['course_id'] and i not in j['dynamics']:
                        if self.service_line in ['mooc', 'spoc']:
                            if i['group_key'] in [settings.MOOC_GROUP_KEY, settings.SPOC_GROUP_KEY]:
                                i['school'] = '全部学生'
                            if i['group_key'] == settings.TSINGHUA_GROUP_KEY:
                                i['school'] = i['group_name']
                            if i['group_key'] == settings.ELECTIVE_ALL_GROUP_KEY:
                                i['school'] = '%s.%s' % ('全部学生', '学分课')
                            if i['group_key'] in [settings.MOOC_GROUP_KEY, settings.SPOC_GROUP_KEY, settings.TSINGHUA_GROUP_KEY, settings.ELECTIVE_ALL_GROUP_KEY]:
                                j['dynamics'].append(i)
                                teacher_data.append(j)
                        if self.service_line == 'credit':
                            if i['group_key'] == settings.TSINGHUA_GROUP_KEY:
                                i['school'] = '%s' % (i['group_name'])
                                j['dynamics'].append(i)
                            if i['group_key'] >= settings.ELECTIVE_GROUP_KEY:
                                i['school'] = '%s.%s' % (i['group_name'], '学分课')
                                j['dynamics'].append(i)
                                teacher_data.append(j)
                        
        
            for i in teacher_data:
                i['dynamics'].sort(lambda x,y: cmp(x["group_key"], y["group_key"]))
            result_data.extend(teacher_data)
        self.success_response({'data': result_data, 'load_more': load_more})

@route('/education/course_download')
class EducationCourseDownload(Academic):
    """
    教务数据课程下载数据
    """
    def get(self):
        
        teacher_power, course_ids = self.get_teacher_power
        statics_result = self.get_statics() if self.role == 1 else self.get_statics(course_ids)
        data = []
        result_data = []
        if statics_result:
            data = [{'course_id': result.course_id, 'chapter_num': result.chapter_num, 'video_length': round(result.video_length,1),\
                    'end_time': result.end_time, 'course_type': result.course_type, 'chapter_issue_num': result.chapter_issue_num,\
                    'chapter_total': result.chapter_num, 'chapter_avg': round((result.chapter_avg_length or 0)/3600,2), 'course_status': self.course_status, \
                    'course_name': result.course_name, 'start_time': result.start_time} for result in statics_result]     
            
            course_ids = [result.course_id for result in statics_result]
            course_id_group_key = {}
            for course_id in course_ids:
                try:
                    course_id_group_key[course_id] = teacher_power[course_id]
                except KeyError:
                    continue
            #查健康度以及相关数据
            field = FIELD_DOWNLOAD.get(self.course_status)
            result = self.get_health(course_id_group_key, field)
            for i in result:
                for j in data:
                    if i['course_id'] == j['course_id']:
                        if self.service_line != 'credit':
                            if i['group_key'] in [settings.MOOC_GROUP_KEY, settings.SPOC_GROUP_KEY]:
                                i['group_name'] = '全部学生'
                            if i['group_key'] == settings.ELECTIVE_ALL_GROUP_KEY:
                                i['group_name'] = '%s.%s' % ('全部学生', '学分课')
                            if i['group_key'] in [settings.MOOC_GROUP_KEY, settings.SPOC_GROUP_KEY, settings.TSINGHUA_GROUP_KEY, settings.ELECTIVE_ALL_GROUP_KEY]:
                                i.update(j)
                        else:
                            if i['group_key'] == settings.TSINGHUA_GROUP_KEY:
                                i.update(j)
                            if i['group_key'] >= settings.ELECTIVE_GROUP_KEY:
                                i['group_name'] = '%s.%s' % (i['group_name'], '学分课')
                                i.update(j)
        
            for i in result:
                if self.service_line != 'credit':
                    if i['group_key'] not in [settings.MOOC_GROUP_KEY, settings.SPOC_GROUP_KEY, settings.ELECTIVE_ALL_GROUP_KEY, settings.TSINGHUA_GROUP_KEY]:
                        continue
                else:
                    if i['group_key'] < settings.ELECTIVE_GROUP_KEY:
                        if i['group_key'] !=  settings.TSINGHUA_GROUP_KEY:
                            continue
                result_ = []
                if isinstance(i,dict):
                    for j in field:
                        if j == 'group_key':
                            continue
                        if j in ['active_rate', 'accomplish_rate']:
                            i[j] = "%.2f%%" %(float(i[j] or 0)*100)
                        if j == 'course_status':
                            i[j] = COURSE_STATUS[i[j]]
                        if j == 'course_type':
                            i[j] = COURSE_TYPE[int(i[j])]
                        result_.append(i[j])
                result_data.append(result_)

        self.success_response({'data': result_data})
 
