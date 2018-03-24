#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from copy import copy
from tornado.web import gen, HTTPError
from elasticsearch_dsl import Q

import settings
from base import BaseHandler
from utils.routes import route
from utils.tools import date_from_string
from utils.mysql_connect import MysqlConnect
from utils.log import Log

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

COURSE_OVERVIEW_FIELD = ['course_name', 'course_id', 'enroll_num', 'society_enroll_num', 'platform_enroll_num', \
                         'main_enroll_num', 'use_platform_num', 'course_status']
COURSE_DETAIL_FIELD = ['enroll_num', 'society_enroll_num', 'platform_enroll_num', 'main_enroll_num', 'discussion_num', 'use_platform_num']
COURSE_TABLE_FIELD = ['plat_name', 'enroll_num', 'avg_video_rate', 'avg_accmp_rate', 'discussion_num']
PLATFORM_OVERVIEW_FIELD = ['plat_name', 'enroll_num', 'use__num', 'open_num', 'unopen_num', \
                           'close_num']
PLATFORM_TABLE_FIELD = ['course_name', 'course_status', 'enroll_num', 'post_reply_num', 'avg_score']

PLATFORM_TABLE_NAME= [u'序号', u'平台名称', u'选课人数(学生)', u'视频平均观看率', u'作业完成率', u'讨论回复数']
COURSE_OVERVIEW_TABLE_NAME= [u'序号', u'课程名称', u'课程ID', u'学生总数', u'社会/平台/主站学生', u'使用平台数', u'课程状态']
COURSE_TABLE_NAME= [u'序号', u'平台名称', u'选课人数(学生)', u'视频平均观看率', u'作业完成率', u'讨论回复数']
PLATFORM_OVERVIEW_TABLE_NAME= [u'序号', u'平台名称', u'学分课学生人次', u'引进学分课数量', u'开课中/待开/已结课数量']

class CreditData(BaseHandler):
    """
    学分课数据类
    """
    def get_total_page(self, total, num):
        if total == 0:
            total_page = 0
        elif total <= num:
            total_page = 1
        else:
            if total % num == 0:
                total_page = total/num
            else:
                total_page = total/num + 1

        return total_page

    def format_course_info(self, results):
        data = []
        for result in results:
            course_info = {}
            course_info['course_name'] = result.course_name
            course_info['study_video_rate'] = self.round_data(result.study_video_rate_course, 4)
            course_info['score_avg'] = self.round_data(result.score_avg_course, 2)
            course_info['effort'] = self.round_data(result.effort_course, 2)
            course_info['student_num'] = result.enroll_num_course
            course_info['teacher_num'] = result.teacher_num_course
            course_info['no_watch_person'] = result.no_watch_person_course
            course_info['course_id'] = result.course_id
            course_info['course_status'] = result.course_status
            course_info['service_line'] = result.service_line
            course_info['org_id'] = result.org_id
            course_info['course_start'] = result.course_start
            data.append(course_info)

        return data
    
    def get_header(self, field, name):
        field = eval(field.upper())
        name = eval(name.upper())
        fields = copy(field)
        fields.insert(0, 'id') 
        header = [{'field': field, 'name': name[index] } for index, field in enumerate(fields)]
        
        return header
    
    def get_table_result(self, field, name):
        field = field.upper()
        name = name.upper()
        fields = copy(field)
        fields.insert(0, 'id') 
        header = [{'field': field, 'name': name[index] } for index, field in enumerate(fields)]
        
        return header


@route('/credit/course_overview')
class CreditCourseOverview(CreditData):
    """
    学分课课程概况
    """
    @gen.coroutine
    def post(self):
        result = self.get_result()
        update_time = yield self.get_updatetime()
        result['update_time'] = update_time

        self.success_response({'data': result})
   
    def get_result(self): 
        header = self.get_header('course_overview_table_field', 'course_overview_table_name') 
        data, course_num, total_page, current_page = self.get_data()
        
        result = dict()
        result['course_num'] = course_num
        result['total_page'] = total_page
        result['current_page'] = current_page
        result['header'] = header
        result['data'] = data
         
        return result
    
    def get_data(self):
        query, num, page = self.get_query()
        results = self.es_execute(query)
        course_num = results.hits.total
        total_page = self.get_total_page(course_num, num)
        
        data = list() 
        for index, result in enumerate(results.hits):
             i = list()
             i.append(index)
             i.append(result.course_name)      
             i.append(result.course_id)      
             i.append(result.enroll_num)      
             i.append('%s/%s/%s' % (result.society_enroll_num, result.platform_enroll_num, result.main_enroll_num)) 
             i.append(result.use_platform_num) 
             #TODO    
             i.append(result.course_status)
             data.append(i)
              
        return data, course_num, total_page, page
    
    def get_query(self):
        app_id = self.get_argument('app_id', None)
        Log.create('credit')
        Log.info('%s-%s' % ('credit_data', app_id)) 
        
        sort_argument = self.get_argument('sort_argument', 'enroll_num')
        sort_argument = json.loads(sort_argument)
        course_ids = self.get_argument('course_ids', None)
        course_ids = json.loads(course_ids) if course_ids else list()
        sort_type = self.get_argument('sort_type', '1')
        sort_type = json.loads(sort_type)
        sort_argument = sort_argument if sort_type else '%s%s' % ('-', sort_argument)
        download_status = self.get_argument('download_status', None)
        download_status = json.loads(download_status) if download_status else 0
        page = self.get_argument('page', '1')
        page = json.loads(page)
        num = self.get_argument('num', '20')
        num = json.loads(num)
       
        query = self.es_query(index=settings.CREDIT_ES_INDEX, doc_type='course_aggs') \
                    .source(COURSE_OVERVIEW_FIELD) \
                    .sort(sort_argument)
        query = query.filter('terms', course_id=course_ids) if course_ids else query
        
        if download_status:
            total = self.es_execute(query[:0]).hits.total
            query = query[:total] 
        else:
            query = query[(num-1)*page:num*page] 
        
        return query, num, page


@route('/credit/course_detail')
class CreditCourseDetail(CreditData):
    """
    学分课课程详情
    """
    def get(self):
        data = self.get_data()
        self.success_response({'data': data})
    
    def get_data(self):
        query = self.get_query()
        results = self.es_execute(query)
        result = results.hits 
        data = dict()
          
        data['enroll_num'] = result[0].enroll_num if results.hits else 0
        data['discussion_num'] = result[0].enroll_num if results.hits else 0
        data['use_platform_num'] = result[0].use_platform_num if results.hits else 0
        society_enroll_num = result[0].society_enroll_num if results.hits else 0
        platform_enroll_num = result[0].platform_enroll_num if results.hits else 0
        main_enroll_num = result[0].main_enroll_num if results.hits else 0
        data['platform_enroll_num'] = '%s/%s/%s' % (society_enroll_num, platform_enroll_num, main_enroll_num)
        #TODO 
        data['group_key'] = 1
         
        return data
    
    def get_query(self):
        app_id = self.get_param('app_id')
        Log.create('credit_data')
        Log.info('%s-%s' % ('credit_data', app_id)) 
        
        course_id = self.get_param('course_id')
        query = self.es_query(index=settings.CREDIT_ES_INDEX, doc_type='course_aggs') \
                    .filter('term', course_id=course_id) \
                    .source(COURSE_DETAIL_FIELD)
        
        return query


@route('/credit/course_table')
class CreditCourseTable(CreditData):
    """
    学分课课程详情表格数据
    """
    @gen.coroutine 
    def get(self):
        data = self.get_result()
        update_time = yield self.get_updatetime()
        data['update_time'] = update_time 
        self.success_response({'data': data})
    
    def get_result(self):
        header = self.get_header('course_table_field', 'course_table_name') 
        result = self.get_data()
        result['header'] = header 
        return result 
    
    def get_data(self):
        query, page, num = self.get_query()
        results = self.es_execute(query)
        plat_num = results.hits.total
        total_page = self.get_total_page(plat_num, num)
        
        data = list() 
        for index, result in enumerate(results.hits):
            l = list()
            l.append(index + 1)
            l.append(result.plat_name)
            l.append(result.enroll_num)
            l.append(result.avg_video_rate)
            l.append(result.avg_accmp_rate)
            l.append(result.discussion_num)
            data.append(l)
        
        result = dict()
        result['data'] = data
        result['plat_num'] = plat_num
        result['total_page'] = total_page
        result['current_page'] = page 
        
        return result

    def get_query(self):
        app_id = self.get_param('app_id')
        Log.create('credit_data')
        Log.info('%s-%s' % ('credit_data', app_id))
        
        course_id = self.get_param('course_id') 
        sort_argument = self.get_argument('sort_argument', 'enroll_num')
        sort_type = self.get_argument('sort_type', '1')
        sort_type = json.loads(sort_type)
        sort_argument = '-%s' % sort_argument if not sort_type else sort_argument 
        download_status = self.get_argument('download_status', '0')
        download_status = json.loads(download_status) 
        page = self.get_argument('page', '1')
        page = json.loads(page)
        num = self.get_argument('num', '20')
        num = json.loads(num) 
       
        query = self.es_query(index=settings.CREDIT_ES_INDEX, doc_type='course_plat_detail') \
                    .filter('term', course_id=course_id) \
                    .sort(sort_argument) \
                    .source(COURSE_TABLE_FIELD) 
       
        if download_status:
            total = self.es_execute(query[:0]).hits.total
            query = query[:total]
        else:
            query = query[(page-1)*num:num*page] 
        
        return query, page, num
    

@route('/credit/platform_overview')
class CreditPlatformOverview(CreditData):
    """
    学分课平台概况
    """
    @gen.coroutine
    def get(self):
        data = self.get_result()
        update_time = yield self.get_updatetime()
        data['update_time'] = update_time
        self.success_response({'data': data})
   
    def get_result(self):
        header = self.get_header()
        data = self.get_data()
        data['header'] = header
        return data
 
    def get_header(self):
        fields = copy(PLATFORM_OVERVIEW_FIELD)
        fields.insert(0, 'id') 
        names = PLATFORM_OVERVIEW_TABLE_NAME
        header = [{'field': field, 'name': names[index] }for index, field in enumerate(fields)]
        return header
    
    def get_data(self):
        query = self.get_query()
        resutls = self.es_execute(query)
        return result

    def get_query(self):
        app_id = sef.get_param('app_id')
        Log.create('credit_data')
        Log.info('%s-%s' % ('credit_data', app_id))
        
        search_argument = self.get_argument('search_argument', None)
        sort_argument = self.get_argument('sort_argument', 'enroll_num')
        sort = self.get_argument('sort', '1')
        sort = json.loads(sort)
        sort_argument = sort_argument if sort else '-%s' % sort_argument 
        download_status = self.get_argument('download_status', '0')
        download_status = json.loads(download_status) 
        num = self.get_argument('num', '20')
        num = json.loads(num)
        page = self.get_argument('page', '1')
        page = json.loads(page) 
       
        query = self.es_query(index=settings.CREDIT_ES_INDEX, doc_type='course_plat_detail') \
                    .filter(Q('bool', should=[Q('wildcard', plat_name='*%s*' % search_argument)])) 
                    
            
        self.success_response({'data': query, 'total_page': total_page, 'current_page': page})

@route('/credit/platform_detail')
class CreditPlatformDetail(CreditData):
    """
    学分课平台详情
    """
    def get(self):
        result = self.get_result()

        self.success_response({'data': result})
    
    def get_result(self):
        query = self.get_query()
        result = self.es_execute(query[:0])
        aggs = result.aggregations
         
        data = {}
        data['credit_enroll_num'] = aggs.enroll_num.value or 0
        
        import_credit_num = 0
        for i in aggs.credit.buckets:
            if i.key:
                import_credit_num = i.enroll_num.value
        data['import_credit_num'] = import_credit_num
        open_num = 0
        unopen_num = 0
        close_num = 0
        for i in aggs.course_status.buckets:
            if i.key == 'open':
                open_num = i.doc_count
            elif i.key == 'unopen':
                unopen_num = i.doc_count
            elif i.key == 'close':
                close_num = i.doc_count
        data['course_status_num'] = '%s/%s/%s' % (open_num, unopen_num, close_num)
        data['discussion_num'] = aggs.discussion_num.value or 0
        
        return data
    
    def get_query(self):
        app_id = self.get_param('app_id')
        Log.create('credit_data')
        Log.info(app_id) 
        
        plat_id = self.get_param('plat_id')
        query = self.es_query(index=settings.CREDIT_ES_INDEX, doc_type='course_plat_detail') \
                    .filter('term', plat_id=plat_id)
        query.aggs.metric('enroll_num', 'sum', field='enroll_num')
        query.aggs.bucket('credit', 'terms', field='is_credit', size=2) \
                  .metric('enroll_num', 'sum', field='enroll_num')
        query.aggs.bucket('course_status', 'terms', field='course_status', size=3)
        query.aggs.metric('discussion_num', 'sum', field='discussion_num')
        
        return query


@route('/credit/platform_table')
class CreditPlatformTable(CreditData):
    """
    学分课平台详情表格数据
    """
    def get(self):
        result = self.get_result() 
        header = self.get_header('platform_table_field', 'platform_table_name')
        result['header'] = header 
        self.success_response({'data': result})
    
    def get_query(self):
        app_id = self.get_param('app_id')
        Log.create('credit_data')
        Log.info(app_id)
        
        plat_id = self.get_param('plat_id')
        search_argument = self.get_argument('search_argument', '')
        sort_argument = self.get_argument('sort_argument', 'enroll_num')
        sort_type = self.get_argument('sort_type', '1')
        sort_argument = sort_argument if sort_type else '-%s' % sort_argument
        download_status = self.get_argument('download_status', '0')
        page = self.get_argument('page', '1')
        num = self.get_argument('num', '20')

        query = self.es_query(index=settings.CREDIT_ES_INDEX, doc_type='course_plat_detail') \
                    .filter('term', plat_id=plat_id) \
                    .filter(Q('bool', should=[Q('wildcard', course_name= '*%s*' % search_argument)])) \
                    .source(['course_name', 'course_status', 'enroll_num', 'discussion_num','avg_score']) \
                    .sort(sort_argument) 
        
        total = self.es_execute(query[:0]).hits.total
        if download_status:
            query = query[:total]
        else:
            query = query[(page-1)*num:page*num]
                
        return query, page, num

    def get_result(self):
        header = self.get_header('platform_table_field', 'platform_table_name')
        query, page, num = self.get_query()
        results = self.es_execute(query).hits
        course_num = results.total
        total_page = self.get_total_page(course_num, num) 
        
        data = list() 
        for index, result in enumerate(results):
            l = list()
            l.append(index + 1)
            l.append(result.course_name)
            l.append(result.course_status)
            l.append(result.enroll_num or 0)
            l.append(result.discussion_num or 0)
            l.append(round(result.avg_score or 0, 2))
            data.append(l)
        
        result = dict()
        result['data'] = data
        result['course_num'] = course_num
        result['total_page'] = total_page
        result['current_page'] = page 
        return result

