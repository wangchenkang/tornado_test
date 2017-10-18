#!/usr/bin/env python
# -*- coding: utf-8 -*-

from utils.routes import route
from .base import BaseHandler
import settings
from tornado.web import gen
from elasticsearch_dsl import Q
from utils.mysql_connect import MysqlConnect
from utils import cache
import time
import copy
import json


COURSE_INFO_FIELD = ['course_id', 'course_name', 'orgid_or_host', 'start_time', 'end_time', 'chapter_num', 'chapter_issue_num', 'course_status', 'chapter_avg_length', 'course_type', 'certification_status', 'video_length']
COURSE_HEALTH_FIELD = ['course_id', 'group_key', 'group_name', 'interactive_rank', 'comment_rank', 'enroll_rank', 'enroll_num',\
                       'post_per', 'school', 'reply_rank', 'active_rank', 'accomplish_num', 'accomplish_rate', 'reply_rate', 'avg_grade', \
                       'active_rate', 'active_user_num', 'post_num', 'accomplish_num', 'accomplish_rate']
COURSE_HEALTH_POP_FIELD = ['dynamics', 'month', 'parent', 'course_time', 'certification_status', 'enroll_num', 'enroll_rank', 'interactive_rank', 'comment_rank', 'reply_rank', 'reply_rate', 'status'] 

class OpenTime(BaseHandler):
    
    @property
    def parent_id(self):
        query = self.es_query(index = 'course_ancestor', doc_type = 'course_ancestor')\
                             .filter('term', course_id = self.course_id)[:1]
        result = self.es_execute(query).hits
        parent_id = result[0].parent_id if result else self.course_id
                
        return parent_id

    def get_children_id_open_times(self, parent_id):
        query = self.es_query(index = 'course_ancestor', doc_type = 'course_ancestor')\
                             .filter('term', parent_id = parent_id)\
                             .filter('range', **{'status': {'gte': -1}})\
                             .source('course_id')
        result = self.es_execute(query[:0]).hits
        open_times = result.total or 1
        result = self.es_execute(query[:open_times]).hits
        course_ids = [item.course_id for item in result] or [self.course_id]
        
        return course_ids, open_times

    def get_course_aggs(self, course_ids, service_line, host):
        size = len(course_ids)
        query = self.es_query(index = 'academics', doc_type = 'tap_academics_statics')\
                            .filter('terms', course_id = course_ids)

        if host != 'studio.xuetangx.com':
            query = query.filter('term', orgid_or_host = host)
            service_line = 'spoc'
       
        opration = 'terms' if isinstance(service_line, list) else 'term'
        query = query.filter('%s' % opration, service_line = service_line)
        query.aggs.bucket('platform_num', 'terms', field = 'orgid_or_host', size = size)
        query.aggs.metric('enroll_total', 'sum', field = 'enrollment_num')
        query.aggs.metric('pass_total', 'sum', field = 'pass_num')
        result = self.es_execute(query[:0])
        aggs = result.aggregations
        total = result.hits.total
        
        return aggs, total

    def round_data(self, data):
        data = round(data or 0, 2)
        
        return data

@route('/open_times/overview')
class OpenTimesOverview(OpenTime):
    """
    """
    def get(self):
        parent_id = self.parent_id
        host = self.host
        title_status = 1 if host == 'studio.xuetangx.com' else 0
        service_line = self.get_argument('service_line')
        service_line = json.loads(service_line) 
        course_ids, _ = self.get_children_id_open_times(parent_id)
        aggs, total = self.get_course_aggs(course_ids, service_line, host)
        platform = aggs.platform_num.buckets
        platform_num = len(platform)
        enroll_total = int(aggs.enroll_total.value or 0)
        pass_total = int(aggs.pass_total.value or 0)
        
        data = {}
        data['open_times'] = total 
        data['platform_num'] = platform_num
        data['enroll_total'] = enroll_total
        data['pass_total'] = pass_total
        data['title_status'] = title_status

        self.success_response({'data': data})


@route('/open_times/search')
class OpenTimesSearch(OpenTime):
    """
    """
    def formate_time(self, time):
        if time:
            time = time.replace('-', '.')
        
        return time

    def update_course_info(self, service_line, course_info, course_health, parent_id, num):
        if service_line != ['mooc', 'spoc']:
            courses = []
        for course in course_info:
            course['month'] = 0
            if course['course_type'] == '1' and course['course_status'] == '开课中':
                #自主课程
                month = int(time.mktime(time.localtime())-time.mktime(time.strptime(course['start_time'],'%Y-%m-%d')))/3600/24/30
                course['month'] = month
            if course['course_status'] == '即将开课':
                if course['start_time']:
                    month = int(time.mktime(time.strptime(course['start_time'],'%Y-%m-%d'))-time.mktime(time.localtime()))/3600/24
                    course['month'] = month
                else:
                    course['month'] = -1#未设置开课时间

            course['dynamics'] = []
            course['parent'] = 0
            course['chapter_avg'] = self.round_data(course.pop('chapter_avg_length'))
            course['chapter'] = course.pop('chapter_issue_num')
            course['chapter_total'] = course.pop('chapter_num')
            course['certification_status'] = int(course.pop('certification_status'))
            course['course_type'] = int(course.pop('course_type'))
            platform_host = course.pop('orgid_or_host')
            course['platform_host'] = platform_host if platform_host else 'www.xuetangx.com'
            start_time = self.formate_time(course.pop('start_time'))
            end_time = self.formate_time(course.pop('end_time'))
            course['course_time'] = '%s-%s' % (start_time, end_time)
            video_length = course.pop('video_length')

            if course['course_id'] == parent_id:
                course['parent'] =1
            for item in course_health:
                if course['course_id'] == item['course_id'] and num != -1:
                    course['dynamics'].append(item)
                    if service_line in ('mooc', 'spoc'):
                        courses.append(course)
        
        if not course_health:
            course_info =[]

        if num == -1:
            course_info = self.update_course_health(course_health, course_info, video_length, start_time, end_time)
        if service_line != ['mooc', 'spoc'] and num != -1:
            course_info = courses
        
        return course_info

    def update_course_health(self, course_health, course_info, video_length, start_time, end_time):
        courses = []
        for item in course_info:
            data = {}
            data_course = []
            for course in course_health:
                if item['course_id'] == course['course_id']:
                    item.update(course)
                    item['course_type'] = u'自主模式' if item.pop('course_type') else u'随堂模式'
                    item['video_length'] = video_length
                    item['start_time'] = start_time.replace('.', '-')
                    item['end_time'] = end_time.replace('.', '-')
                    data.update(item)
                    if data not in data_course:
                        data_course.append(data)
                    for key in COURSE_HEALTH_POP_FIELD:
                        if key in item:
                            item.pop(key)
            courses.extend(data_course)
        return courses

    def get_course_infos(self, parent_id, course_ids, course_name, page, num, service_line, host):
        operation = 'terms' if isinstance(service_line, list) else 'term'
        if host != 'studio.xuetangx.com':
            operation = 'term'
            service_line = 'spoc'
        query = self.es_query(index = 'academics', doc_type = 'tap_academics_statics')\
                            .filter('term', course_id = parent_id)\
                            .source(COURSE_INFO_FIELD)\
                            .filter('%s' % operation, service_line = service_line)
        if host != 'studio.xuetangx.com':
            query = query.filter('term', orgid_or_host = host)
        parent_info = self.es_execute(query).hits
        parent_info = parent_info[0].to_dict() if parent_info else None
        size = len(course_ids)
        course_ids_download = copy.deepcopy(course_ids)
        if parent_id in set(course_ids):
            course_ids.pop(course_ids.index(parent_id))
        
        if num == -1:
            course_ids = course_ids_download
        
        query = self.es_query(index = 'academics', doc_type = 'tap_academics_statics')\
                            .filter('terms', course_id = course_ids)\
                            .sort('-start_time')\
                            .source(COURSE_INFO_FIELD)
        if host != 'studio.xuetangx.com':
            query = query.filter('term', orgid_or_host = host)
        
        if course_name != 'all':
            query = query.filter(Q('bool', should=[Q('wildcard', course_name='*%s*' % course_name)]))
       
        query = query.filter('%s' % operation, service_line = service_line)
        if num == -1:
            course_info = self.es_execute(query[:size]).hits
        else:
            if page == 1:
                if parent_info:
                    num =9
            course_info = self.es_execute(query[(page-1)*num:page*num]).hits

        course_ids = [item.course_id for item in course_info]
        course_info = [item.to_dict() for item in course_info]
        if page == 1 and num != -1:
            if parent_info:
                course_info.insert(0, parent_info)
        
        course_ids.append(parent_id)
        
        return course_ids, course_info

    @property
    def group_keys(self):
        service_line = self.get_argument('service_line')
        service_line = json.loads(service_line)
        host = self.host

        if service_line == ['mooc', 'spoc']:
            if host == 'studio.xuetangx.com':
                group_keys = [settings.MOOC_GROUP_KEY, settings.SPOC_GROUP_KEY]
            else:
                group_keys = [settings.SPOC_GROUP_KEY]
        elif service_line == 'mooc':
            group_keys = [settings.MOOC_GROUP_KEY]
        else:
            group_keys = [settings.SPOC_GROUP_KEY]

        return group_keys

    def get_course_health(self, course_ids, group_keys, size):
        query = self.es_query(index = settings.ES_INDEX, doc_type = 'course_health')\
                            .filter('terms', course_id = course_ids)\
                            .filter('terms', group_key = group_keys)\
                            .source(COURSE_HEALTH_FIELD)
        course_health = self.es_execute(query[:size]).hits
        course_health = [item.to_dict() for item in course_health]
        teacher_power = MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).get_courses(self.user_id)
        for course in course_health:
            for k,v in course.items():
                course[k] = v or 0
            course['status'] = 0
            course['school'] = course.pop('group_name')
            group_keys = teacher_power.get(course['course_id'], [])
            if course['group_key'] in group_keys:
                course['status'] = 1
        return course_health

    def get(self):
        course_name = self.get_argument('course_name', 'all')
        page = int(self.get_argument('page', 1))
        num = int(self.get_argument('num', 10))
        service_line = self.get_argument('service_line') 
        service_line = json.loads(service_line)
        parent_id = self.parent_id
        host = self.host
        course_ids_total, _ = self.get_children_id_open_times(parent_id)
        size = len(course_ids_total)
        courses = copy.deepcopy(course_ids_total)
        course_ids, course_info = self.get_course_infos(parent_id, course_ids_total, course_name, page, num, service_line, host)
        _, total = self.get_course_aggs(courses, service_line, host)
        course_info_num = len(course_ids)
        group_keys = self.group_keys

        if num == -1:
            course_health = self.get_course_health(course_ids, group_keys, size)
        else:
            course_health = self.get_course_health(course_ids, group_keys, size)
        course_info = self.update_course_info(service_line, course_info, course_health, parent_id, num)
        
        load_more = 0
        if (page-1)*num + course_info_num < total:
            load_more = 1

        self.success_response({'data': course_info, 'load_more': load_more, 'page': page, 'size': num})


