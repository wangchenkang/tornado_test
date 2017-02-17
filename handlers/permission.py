#! -*- coding: utf-8 -*-

from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler
import settings
import datetime

Log.create('permission')

@route('/permission/teacher_permission')
class TeacherPermission(BaseHandler):
    """
    教师权限接口
    """
    def get(self):
        #获取page,size,因为tap层已经有默认值，所以此处不必再设默认值
        page = int(self.get_argument("page"))
        size = int(self.get_argument("size"))
        
        now = datetime.datetime.utcnow()
        status = self.get_argument("status")
        
        query = self.es_query(index='tap', doc_type='teacher_power')\
                .filter('term', user_id=str(self.user_id)).sort("-start")
        #开课
        if status == "process":
            query = query.filter('range', **{'start': {'lt': now}})\
                    .filter('range', **{'end': {'gt': now}}).sort("-start")
        #结课
        if status == "close":
            query = query.filter('range', **{'end': {'lte': now}}).sort("-end")
        #即将开课
        if status == "unopen":
            query = query.filter('range', **{'start': {'gt': now}}).sort("start")

        total = self.es_execute(query[:0]).hits.total
        if total%size != 0:
            total_page = total/size + 1
        else:
            total_page = total/size

        if page*size >= total:
            load_more = 0
        else:
            load_more = 1

        result_size = self.es_execute(query[:0]).hits.total
        query_results = self.es_execute(query[:result_size])
        query_results = query_results.hits
        
        powers_dict  = {}
        course_ids = []
        for query_result in query_results:
            if query_result.course_id not in powers_dict:
                course_ids.append(query_result.course_id)
                powers_dict[query_result.course_id] = []
            power = query_result.group_key
            powers_dict[query_result.course_id].append(power)
        
        powers = {}
        for course_id in course_ids[(page-1)*size:page*size]:
            powers[course_id]=powers_dict[course_id]
        powers['course-v1:TsinghuaX+00690092X+2016_T1'] = powers_dict['course-v1:TsinghuaX+00690092X+2016_T1']
        
        course_result = {}
        course_result['powers'] = powers
        course_result['total'] = total
        course_result['total_page'] = total_page
        course_result['load_more'] = load_more
        
        self.success_response({'powers': course_result})
