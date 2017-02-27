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

        #获取page,size,status因为tap层已经有默认值，所以此处不必再设默认值
        page = int(self.get_argument("page"))
        size = int(self.get_argument("size"))
        status = self.get_argument("status")

        now = datetime.datetime.utcnow()
        
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

        results = self.es_execute(query[:total])
        results = results.hits
        
        #得到课程ID列表和每个课程对应group_key list
        powers_dict  = {}
        course_ids = []
        for result in results:
            if result.course_id not in powers_dict:
                course_ids.append(result.course_id)
                powers_dict[result.course_id] = []
            power = result.group_key
            powers_dict[result.course_id].append(power)
        
        #获取指定数量的课程id dict
        powers = {}
        for course_id in course_ids[(page-1)*size:page*size]:
            powers[course_id]=powers_dict[course_id]
        
        course_result = {}
        course_result['powers'] = powers
        course_result['total'] = total
        course_result['total_page'] = total_page
        course_result['load_more'] = load_more
        
        self.success_response({'powers': course_result})
