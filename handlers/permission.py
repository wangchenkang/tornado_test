#! -*- coding: utf-8 -*-

from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler
import settings

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
        query = self.es_query(index='tap', doc_type='teacher_power') \
                .filter('term', user_id=str(self.user_id)) \
                #.filter('terms', course_id=['course-v1:TsinghuaX+00690092X+2016_T1','YunTech/YunTech001/2015_T1'])
        
        query_test = self.es_query(index='tap', doc_type='teacher_power')\
                     .filter('term', user_id=str(self.user_id))\
                     .filter('term', course_id='course-v1:TsinghuaX+00690092X+2016_T1')
        result_test = self.es_execute(query_test[(page-1)*size:page*size])
        hits_test = result_test.hits

        result = self.es_execute(query[(page-1)*size:page*size])
        hits = result.hits
        hits.insert(0, result_test.hits[0])

        powers = {}
        for hit in hits:
            if hit.course_id not in powers:
                powers[hit.course_id] = []
            if settings.MOOC_GROUP_KEY == hit.group_key:
                power = {'group_key': hit.group_key, 'type': 'mooc'}
            else:
                power = {'group_key': hit.group_key, 'type': 'mooc_org'}
            powers[hit.course_id].append(power)
        self.success_response({'powers': powers})
