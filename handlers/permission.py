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
        query = self.es_query(index='tap', doc_type='teacher_power') \
                .filter('term', user_id=str(self.user_id))

        result = self.es_execute(query)
        result = self.es_execute(query[:result.hits.total])
        hits = result.hits

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
