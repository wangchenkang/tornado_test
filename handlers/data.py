#! -*- coding: utf-8 -*-
import hashlib
from .base import BaseHandler
from utils.routes import route


@route('/data/export')
class DataExport(BaseHandler):
    """ 
    获取导出文件记录
    """
    def get(self):
        course_id = self.course_id
        data_type = self.get_param('data_type')
        
        data_type = [item.strip() for item in data_type.split(',') if item.strip()]

        data = {}
        for item in data_type:
            data_id = hashlib.md5(course_id + item).hexdigest()
            record = self.es.get(index='dataimport', doc_type='course_data', id=data_id)

            try:
                data[item] = {
                    'course_id': record['_source']['course_id'],
                    'data_type': record['_source']['data_type'],
                    'name': record['_source']['name'],
                    'link': record['_source']['data_link'],
                    'update_time': record['_source']['update_time']
                }
            except KeyError:
                data[item] = {}


        self.success_response({'data': data})


