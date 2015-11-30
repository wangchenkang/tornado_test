#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/student/binding_org')
class StudentOrg(BaseHandler):
    """ 
    获取学堂选修课学生列表
    """
    def get(self):
        course_id = self.course_id
        org = self.get_param('org')
    
        default_size = 100000
        query = { 
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'courses': course_id}},
                                {'term': {'binding_org': org}},
                            ]   
                        }   
                    }   
                }   
            },  
            'size': default_size
        }   

        data = self.es_search(index='main', doc_type='student', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='student', body=query)

        students = []
        for item in data['hits']['hits']:
            students.append({
                'user_id': int(item['_source']['uid']),
                'binding_id': item['_source'].get('binding_uid', None),
                'binding_org': item['_source'].get('binding_org', None),
            })

        self.success_response({'students': students})


