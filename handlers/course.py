#! -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from .base import BaseHandler
from utils.routes import route
from utils.tools import utc_to_cst, date_to_query


@route('/course/week_activity')
class CourseActivity(BaseHandler):
    """
    课程7日活跃统计
    """
    def get(self):
        course_id = self.course_id
        yestoday = utc_to_cst(datetime.utcnow() - timedelta(days=1))
        date = self.get_argument('date', date_to_query(yestoday))
        
        query = { 
            'query': {
                'filtered': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'date': date}},
                            ]
                        }
                    }
                }
            },
            'size': 1
        }

        data = self.es_search(index='api1', doc_type='course_week_active', body=query)

        try:
            source = data['hits']['hits'][0]['_source']
            result = {
                'active_num': source['active_num'],
                'effective_num': source['effective_student_num'],
                'date': source['date'],
                'percent': '{:0.4f}'.format(float(source['percent']))
            }
        except IndexError:
            result = {}

        self.success_response({'data': result})


@route('/course/enrollments')
class CourseEnrollments(BaseHandler):
    def get(self):
        default_max_size = 100000
        query = { 
            'query': {
                'filtered': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'courses': self.course_id}},
                            ]
                        }
                    }
                }
            },
            'size': default_max_size
        }

        students = []
        data = self.es_search(index='main', doc_type='student', body=query)
        if data['hits']['total'] > default_max_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='student', body=query)

        for item in data['hits']['hits']:
            students.append(int(item['_source']['uid']))

        self.success_response({'students': students})
