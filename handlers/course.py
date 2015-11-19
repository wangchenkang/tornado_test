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
                    'filter': {
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
