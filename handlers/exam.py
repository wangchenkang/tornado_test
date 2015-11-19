#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/exam/students_stat')
class StudentStat(BaseHandler):
    """
    考试章学生数统计
    """
    def get(self):
        course_id = self.course_id
        sequential_id = self.get_param('sequential_id')
        group_max_number = 10000

        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'seq_id': sequential_id}},
                                {'term': {'librayc_id': '-1'}}
                            ]
                        }
                    }
                }
            },
            'size': group_max_number
        }
        # 取没有group的学生数
        data = self.es_search(index='main', doc_type='libc_stu_num_exam', body=query)
        try:
            exam_total = data['hits']['hits'][0]['_source']['user_num']
        except (KeyError, IndexError):
            exam_total = 0

        # 取group后各个分数段的学生数
        data = self.es_search(index='main', doc_type='libc_group_stu_num_exam', body=query)
        # 确保不会少查询数据
        if data['hits']['total'] > group_max_number:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='libc_group_stu_num_exam', body=query)

        groups = {}
        for item in data['hits']['hits']:
            groups[item['_source']['group_id']] = {
                'group_id': item['_source']['group_id'],
                'user_num': item['_source']['user_num']
            }

        self.success_response({'exam_student_num': exam_total, 'groups': groups})


@route('/exam/content_students_stat')
class ContentStudentStat(BaseHandler):
    """
    考试章内容(library content)学生统计
    """
    def get(self):
        course_id = self.course_id
        sequential_id = self.get_param('sequential_id')
        group_max_number = 10000

        query = {
            'query': {
                'filtered': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'seq_id': sequential_id}},
                            ]
                        }
                    },
                    'filter': {
                        'not': {
                            'filter': {
                                'term': {'librayc_id': '-1'}
                            }
                        }
                    }
                }
            },
            'size': group_max_number
        }

        content = {}
        data = self.es_search(index='main', doc_type='libc_group_stu_num_exam', body=query)
        for item in data['hits']['hits']:
            content.setdefault(item['_source']['librayc_id'], {})
            content[item['_source']['librayc_id']][item['_source']['group_id']] = {
                'block_id': item['_source']['librayc_id'],
                'group_id': item['_source']['group_id'],
                'user_num': item['_source']['user_num']
            }

        self.success_response({'content_groups': content})
