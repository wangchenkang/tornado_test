#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/exam/examed_students')
class ExamedStudent(BaseHandler):
    """ 
    获取考试学生列表
    """
    def get(self):
        course_id = self.course_id
        sequential_id = self.get_param('sequential_id')

        query = { 
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'seq_id': sequential_id}}
                            ]
                        }
                    }
                }
            },
            'sort': [
                {'max_score': {'order': 'desc'}}
            ],
            'size': 0
        }
        data = self.es_search(index='main', doc_type='seq_stu_grade_exam', body=query, search_type='count')
        query['size'] = data['hits']['total']
        data = self.es_search(index='main', doc_type='seq_stu_grade_exam', body=query)
    
        students = set()
        for item in data['hits']['hits']:
            students.add(int(item['_source']['user_id']))

        self.success_response({'students': list(students)})


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
        # 获取各个考试章library_content 下学生数
        data = self.es_search(index='main', doc_type='libc_stu_num_exam', body=query)
        if data['hits']['total'] > group_max_number:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='libc_stu_num_exam', body=query)

        for item in data['hits']['hits']:
            content.setdefault(item['_source']['librayc_id'], {})
            content[item['_source']['librayc_id']]['student_num'] = item['_source']['user_num']

        # 获取各个考试章library_content 下各分数段的学生数
        data = self.es_search(index='main', doc_type='libc_group_stu_num_exam', body=query)
        if data['hits']['total'] > query['size']:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='libc_group_stu_num_exam', body=query)

        for item in data['hits']['hits']:
            content.setdefault(item['_source']['librayc_id'], {}).setdefault('groups', {})
            content[item['_source']['librayc_id']]['groups'][item['_source']['group_id']] = {
                'block_id': item['_source']['librayc_id'],
                'group_id': item['_source']['group_id'],
                'user_num': item['_source']['user_num']
            }

        self.success_response({'content_groups': content})


@route('/exam/students_grade')
class StudentGrade(BaseHandler):
    """
    获取学生考试成绩
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')
        user_id = self.get_argument('user_id', None)

        defautl_max_size = 100000
        query = {
            'query': {
                'filtered': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': self.course_id}},
                                {'term': {'seq_id': sequential_id}},
                            ]
                        }
                    }
                }
            },
            'size': defautl_max_size
        }

        if user_id is not None:
            students = [u.strip() for u in user_id.split(',')]
            query['query']['filtered']['query']['bool']['must'].append({'terms': {'user_id': students}})

        data = self.es_search(index='main', doc_type='seq_stu_grade_exam', body=query)
        if data['hits']['total'] > defautl_max_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='seq_stu_grade_exam', body=query)

        students_grade = {}
        for item in data['hits']['hits']:
            students_grade[item['_source']['user_id']] = {
                'user_id': int(item['_source']['user_id']),
                'grade': item['_source']['grade'],
                'max_score': item['_source']['max_score'],
                'grade_percent': item['_source']['grade_percent'],
            }

        self.success_response({'students_grade': students_grade})


@route('/exam/students_grade_detail')
class StudentGradeDetail(BaseHandler):
    """
    获取学生考试成绩详细信息
    """
    def get(self):
        sequential_id = self.get_param('sequential_id')
        user_id = self.get_argument('user_id', None)

        defautl_max_size = 100000
        query = {
            'query': {
                'filtered': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': self.course_id}},
                                {'term': {'seq_id': sequential_id}},
                            ]
                        }
                    }
                }
            },
            'size': defautl_max_size
        }

        if user_id is not None:
            students = [u.strip() for u in user_id.split(',')]
            query['query']['filtered']['query']['bool']['must'].append({'terms': {'user_id': students}})

        data = self.es_search(index='main', doc_type='lib_stu_grade_exam', body=query)
        if data['hits']['total'] > defautl_max_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='lib_stu_grade_exam', body=query)

        students_grade = {}
        for item in data['hits']['hits']:
            students_grade.setdefault(item['_source']['user_id'], {})
            students_grade[item['_source']['user_id']][item['_source']['librayc_id']] = {
                'user_id': int(item['_source']['user_id']),
                'grade': item['_source']['grade'],
                'max_score': item['_source']['max_score'],
                'grade_percent': item['_source']['grade_percent'],
                'seq_format': item['_source']['seq_format'],
                'content_id': item['_source']['librayc_id']
            }

        self.success_response({'students_grade': students_grade})

