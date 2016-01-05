#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log


Log.create('student')

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


@route('/student/course_grade')
class StudentCourseGrade(BaseHandler):
    """
    获取课程学生成绩
    """
    def get(self):
        course_id = self.course_id
        user_id = self.get_argument('user_id', None)

        query = self.es_query(index='main', doc_type='student_course_grade') \
                .filter('term', course_id=course_id)

        if user_id:
            students = [u.strip() for u in user_id.split(',') if u.strip()]
            if students:
                query = query.filter('terms', user_id=students)

        default_size = 100000
        data = self.es_execute(query[:default_size])
        if data.hits.total > default_size:
            data = self.es_execute(query[:data.hits.total])

        result = []
        for item in data:
            result.append({
                'user_id': item.user_id,
                'grade': item.grade
            })

        self.success_response({
            'data': result,
            'total': data.hits.total
        })


@route('/student/study_sutdent_list')
class StudyStudentList(BaseHandler):
    """
    获取章学生列表
    """
    # TODO: 未完成
    def get(self):
        course_id = self.course_id
        chapter_id = self.get_argument('chapter_id', None)

        default_size = 100000

        grade_query = self.search(index='main', doc_type='student_grade')
        grade_query.filter('term', course_id=course_id)
        if chapter_id is not None:
            grade_query.filter('term', chapter_id=chapter_id)
        grade_result = grade_query.execute()

        Log.error(grade_result)

        self.success_response({'students': ''})
