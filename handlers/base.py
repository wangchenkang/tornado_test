#! -*- coding: utf-8 -*-
import json
from tornado.web import RequestHandler, Finish, MissingArgumentError
from tornado.options import options
from tornado.escape import url_unescape, json_encode
from elasticsearch import ConnectionError, ConnectionTimeout, RequestError
from elasticsearch_dsl import Search, F
from utils.tools import fix_course_id


class BaseHandler(RequestHandler):
    def write_error(self, status_code, **kwargs):
        error_msg = '{}:{}'.format(status_code, self._reason)
        self.set_status(200)
        self.error_response(100, error_msg)

    @property
    def es(self):
        return self.application.es

    def write_json(self, data):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        if options.debug:
            json_data = json.dumps(data, indent=2)
        else:
            json_data = json_encode(data)

        self.write(json_data)

    def error_response(self, error_code, error_msg):
        data = {
            'error_code': error_code,
            'error_msg': error_msg
        }

        self.write_json(data)
        raise Finish

    def success_response(self, data):
        data.update({
            'error_code': 0,
            'error_msg': ''
        })

        self.write_json(data)
        raise Finish

    @property
    def course_id(self):
        course_id = self.get_argument('course_id', None)
        if course_id is None:
            self.error_response(200, u'参数错误')
        return fix_course_id(course_id)

    @property
    def chapter_id(self):
        chapter_id = self.get_argument('chapter_id', None)
        if chapter_id is None:
            self.error_response(200, u'参数错误')
        return chapter_id

    @property
    def elective(self):
        elective = self.get_argument('elective', None)
        return elective


    def get_param(self, key):
        try:
            param = self.get_argument(key)
        except MissingArgumentError:
            self.error_response(200, u'参数错误')

        try:
            return url_unescape(param)
        except TypeError:
            return param

    def es_search(self, **kwargs):
        try:
            response = self.es.search(**kwargs)
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response

    def es_query(self, **kwargs):
        if 'index' not in kwargs:
            kwargs['index'] = 'tap'
        return Search(using=self.es, **kwargs)

    def es_execute(self, query):
        try:
            response = query.execute()
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response
 
    def get_enroll(self, elective=None, course_id=None):
        query = self.es_query(doc_type='course')
        if elective:
            query = query.filter('term', elective=elective)
        else:
            query = query.filter(~F('exists', field='elective'))
        if course_id:
            query = query.filter('term', course_id=course_id)
        hits = self.es_execute(query[:100]).hits
        if hits.total > 100:
            hits = self.es_execute(query[:hits.total]).hits
        if course_id:
            return hits[0].enroll_num
        else:
            return dict([(hit.course_id, int(hit.enroll_num)) for hit in hits])
    
    def get_users(self):
        query = self.es_query(doc_type='student')
        if self.elective:
            query = query.filter('term', elective=elective)
        else:
            query = query.filter(~F('exists', field='elective'))
        if self.course_id:
            query = query.filter('term', course_id=course_id)
        size = self.es_execute(query[:0]).hits.total
        hits = self.es_execute(query[:size]).hits
        return [hit.user_id for hit in hits]
        
