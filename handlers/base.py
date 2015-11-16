#! -*- coding: utf-8 -*-
import json
from tornado.web import RequestHandler, Finish
from tornado.options import options
from elasticsearch import ConnectionError, ConnectionTimeout, RequestError


class BaseHandler(RequestHandler):
    
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

    def error_response(self, error):
        data = {
            'success': False,
            'error': error
        }

        self.write_json(data)
        raise Finish

    def success_response(self, data):
        data.update({
            'success': True,
            'error': ''
        })

        self.write_json(data)
        raise Finish

    @property
    def course_id(self):
        course_id = self.get_argument('course_id', None)
        if course_id is None:
            self.error_response(u'参数错误')
        return course_id

    @property
    def chapter_id(self):
        chapter_id = self.get_argument('chapter_id', None)
        if chapter_id is None:
            self.error_response(u'参数错误')
        return chapter_id

    def get_param(self, key, default=None):
        param = self.get_argument(key, default)
        if param is None:
            self.error_response(u'参数错误')
        return param

    def es_search(self, **kwargs):
        try:
            response = self.es.search(**kwargs)
        except (ConnectionError, ConnectionTimeout):
            self.error_response(u'Elasticsearch 连接错误')
        except RequestError, e:
            self.error_response(u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response
