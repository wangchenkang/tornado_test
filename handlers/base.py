#! -*- coding: utf-8 -*-
import json
from tornado.web import RequestHandler, Finish
from tornado.options import options
from tornado.escape import url_unescape, json_encode
from elasticsearch import ConnectionError, ConnectionTimeout, RequestError
from utils.tools import fix_course_id


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

    def get_param(self, key, default=None):
        param = self.get_argument(key, default)
        if param is None:
            self.error_response(200, u'参数错误')
        return url_unescape(param)

    def es_search(self, **kwargs):
        try:
            response = self.es.search(**kwargs)
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError, e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response
