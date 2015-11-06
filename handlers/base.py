#! -*- coding: utf-8 -*-
import json
from tornado.web import RequestHandler, Finish
from tornado.options import options


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
