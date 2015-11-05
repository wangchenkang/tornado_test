#! -*- coding: utf-8 -*-
from tornado.web import RequestHandler


class BaseHandler(RequestHandler):
    
    @property
    def es(self):
        return self.application.es
