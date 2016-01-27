#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/service/status')
class ServiceStatus(BaseHandler):
    def get(self):
        self.success_response({'status': 'ok'})


class NotFoundHandler(BaseHandler):
    def get(self):
        self.error_response(100, 'Not Found')
