#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route


@route('/video')
class VideoHandler(BaseHandler):
    def get(self):
        self.write('ok')
