#!/usr/bin/env python
#! -*- coding: utf-8 -*-
import os
import tornado.httpserver
import tornado.options
import tornado.web
from tornado.options import define, options
from elasticsearch import Elasticsearch
import settings
from utils.routes import route
from utils.log import Log
from handlers import *

define('port', default=8001, help='run port', type=int)
define('debug', default=True, help='debug', type=bool)

Log.create(__name__)

class Application(tornado.web.Application):
    def __init__(self):
        app_settings = {
            'title': settings.PROJECT_NAME,
            'template_path': os.path.join(os.path.dirname(__file__), 'templates'),
            'static_path': os.path.join(os.path.dirname(__file__), 'static'),
            'cookie_secret': settings.COOKIE_SECRET,
            'debug': options.debug,
        }

        routed_handlers = route.get_routes()
        if options.debug:
            routed_handlers.append(
                tornado.web.url(r'/static/(.*)', tornado.web.StaticFileHandler, {'path': app_settings['static_path']}))

        self.es = Elasticsearch(settings.es_cluster)

        super(Application, self).__init__(routed_handlers, **app_settings)

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    Log.debug('server start on %s' % options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
