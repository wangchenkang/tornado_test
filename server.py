#!/usr/bin/env python
#! -*- coding: utf-8 -*-
import os
import tornado.httpserver
import tornado.options
import tornado.web
from tornado.options import define, options
from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
import settings
from utils.log import Log
from handlers import *
import memcache

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
        routed_handlers.append(
            (r'.*', NotFoundHandler)
        )

        if options.debug:
            routed_handlers.append(
                tornado.web.url(r'/static/(.*)', tornado.web.StaticFileHandler, {'path': app_settings['static_path']}))

        self.es = connections.create_connection(hosts=settings.es_cluster, timeout=30, max_retries=4)
        self.memcache = memcache.Client(settings.memcache_host)

        super(Application, self).__init__(routed_handlers, **app_settings)

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    Log.debug('server start on %s' % options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
