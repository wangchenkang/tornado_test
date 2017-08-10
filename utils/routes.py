#! -*- coding: utf-8 -*-
import tornado.web


class route(object):
    """
    decorates RequestHandler
    """

    _routes = {}

    def __init__(self, uri, name=None):
        self.uri = uri
        self.name = name
    
    def __call__(self, _handler):
        name = self.name or _handler.__name__
        route_path = route_path_convert('{}:{}'.format(_handler.__module__, _handler.__name__))
        self._routes[route_path] = tornado.web.url(self.uri, _handler, name=name)
        return _handler

    @classmethod
    def get_routes(cls):
        return cls._routes.values()

    @classmethod
    def get_route(cls, path):
        return cls._routes[path]

def route_path_convert(*args):
    name = []
    for item in args:
        for s in item:
            if s == '.':
                name.append('_')
            else:
                name.append(s if s.islower() else '_{}'.format(s.lower()))

    return ''.join(name)
