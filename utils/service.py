#! -*- coding: utf-8 -*-
import requests
import urllib
from requests.exceptions import ConnectionError, ConnectTimeout
from tornado.escape import json_decode
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from .log import Log
import settings
import datetime

Log.create('service')

class AsyncService(object):
    """
    """
    def __init__(self):
        self.client = AsyncHTTPClient()

    def _get_host(self):
        return settings.data_service_api

    def _build_url(self, api):
        host = self._get_host().lower()
        if not host.startswith('http://'):
            host = 'http://' + host
        return '{}/{}'.format(host.rstrip('/'), api.lstrip('/'))

    def get(self, url, params):
        request_url = self._build_url(url) + '?' + urllib.urlencode(params)
        return self.client.fetch(request_url, request_timeout = 500)

    def post(self, url, params):
        request_url = self._build_url(url)
        params = urllib.urlencode(params)
        return self.client.fetch(request_url, method='POST', body=params, request_timeout = 500)

    @classmethod
    def parse_response(cls, response):
        if response.error:
            Log.error('service response error: %s' % response.error)
            raise ServiceError
        else:
            try:
                data = json_decode(response.body)
            except ValueError, e:
                Log.error('service response error: %s, content: %s' % (e, content))
                raise ServiceError
        return data


class AsyncCourseService(AsyncService):
    def _get_host(self):
        return settings.course_service_api


class ServiceRequest(object):

    @classmethod
    def _get_host(cls):
        raise NotImplementedError

    @classmethod
    def _build_url(cls, api):
        api_host = cls._get_host()
        if api_host.startswith('http://'):
            host = api_host.rstrip('/')
        else:
            host = 'http://{}'.format(api_host).rstrip('/')

        return '{}/{}'.format(host, api.lstrip('/'))

    @classmethod
    def _parse_response(cls, content):
        try:
            data = json_decode(content)
        except ValueError, e:
            Log.error('service response error: %s, content: %s' % (e, content))
            return None
        return data

    @classmethod
    def get(cls, api, params={}):
        try:
            url = cls._build_url(api)
            response = requests.get(url, params)
        except (ConnectionError, ConnectTimeout):
            Log.error('service request error, url: %s' % url)
            return None
        return cls._parse_response(response.content)

    @classmethod
    def get_raw(cls, api, params={}):
        try:
            url = cls._build_url(api)
            response = requests.get(url, params)
            return response
        except (ConnectionError, ConnectTimeout):
            Log.error('service request error, url: %s' % url)
            return None

    @classmethod
    def post(cls, api, params={}):
        try:
            url = cls._build_url(api)
            response = requests.post(url, params)
        except (ConnectionError, ConnectTimeout):
            Log.error('service request error, url: %s' % url)
            return None
        return cls._parse_response(response.content)

    @classmethod
    def post(cls, api, params={}):
        try:
            url = cls._build_url(api)
            response = requests.post(url, params)
        except (ConnectionError, ConnectTimeout):
            Log.error('service request error, url: %s' % url)
            return None
        return cls._parse_response(response.content)


class CourseService(ServiceRequest):
    """
    course service api requests
    """
    @classmethod
    def _get_host(cls):
        return settings.course_service_api

