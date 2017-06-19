#! -*- coding: utf-8 -*-
import requests
import urllib
from requests.exceptions import ConnectionError, ConnectTimeout
from tornado.escape import json_decode
from .log import Log
import settings
import datetime

Log.create('service')


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

