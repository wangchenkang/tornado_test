#! -*- coding: utf-8 -*-
import json
import hashlib
from tornado.web import RequestHandler, Finish, MissingArgumentError
from tornado.options import options
from tornado.escape import url_unescape, json_encode
from elasticsearch import ConnectionError, ConnectionTimeout, RequestError
from elasticsearch_dsl import Search, F
from utils.tools import fix_course_id


class BaseHandler(RequestHandler):
    def write_error(self, status_code, **kwargs):
        error_msg = '{}:{}'.format(status_code, self._reason)
        self.set_status(200)
        self.error_response(100, error_msg)

    @property
    def es(self):
        return self.application.es

    @property
    def memcache(self):
        return self.application.memcache

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

    @property
    def elective(self):
        elective = self.get_argument('elective', None)
        if elective == "None":
            elective = None
        #elective = '西安交通大学'
        return elective

    @property
    def group_id(self):
        group_id = self.get_argument('group_id', None)
        if group_id == "None":
            group_id = None
        if group_id:
            group_id = int(group_id)
        return group_id

    def get_param(self, key):
        try:
            param = self.get_argument(key)
        except MissingArgumentError:
            self.error_response(200, u'参数错误')

        try:
            return url_unescape(param)
        except TypeError:
            return param

    def es_search(self, **kwargs):
        try:
            response = self.es.search(**kwargs)
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response

    def es_query(self, **kwargs):
        if 'index' not in kwargs:
            kwargs['index'] = 'tap'
        return Search(using=self.es, **kwargs)

    def es_execute(self, query):
        try:
            response = query.execute()
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response

    def get_enroll(self, group_id=None, course_id=None):
        query = self.es_query(doc_type='course')
        #if elective:
        #    query = query.filter('term', elective=elective)
        #else:
        #    query = query.filter(~F('exists', field='elective'))
        if course_id:
            query = query.filter('term', course_id=course_id)
        hits = self.es_execute(query[:100]).hits
        if hits.total > 100:
            hits = self.es_execute(query[:hits.total]).hits
        if course_id:
            if hits.total:
                return hits[0].enroll_num
            else:
                return 0
        else:
            return dict([(hit.course_id, int(hit.enroll_num)) for hit in hits])

    def get_users(self, is_active=True):
        hashstr = "student" + self.course_id + (str(self.group_id) or "") + str(is_active)
        hashcode = hashlib.md5(hashstr).hexdigest()
        #users = self.memcache.get(hashcode)
        #if users:
        #    return users
        query = self.es_query(doc_type='student')\
                .fields(fields="user_id")
        if self.group_id:
            query = query.filter('term', group_id=self.group_id)
        #else:
        #    query = query.filter(~F('exists', field='group_id'))
        if is_active:
            query = query.filter('term', is_active=1)
        elif is_active == False:
            query = query.filter('term', is_active=0)

        if self.course_id:
            query = query.filter('term', course_id=self.course_id)

        size = self.es_execute(query[:0]).hits.total
        size = 100000
        hits = self.es_execute(query[:size]).hits
        users = [hit.user_id[0] for hit in hits]
        self.memcache.set(hashcode, users, 60*60)
        return users

    def get_problem_users(self):
        hashstr = "problem_student" + self.course_id + (str(self.group_id) or "")
        hashcode = hashlib.md5(hashstr).hexdigest()
        users = self.memcache.get(hashcode)
        if users:
            return users
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='problem')\
                .filter("term", course_id=self.course_id)\
                .filter("terms", user_id=users)
        query.aggs.bucket("p", "terms", field="user_id", size=0)
        results = self.es_execute(query[:0])
        aggs = results.aggregations["p"]["buckets"]
        users = [item["key"] for item in aggs]
        self.memcache.set(hashcode, users, 60*60)
        return users

    def get_video_users(self):
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='video')\
                .filter("term", course_id=self.course_id)\
                .filter("terms", user_id=users)
        query.aggs.bucket("p", "terms", field="user_id", size=0)
        results = self.es_execute(query[:0])
        aggs = results.aggregations["p"]["buckets"]
        return [item["key"] for item in aggs]

    def search(self, **kwargs):
        response = Search(using=self.es, **kwargs)
        return response

    def get_user_name(self, users=None):
        if not users:
            users = self.get_users()
        query = self.es_query(index='tap', doc_type='student')\
                .filter("term", course_id=self.course_id)\
                .filter("terms", user_id=users)\
                .fields(fields=["rname", "nickname", "user_id"])
        if self.group_id:
            query = query.filter('term', group_id=self.group_id)
        else:
            query = query.filter(~F('exists', field='group_id'))
        results = self.es_execute(query[:len(users)]).hits
        result = {}
        for item in results:
            user_id = item.user_id[0]
            if item.rname and item.rname[0] != "":
                name = item.rname[0]
            else:
                name = item.nickname[0]
            result[user_id] = name
        return result

    def get_grade(self, users=None):
        if not users:
            users = self.get_users()
        query = self.es_query(index='tap', doc_type='problem_course')\
                .filter("term", course_id=self.course_id)\
                .filter("terms", user_id=users)
        results = self.es_execute(query[:len(users)])
        result = {}
        for item in results:
            if item.grade_ratio < 0:
                item.grade_ratio = 0
            result[item.user_id] = item.grade_ratio
        return result
