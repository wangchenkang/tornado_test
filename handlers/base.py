#! -*- coding: utf-8 -*-
import json
import hashlib
from tornado.web import RequestHandler, Finish, MissingArgumentError
from tornado.options import options
from tornado.escape import url_unescape, json_encode
from elasticsearch import ConnectionError, ConnectionTimeout, RequestError
from elasticsearch_dsl import Search, Q
from utils.tools import fix_course_id
from utils.tools import get_group_type
import settings

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

    def get_hash_key(self, key):
        hash_key = hashlib.md5(key).hexdigest()
        return hash_key

    def get_memcache_data(self, key):
        hash_key = self.get_hash_key(key)
        result = self.memcache.get(hash_key)
        return hash_key, result

    def set_memcache_data(self, hash_key, result):
        self.memcache.set(hash_key, result, 60*60)
        return True

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
    def user_id(self):
        user_id = self.get_argument('user_id', None)
        if user_id is None:
            self.error_response(200, u'参数错误')
        return user_id

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
    def group_key(self):
        group_key = self.get_argument('group_key', settings.MOOC_GROUP_KEY)
        if group_key == "None":
            group_key = settings.MOOC_GROUP_KEY
        if group_key:
            group_key = int(group_key)
        return group_key

    @property
    def group_type(self):
        return get_group_type(self.group_key)

    @property
    def group_name(self):
        query = self.es_query(doc_type='course_community') \
            .filter('term', course_id=self.course_id) \
            .filter('term', group_key=self.group_key)
        result = self.es_execute(query)
        return result.hits[0].group_name if result.hits else 'xuetangx'

    @property
    def course_group_key(self):
        course_group_key = self.get_argument("course_group_key", None)
        if course_group_key is None:
            self.error_response(200, u"参数错误")
        else:
            return course_group_key

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
            kwargs['index'] = settings.ES_INDEX

        return Search(using=self.es, **kwargs)

    def es_execute(self, query):
        try:
            response = query.execute()
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response

    def get_enroll(self, group_key=None, course_id=None):
        query = self.es_query(doc_type='student_enrollment_info') \
            .filter('term', is_active=1)
        if group_key:
            query = query.filter('term', group_key=group_key)
        if course_id:
            query = query.filter('term', course_id=course_id)
            hits = self.es_execute(query).hits
            return hits.total if hits.total else 0
        query.aggs.bucket('course', 'terms', field='course_id', size=10000)
        result = self.es_execute(query)
        course_num = {}
        for course in result.aggregations.course.buckets:
            course_num[course.key] = course.doc_count
        return course_num
    
    def get_student_num(self, course_group_key=None):
    
        result = {}
        if not course_group_key:
            return result
        else:
            for course_id, group_key_list in eval(course_group_key).items():
                if course_id not in result.keys():
                    result[course_id] = []
                for group_key in group_key_list:
                    course_depend_group_key = {}
                    query = self.es_query(doc_type='course_community')\
                            .filter('term', course_id=course_id)\
                            .filter('term', group_key=group_key)
                    hits = self.es_execute(query).hits
                    if hits.total > 0:
                        course_depend_group_key["enroll_num"] = hits[0].enroll_num or 0
                        course_depend_group_key["course_type"] = hits[0].course_type
                        course_depend_group_key["group_key"] = group_key
                    result[course_id].append(course_depend_group_key)
        return result

    def get_users(self, is_active=True):
        hashstr = "student" + self.course_id + (str(self.group_key) or "") + str(is_active)
        hashcode = hashlib.md5(hashstr).hexdigest()
        users = self.memcache.get(hashcode)
        if users:
            return users
        query = self.es_query(doc_type='student_enrollment_info')#\
                # .fields(fields="user_id")
        if self.group_key:
            query = query.filter('term', group_key=self.group_key)
        #else:
        #    query = query.filter(~F('exists', field='group_key'))
        if is_active:
            query = query.filter('term', is_active=1)
        elif is_active == False:
            query = query.filter('term', is_active=0)

        if self.course_id:
            query = query.filter('term', course_id=self.course_id)

        size = self.es_execute(query[:0]).hits.total
        size = 50000
        hits = self.es_execute(query[:size]).hits
        users = [hit.user_id for hit in hits]
        self.memcache.set(hashcode, users, 60*60)
        return users
    
    def get_owner(self):
        query = self.es_query(doc_type="course_community")\
                            .filter('term', course_id=self.course_id)\
                            .filter('term', group_key=self.group_key)
        hits = self.es_execute(query[:1]).hits
        owner = hits[0].owner
        return owner

    def get_problem_users(self):
        #hashstr = "problem_student" + self.course_id + (str(self.group_key) or "")
        #hashcode = hashlib.md5(hashstr).hexdigest()
        #users = self.memcache.get(hashcode)
        #if users:
        #    return users
        users = self.get_users()
        query = self.es_query(doc_type='study_problem')\
                .filter("term", course_id=self.course_id)\
                .filter("term", group_key=self.group_key) \
                .filter("terms", user_id=users)
        query.aggs.bucket("p", "terms", field="user_id", size=50000)
        results = self.es_execute(query[:0])
        aggs = results.aggregations["p"]["buckets"]
        users = [item["key"] for item in aggs]
        #self.memcache.set(hashcode, users, 60*60)
        return users

    def get_video_users(self):
        users = self.get_users()
        query = self.es_query(doc_type='study_video')\
                .filter("term", course_id=self.course_id)\
                .filter("term", group_key=self.group_key) \
                .filter("terms", user_id=users)
        query.aggs.bucket("p", "terms", field="user_id", size=50000)
        results = self.es_execute(query[:0])
        aggs = results.aggregations["p"]["buckets"]
        return [item["key"] for item in aggs]

    def search(self, **kwargs):
        response = Search(using=self.es, **kwargs)
        return response

    def get_user_name(self, users=None, group_key=None, owner="xuetangX"):
      
        if not users:
            users = self.get_users()
        query = self.es_query(doc_type='student_enrollment_info')\
                .filter("term", course_id=self.course_id)\
                .filter("terms", user_id=users)\
                .source(fields=["rname", "nickname", "user_id"])
        if self.group_key:
            query = query.filter('term', group_key=self.group_key)
        #else:
        #    query = query.filter(~F('exists', field='group_key'))
        results = self.es_execute(query[:len(users)]).hits
        result = {}
        for item in results:
            user_id = item.user_id
            name = []
            if self.group_type == 'mooc':
                nickname = item.nickname
                rname = "--"
                name.append(nickname)
                name.append(rname)
            else:
                nickname = item.nickname
                rname = item.rname
                name.append(nickname)
                name.append(rname)
            result[int(user_id)] = name
        return result

    def get_grade(self, users=None):
        if not users:
            users = self.get_users()
        query = self.es_query(doc_type='problem_course')\
                .filter("term", course_id=self.course_id)\
                .filter("term", group_key=self.group_key) \
                .filter("terms", user_id=users)
        results = self.es_execute(query[:len(users)])
        result = {}
        for item in results:
            if item.grade_ratio < 0:
                item.grade_ratio = 0
            result[item.user_id] = item.grade_ratio
        return result


