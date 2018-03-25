# -*- coding: utf-8 -*-

import json
import hashlib
from tornado import gen
from utils.log import Log
from tornado.web import RequestHandler, Finish, MissingArgumentError
from tornado.options import options
from tornado.escape import url_unescape, json_encode, json_decode
from elasticsearch import ConnectionError, ConnectionTimeout, RequestError
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections
from utils.service import CourseService, AsyncService, AsyncCourseService
from utils.tools import fix_course_id
from utils.tools import get_group_type
from utils.tools import is_ended, date_from_string, feedback
import settings
from datetime import datetime

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

    def set_memcache_data_warning(self, hash_key, result):
        self.memcache.set(hash_key, result, 60*60*24)
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
            self.error_response(100, u'参数错误')
        return user_id

    @property
    def course_id(self):
        course_id = self.get_argument('course_id', None)
        if course_id is None:
            self.error_response(100, u'参数错误')
        return fix_course_id(course_id)

    @property
    def chapter_id(self):
        chapter_id = self.get_argument('chapter_id', None)
        if chapter_id is None:
            self.error_response(100, u'参数错误')
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
            param = url_unescape(param).replace(' ', '+') if key == 'course_id' else url_unescape(param)
            return param
        except TypeError:
            return param

    def es_search(self, **kwargs):
        try:
            response = self.es.search(**kwargs)
        except (ConnectionError, ConnectionTimeout):
            self.error_response(400, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

        return response

    def es_query(self, **kwargs):
        if 'index' not in kwargs:
            kwargs['index'] = settings.ES_INDEX
        return Search(using=self.es, **kwargs)

   # def moocnd_es_query(self, **kwargs):
   #     moocnd_es = connections.create_connection(hosts=settings.moocnd_es_cluster, timeout=30)
   #     return Search(using=moocnd_es, **kwargs)
   # 
   # def search_es_query(self, **kwargs):
   #     search_es = connections.create_connection(hosts=settings.search_es_cluster, timeout=30)
   #     return Search(using=search_es, **kwargs)

    def es_execute(self, query):
        try:
            if self.request.uri.startswith('/open_times'):
                response = query.execute()
                return response
            else:
                if not list(set(query._index)&set(settings.ES_INDEXS)) or query._doc_type in ['data_conf', ['data_conf']]:
                    response = query.execute()
                    return response
                course_id = self.get_argument('course_id', None)
                end_time = None
                if course_id:
                    course_structure = self.course_structure(fix_course_id(course_id), 'course')
                    end_time = course_structure.get('end')
                if is_ended(end_time):
                    for index in query._index:
                        if index == 'realtime':
                            query._index='tap_lock'
                        elif index == 'tap_table_video_realtime':
                            query._index = 'tap_table_video_lock'
                        elif index == 'realtime_discussion_table':
                            query._index = 'tap_table_discussion_lock'
                        elif not index.endswith('lock'):
                            query._index.append('lock')
                            query._index = '_'.join(query._index)
                new_query = Search(using=self.es).from_dict(query.to_dict())
                response = query.execute()
                if not response.hits.total:
                    if end_time:
                        end = date_from_string(end_time)
                        now = datetime.utcnow()
                        expires = (now-end).days
                        if expires > 3:
                            Log.create('es')
                            Log.info('%s-%s' % (query._index,course_id))
                           # if not self.request.uri.startswith('/course/cohort_info'):
                           #    if not isinstance(query._index, list):
                           #         es_index = query._index
                           #    else:
                           #         es_index = query._index[0]
                           #    key = '%s_%s_%s' %(es_index, query._doc_type, course_id)
                           #    hash_key, value = self.get_memcache_data(key)
                           #    if not value:                                
                           #       feedback(query._index, query._doc_type, course_id).set_email()
                           #    else:j
                           #       self.set_memcache_data_warning(hash_key, 1)
                           #       feedback(query._index, query._doc_type, course_id).set_email()
                        if query._index in ['tap_table_video_lock', ['tap_table_video_lock']]:
                            new_query._index = 'tap_table_video_realtime'
                        elif query._index in ['tap_table_discussion_lock', ['tap_table_discussion_lock']]:
                            new_query._index = 'realtime_discussion_table'
                        else:
                            new_query._index = query._index.split('_lock')[0] if not isinstance(query._index, list) else query._index[0].split('_lock')[0]
                        new_query._doc_type = query._doc_type[0]
                        response = new_query.execute()
                return response
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))

    def acc_es_execute(self, query):
        try:
           response = query.execute()
           return response
        except (ConnectionError, ConnectionTimeout):
            self.error_response(100, u'Elasticsearch 连接错误')
        except RequestError as e:
            self.error_response(100, u'查询错误: {} - {}'.format(e.status_code, e.error))
           
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

    def course_structure(self, course_id, block_id, depth=2):
        """ 
        获取课件block_id指定章或节以及向下depth深度的课件
        """
        if block_id == "course":
            structure_data = CourseService.get('courses/tree_structure', {'course_id': course_id, 'depth': depth})
        else:
            structure_data = CourseService.get('courses/tree_structure', {'course_id': course_id, 'block_id': block_id, 'depth': depth})
        if not structure_data:
            self.error_response(100, u'课程数据获取失败')

        return structure_data

    @gen.coroutine
    def async_course_service(self, method, *args, **kwargs):
        def _parse_response(response):
            data = None
            if response.error:
                Log.error('service response error: %s' % response.error)
                self.error_response(100, u'Service failed')
            
            try:
                data = json_decode(response.body)
            except ValueError as e:
                Log.error('service response error: %s, content: %s' % (e, response.body))
                self.error_response(100, u'Service failed')

            if data is None:
                Log.error('Course service api failed: {}'.format(str(args)))

            return data
        
        response = yield getattr(AsyncCourseService(), method)(*args, **kwargs)
        raise gen.Return(_parse_response(response))
    
    @gen.coroutine
    def course_detail(self, course_id):
        """
        获取课件信息
        """
        course_detail = yield  self.async_course_service('get', 'courses/detail', {'course_id': course_id})
        raise gen.Return(course_detail)

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
        query = self.es_query(doc_type='student_enrollment_info')

        if self.group_key:
            query = query.filter('term', group_key=self.group_key)
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
       # hashstr = "problem_student" + self.course_id + (str(self.group_key) or "")
       # hashcode = hashlib.md5(hashstr).hexdigest()
       # users = self.memcache.get(hashcode)
       # if users:
       #     return users
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
    
    @property
    def host(self):
        host = self.get_argument('host', None)
        if not host:
            self.error_response(100, u'缺少参数')
        
        return host

    @property
    def course_status(self):
        course_status = self.get_argument('course_status', None)
        if not course_status:
            self.error_response(100, u'缺少参数')

        return course_status

    @property
    def service_line(self):
        service_line = self.get_argument('service_line', None)
        if not service_line:
            self.error_response(100, u'缺少参数')

        return service_line

    @property
    def course_open_num(self):
        query = self.es_query(index='problems_focused', doc_type='course_video_open')\
                    .filter('term', course_id=self.course_id)
        query.aggs.metric('num', 'cardinality', field='video_id')
        result = self.es_execute(query)
        aggs = result.aggregations

        return aggs.num.value
    
    @property
    def chapter_id(self):
        chapter_id = self.get_argument('chapter_id', None)
        if not chapter_id:
            self.error_response(100, u'缺少参数')

        return chapter_id

    @property
    def chapter_open_num(self):
        query = self.es_query(index='problems_focused',doc_type='course_video_open')\
                    .filter('term', course_id=self.course_id)
        query.aggs.bucket('chapter_ids', 'terms', field='chapter_id', size=1000)\
                  .metric('num', 'cardinality', field='video_id')
        result = self.es_execute(query)
        aggs = result.aggregations
        buckets = aggs.chapter_ids.buckets

        return [{'chapter_id': bucket.key, 'open_num': bucket.num.value} for bucket in buckets]
    
    @property
    def seq_open_num(self):
        query = self.es_query(index='problems_focused',doc_type='course_video_open')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', chapter_id=self.chapter_id)
        query.aggs.bucket('seq_ids', 'terms', field='seq_id', size=1000)\
                  .metric('num', 'cardinality', field='video_id')
        result = self.es_execute(query)
        aggs = result.aggregations
        buckets = aggs.seq_ids.buckets

        return [{'seq_id':bucket.key, 'open_num': bucket.num.value}for bucket in buckets]

    # 新加的
    @gen.coroutine
    def get_updatetime(self):
        # data_conf集群和my_student_es_cluster一致
        query = self.es_query(doc_type='data_conf')[:1]
        result = self.es_execute(query).hits
        update_time = '%s' % result[0].latest_data_date
        raise gen.Return(update_time)

    def get_moocnd_update_time(self):
        query = self.moocnd_es_query(index='moocnd_datetime', doc_type='processstate')
        response = self.es_execute(query).hits
        update_time = response[0].current_time
        update_time = update_time.split('+')[0].replace('T', ' ')
        return update_time
