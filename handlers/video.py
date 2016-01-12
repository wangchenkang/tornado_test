#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
from utils import es_utils

Log.create('video')

@route('/video/chapter_stat')
class ChapterVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id

        query = self.es_query(index='rollup', doc_type='video_student_num_ds') \
                .filter('term', course_id=course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('term', video_id='-1')

        default_size = 0
        data = self.es_execute(query[:default_size])
        if data.hits.total > default_size:
            data = self.es_execute(query[:data.hits.total])

        video_stat = {
            'total': data.hits.total,
            'sequentials': {}
        }
        for item in data.hits:
            video_stat['sequentials'][item.seq_id] = {
                'student_num': item.view_user_num,
                'review_num': item.review_user_num
            }

        self.success_response(video_stat)


@route('/video/chapter_student_stat')
class ChapterStudentVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        uid = self.get_param('user_id')
        students = [u.strip() for u in uid.split(',') if u.strip()]

        default_size = 0
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'terms': {'uid': students}},
                                {'exists': {'field': 'duration'}},
                                {'exists': {'field': 'study_rate'}}
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='main', doc_type='video', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='video', body=query)

        chapter_student_stat = {}
        for item in data['hits']['hits']:
            sequential_id = item['_source']['sequential_id']
            student_id = item['_source']['uid']
            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id].setdefault(student_id, [])

            chapter_student_stat[sequential_id][student_id].append({
                'video_id': item['_source']['vid'],
                'review': item['_source']['review'],
                'review_rate': item['_source']['review_rate'],
                'watch_num': item['_source']['watch_num'],
                'duration': item['_source']['duration'],
                'study_rate': item['_source']['study_rate'],
                'la_access': item['_source']['la_access']
            })

        result = {
            'total': data['hits']['total'],
            'sequentials': chapter_student_stat
        }

        self.success_response(result)


@route('/video/course_study_rate')
class CourseVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        user_id = self.get_argument('user_id', None)

        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                            ]
                        }
                    }
                }
            },
            'size': 0
        }

        if user_id is not None:
            max_length = 1000
            user_id = [int(u.strip()) for u in user_id.split(',') if u.strip()][0:max_length]
            query['query']['filtered']['filter']['bool']['must'].append({
                'terms': {'uid': user_id}
            })
            query['size'] = max_length
            data = self.es_search(index='rollup', doc_type='course_video_rate', body=query) 
        else:
            default_size = 0
            query['size'] = default_size
            data = self.es_search(index='rollup', doc_type='course_video_rate', body=query) 
            if data['hits']['total'] > default_size:
                query['size'] = data['hits']['total']
                data = self.es_search(index='rollup', doc_type='course_video_rate', body=query) 

        result = []
        users_has_study = set()
        for item in data['hits']['hits']:
            # fix uid: anonymous_id-anonymous_id-e03be04b3e8a30b74b9779ace15e1b50
            try:
                item_uid = int(item['_source']['uid'])
            except ValueError:
                continue
            result.append({
                'user_id': item_uid,
                'study_rate': float(item['_source']['study_rate_open'])
            })
            users_has_study.add(item_uid)

        if user_id:
            users_not_study = set(user_id).difference(users_has_study)
            for item in users_not_study:
                result.append({
                    'user_id': item,
                    'study_rate': 0.000
                })

        self.success_response({'data': result})

@route('/video/chapter_video_stat')
class ChapterVideoStat(BaseHandler):
    def get(self):
        result = {}
        query = self.search(index='api1', doc_type='video_chapter_study_review') \
                .filter('term', course_id=self.course_id)\
                .filter('term', chapter_id=self.chapter_id)\
                .sort('-date')[:1]
        data = query.execute()
        hit = data.hits
        result['course_id'] = self.course_id
        result['chapter_id'] = self.chapter_id
        if hit:
            hit = hit[0]
            result['study_num'] = int(hit.study_num)
            result['review_num'] = int(hit.review_num)
        else:
            result['study_num'] = 0
            result['review_num'] = 0
        self.success_response(result)

@route('/video/chapter_video_detail')
class CourseChapterVideoDetail(BaseHandler):
    def get(self):
        result = []
        query = self.search(index='main', doc_type='video')\
                .filter('term', course_id=self.course_id)\
                .filter('term', chapter_id=self.chapter_id)\
                .filter('exists', field='watch_num')[:0]
        query.aggs.bucket("video_watch", "terms", field="vid", size=0)\
                .metric('num', 'range', field="watch_num", ranges=[{"from": 1, "to": 2}, {"from": 2}])
        results = query.execute()
        aggs = results.aggregations
        buckets = aggs["video_watch"]["buckets"]
        for bucket in buckets:
            vid = bucket["key"]
            items = bucket["num"]["buckets"]
            num1 = 0
            num2 = 0
            for item in items:
                if item["key"] == "1.0-2.0":
                    num1 = item["doc_count"]
                elif item["key"] == "2.0-*":
                    num2 = item["doc_count"]
            result.append({
                "vid": vid,
                "num1": num1,
                "num2": num2
                })
        self.success_response({"data": result})
