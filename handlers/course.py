#! -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from .base import BaseHandler
from utils.routes import route
from utils.tools import utc_to_cst, date_to_str, datedelta
from elasticsearch_dsl import A
import json


@route('/course/week_activity')
class CourseActivity(BaseHandler):
    """
    课程7日活跃统计
    """
    def get(self):
        course_id = self.course_id
        yestoday = utc_to_cst(datetime.utcnow() - timedelta(days=1))
        date = self.get_argument('date', date_to_str(yestoday))
        
        query = { 
            'query': {
                'filtered': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'date': date}},
                            ]
                        }
                    }
                }
            },
            'size': 1
        }

        data = self.es_search(index='api1', doc_type='course_week_active', body=query)

        try:
            source = data['hits']['hits'][0]['_source']
            result = {
                'active_num': int(source['active_num']),
                'effective_num': int(source['effective_student_num']),
                'date': source['date'],
                'percent': '{:0.4f}'.format(float(source['percent']))
            }
        except IndexError:
            result = {}

        self.success_response({'data': result})


@route('/course/grade_distribution')
class CourseGradeDistribution(BaseHandler):
    """ 
    课程成绩分布统计
    """
    def get(self):
        course_id = self.course_id
        query = self.search(index="api1", doc_type="course_grade_distribution")
        query = query.filter("term", course_id=self.course_id)\
                .sort("-date")[:1]
        result = query.execute()
        hits = result.hits
        data = {}
        if hits:
            hit = hits[0]
            data["grade_distribution"] = list(hit.distribution)
            data["above_average"] = hit.above_average
            data["total_student_num"] = sum(list(hit.distribution))
        else:
            data["grade_distribution"] = [0]*50
            data["above_average"] = 0
            data["total_student_num"] = 0
        self.success_response({'data': data})

@route('/course/enrollments')
class CourseEnrollments(BaseHandler):
    def get(self):
        is_active = self.get_argument("is_active", "")
        query = self.search(index="main", doc_type="enrollment")
        if is_active == "true":
            query = query.filter("term", is_active=True)
        elif is_active == "false":
            query = query.filter("term", is_active=False)
        query = query.filter("term", course_id=self.course_id)
        query = query[:0]
        results = query.execute()
        total = results.hits.total
        self.success_response({"data": total})

@route('/course/enrollments_date')
class CourseEnrollmentsDate(BaseHandler):
    def get(self):
        query = self.search(index="main", doc_type="enrollment")
        end = self.get_param("end")
        start = self.get_param("start")
        query = query.filter("range", **{'event_time': {'lte': end, 'gte': start}})\
                .filter("term", course_id=self.course_id)[:0]
        query.aggs.bucket('value', A("date_histogram", field="event_time", interval="day"))\
                .metric('count', "terms", field="is_active", size=2)
        results = query.execute()
        aggs = results.aggregations
        buckets = aggs['value']['buckets']
        res_dict = {}
        for x in buckets:
            date = str(x["key_as_string"][:10])
            data = {}
            aggs_buckets = x["count"]["buckets"]
            for aggs_bucket in aggs_buckets:
                if str(aggs_bucket["key"]) == 'T':
                    data['enroll'] = aggs_bucket["doc_count"]
                elif str(aggs_bucket['key']) == 'F':
                    data['unenroll'] = aggs_bucket["doc_count"]
            data["date"] = date
            res_dict[date] = data
        item = start
        end_1 = datedelta(end, 1)
        while item != end_1:
            if item in res_dict:
                if not "enroll" in res_dict[item]:
                    res_dict[item]["enroll"] = 0
                if not "unenroll" in res_dict[item]:
                    res_dict[item]["unenroll"] = 0
            else:
                res_dict[item] = {
                        "date": item,
                        "enroll": 0,
                        "unenroll": 0
                        }
            item = datedelta(item, 1)
        # 取end的数据
        query = self.search(index="main", doc_type="enrollment")
        start = self.get_param("start")
        query = query.filter("range", **{'event_time': {'lte': start}})
        query = query.filter("term", course_id=self.course_id)
        query = query[:0]
        query.aggs.bucket('value', "terms", field="is_active", size=2)
        results = query.execute()
        aggs = results.aggregations
        buckets = aggs['value']['buckets']
        enroll = 0
        unenroll = 0
        for x in buckets:
            if str(x["key"]) == 'T':
                enroll = x["doc_count"]
            elif str(x['key']) == 'F':
                unenroll = x["doc_count"]
        data = sorted(res_dict.values(), key=lambda x: x["date"])
        for item in data:
            item["total_enroll"] = enroll
            item["total_unenroll"] = unenroll
            item["enrollment"] = enroll - unenroll
            enroll += item["enroll"]
            unenroll += item["unenroll"]
        self.success_response({"data": data})


@route('/course/active_num')
class CourseActive(BaseHandler):
    def get(self):
        query = self.search(index="rollup", doc_type="course_active")
        end = self.get_param("end")
        start = self.get_param("start")
        query = query.filter("range", **{'date': {'lte': end, 'gte': start}})
        query = query.filter("term", course_id=self.course_id)
        query = query[:100]
        results = query.execute()
        res_dict = {}
        for item in results.hits:
            res_dict[item.date] = {
                "active": item.active,
                "inactive": item.inactive,
                "newinactive": item.newinactive,
                "revival": item.revival,
                "date": item.date
            }
        item = start
        end_1 = datedelta(end, 1)
        while item != end_1:
            if not item in res_dict:
                res_dict[item] = {
                    "date": item, 
                    "active": 0,
                    "inactive": 0,
                    "newinactive": 0,
                    "revival": 0
                }
            item = datedelta(item, 1)
        data = sorted(res_dict.values(), key=lambda x: x["date"])
        self.success_response({'data': data})

@route('/course/distribution')
class CourseDistribution(BaseHandler):
    def get(self):
        query = self.search(index="main", doc_type="student")
        top = int(self.get_argument("top", 10))
        query = query.filter("term", courses=self.course_id)\
                .filter("term", country='中国')[:0]
        query.aggs.bucket("area", "terms", field="prov", size=top)
        results = query.execute()
        aggs = results.aggregations["area"]["buckets"]
        data = []
        for item in aggs:
            data.append({
                "area": u'中国-' + item["key"],
                "total_enrolls": item["doc_count"]
                })
        self.success_response({'data': data})
        
@route('/course/watch_num')
class CourseVideoWatch(BaseHandler):
    def get(self):
        query = self.search(index="api1", doc_type="video_course_active_learning")
        query = query.filter("term", course_id=self.course_id)
        end = self.get_param("end")
        start = self.get_param("start")
        query = query.filter("range", **{'date': {'lte': end, 'gte': start}})
        results = query.execute()
        hits = results.hits
        res_dict = {}
        for hit in hits:
            res_dict[hit.date] = {
                "date": hit.date,
                "hour_watch_num": list(hit.watch_num_list)
            }
        item = start
        end_1 = datedelta(end, 1)
        while item != end_1:
            if not item in res_dict:
                res_dict[item] = {
                    "date": item, 
                    "hour_watch_num": [0]*24
                }
            item = datedelta(item, 1)
        data = sorted(res_dict.values(), key=lambda x: x["date"])
        self.success_response({'data': data})
            

@route('/course/topic_keywords')
class CourseTopicKeywords(BaseHandler):
    def get(self):
        query = {
            'query': {
                'filtered': {
                    'query': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': self.course_id}},
                            ]
                        }
                    }
                }
            },
            'size': 1
        }

        data = self.es_search(index='rollup', doc_type='course_keywords', body=query)
        try:
            source = data['hits']['hits'][0]['_source']
        except (IndexError, KeyError):
            source = {}

        self.success_response({'data': source})

