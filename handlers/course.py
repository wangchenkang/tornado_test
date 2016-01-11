#! -*- coding: utf-8 -*-
from __future__ import division
from datetime import datetime, timedelta
from elasticsearch_dsl import A
from .base import BaseHandler
from utils.routes import route
from utils.tools import utc_to_cst, date_to_str, datedelta


@route('/course/week_activity')
class CourseActivity(BaseHandler):
    """
    课程7日活跃统计
    """
    def get(self):
        query = self.search(index='rollup', doc_type='recent_active_percent') \
                .filter('term', course_id=self.course_id)[:1]
        data = query.execute()

        try:
            hit = data.hits[0]
            result = {
                'active_user_num': hit.active_user_num,
                'enroll_user_num': hit.enroll_user_num,
                'percent': round(hit.active_user_percent, 4),
                'owner_course_num': hit.owner_course_num,
                'rank': hit.rank,
                'overcome': 1 - round(hit.rank / hit.owner_course_num, 4)
            }
        except IndexError:
            result = {
                'active_user_num': 0,
                'enroll_user_num': 0,
                'percent': 0,
                'owner_course_num': 0,
                'rank': 0,
                'overcome': 0
            }

        self.success_response({'data': result})


@route('/course/register_rank')
class CourseRegisterRank(BaseHandler):
    """
    课程总注册人数统计及排名
    """
    def get(self):
        query = self.search(index='rollup', doc_type='course_user_num_ds') \
                .filter('term', course_id=self.course_id)
        data = query.execute()
        result = {}
        try:
            result['user_num'] = data.hits[0].user_num
            result['owner_course_num'] = data.hits[0].owner_course_num
            result['rank'] = data.hits[0].rank
            result['overcome'] = 1 - round(data.hits[0].rank / data.hits[0].owner_course_num, 4)
        except IndexError:
            result['user_num'] = 0
            result['owner_course_num'] = 0
            result['rank'] = 0
            result['overcome'] = 0

        self.success_response({'data': result})


@route('/course/grade_distribution')
class CourseGradeDistribution(BaseHandler):
    """ 
    课程成绩分布统计
    """
    def get(self):
        course_id = self.course_id
        query = self.search(index="api1", doc_type="course_grade_distribution")
        query = query.filter("term", course_id=self.course_id).sort("-date")[:1]
        result = query.execute()
        hits = result.hits
        data = {}
        if hits:
            hit = hits[0]
            data["distribution"] = list(hit.distribution)
            data["above_average"] = int(hit.above_average)
            data["student_num"] = sum(list(hit.distribution))
            data["date"] = hit.date
        else:
            data["distribution"] = [0]*50
            data["above_average"] = 0
            data["student_num"] = 0
            data["date"] = ''
        self.success_response({'data': data})


@route('/course/enrollments/count')
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
        self.success_response({"total": total})


@route('/course/enrollments/users')
class CourseEnrollments(BaseHandler):
    def get(self):
        is_active = self.get_argument("is_active", "")
        query = self.search(index="main", doc_type="enrollment")
        if is_active == "true":
            query = query.filter("term", is_active=True)
        elif is_active == "false":
            query = query.filter("term", is_active=False)
        query = query.filter("term", course_id=self.course_id)

        default_max_size = 100000
        data = query[:default_max_size].execute()
        if data.hits.total > default_max_size:
            data = query[:data.hits.total].execute()

        students = []
        for item in data.hits:
            students.append(item.uid)

        self.success_response({"students": students, "total": data.hits.total})


@route('/course/enrollments_date')
class CourseEnrollmentsDate(BaseHandler):
    """
    课程选课退课每日数据
    """
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
        query = query.filter("range", **{'event_time': {'lt': start}})
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
            enroll += item["enroll"]
            unenroll += item["unenroll"]
            item["total_enroll"] = enroll + unenroll
            item["total_unenroll"] = unenroll
            item["enrollment"] = enroll

        self.success_response({"data": data})


@route('/course/active_num')
class CourseActive(BaseHandler):
    def get(self):
        query = self.search(index="rollup", doc_type="course_active")
        end = self.get_param("end")
        start = self.get_param("start")
        query = query.filter("range", **{'date': {'lte': end, 'gte': start}})
        query = query.filter("term", course_id=self.course_id)
        query = query[:100000]
        results = query.execute()
        res_dict = {}
        for item in results.hits:
            res_dict[item.date] = {
                "active": getattr(item, 'active', 0),
                "inactive": getattr(item, 'inactive', 0),
                "new_inactive": getattr(item, 'newinactive', 0),
                "revival": getattr(item, 'revival', 0),
                "date": item.date
            }
        item = start
        while item != end:
            if not item in res_dict:
                res_dict[item] = {
                    "date": item, 
                    "active": 0,
                    "inactive": 0,
                    "new_inactive": 0,
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
        query = query.filter("range", **{'date': {'lte': end, 'gte': start}})[:100]

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

@route('/course/student_from_tsinghua')
class CourseTsinghuaStudent(BaseHandler):
    def get(self):
        result = 0
        query = self.search(index='main', doc_type='student')\
                .filter('term', courses=self.course_id)\
                .filter('term', binding_org='tsinghua')[:0]
        data = query.execute()
        total = data.hits.total

        self.success_response({'data': total})

