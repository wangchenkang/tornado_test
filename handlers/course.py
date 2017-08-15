#! -*- coding: utf-8 -*-
from __future__ import division
from .base import BaseHandler
from utils.routes import route
from utils.tools import utc_to_cst, date_to_str, datedelta
from elasticsearch_dsl import A
import json
import settings
from utils.log import Log
from elasticsearch_dsl import Q


@route('/course/week_activity')
class CourseActivity(BaseHandler):
    """
    课程7日活跃统计
    """
    def get(self):
        date = self.get_argument('date')[:10]
        # 拿到所有当前group的所有课程及其注册人数
        course_enrolls = self.get_enroll(group_key=self.group_key)
        courses = course_enrolls.keys()
        # 查询该group的所有课程活跃数据
        query = self.es_query(doc_type = 'course_active_num') \
            .filter('term', time_d=date) \
            .filter('terms', course_id=courses) \
            .filter('term', group_key=self.group_key)

        total = self.es_execute(query[:0]).hits.total
        hits = self.es_execute(query[:total]).hits
        result = {'active_user_num': 0, 'percent': 0, 'overcome': 0 }
        # 计算该课程该group的7日活跃率
        for hit in hits:
            if hit.course_id == self.course_id:
                active_user_num = hit.active_user_num
                course_enroll_num = course_enrolls.get(self.course_id, 0.001)
                if active_user_num > course_enroll_num:
                    active_user_num = course_enroll_num
                result['active_user_num'] = active_user_num
                result['percent'] = float(active_user_num) / course_enroll_num
                break
        # 计算该group在所有课程中的7日活跃率排名
        overcome = 0
        for hit in hits:
            if hit.course_id == self.course_id:
                continue
            course_activity_rate = float(hit.active_user_num) / course_enrolls.get(hit.course_id, 0.001)
            if course_activity_rate < result['percent']:
                overcome += 1
        result['overcome'] = float(overcome) / (len(hits) or 0.001)
        self.success_response({'data': result})


@route('/course/register_rank')
class CourseRegisterRank(BaseHandler):
    """
    课程总注册人数统计及排名
    """
    def get(self):
        student_num = self.get_enroll(self.group_key, self.course_id)
        courses_student_num = self.get_enroll(self.group_key)
        overcome = 0
        for course_id, value in courses_student_num.items():
            if course_id != self.course_id and student_num > value:
                overcome += 1
        overcome = float(overcome) / len(courses_student_num)
        result = {
                "user_num": student_num,
                "overcome": overcome
                }

        self.success_response({'data': result})

@route('/course/student_num')
class CourseStudentNum(BaseHandler):
    """
    课程学生人数，针对某个课程下的多个group_key
    """
    def get(self):

        student_num = self.get_student_num(self.course_group_key)
        result = {
                "student_num": student_num
                }

        self.success_response({"data": result})

@route('/course/grade_distribution')
class CourseGradeDistribution(BaseHandler):
    """
    课程成绩分布统计
    """
    def get(self):
        query = self.es_query(doc_type="course_grade_distribution")
        query = query.filter("term", course_id=self.course_id).sort("-date") \
                     .filter("term", group_key=self.group_key)[:1]
        result = self.es_execute(query)
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
class CourseEnrollmentsCount(BaseHandler):
    """
    获取课程当前总选课人数
    """
    def get(self):
        print self.get_enroll(self.group_key, self.course_id)
        self.success_response({"total": self.get_enroll(self.group_key, self.course_id)})

@route('/course/enrollments/users')
class CourseEnrollments(BaseHandler):
    """
    获取课程当前选课用户列表
    """
    def get(self):
        is_active = self.get_argument("is_active", "")
        query = self.search(doc_type="student_courseenrollment")
        if is_active == "true":
            query = query.filter("term", is_active=True)
        elif is_active == "false":
            query = query.filter("term", is_active=False)
        query = query.filter("term", course_id=self.course_id) \
                .filter("term", group_key=self.group_key)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])
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
        start = self.get_param("start")
        end = self.get_param("end")
       
        query = self.es_query(doc_type="student_courseenrollment")
        query = query.filter("range", **{'event_time': {'lt': '%s%s' % (datedelta(end,2),'T00:00:00+08:00'), 'gte': '%s%s' %(start, 'T00:00:00+08:00')}}) \
                    .filter('term', group_key=self.group_key) \
                    .filter("term", course_id=self.course_id)[:0]
        query.aggs.bucket('value', A("date_histogram", field="event_time", interval="day", time_zone='+08:00')) \
                .metric('count', "terms", field="is_active", size=2)
        results = self.es_execute(query)
        aggs = results.aggregations
        buckets = aggs['value']['buckets']
        res_dict = {}
        for x in buckets:
            date = str(x["key_as_string"][:10])
            data = {}
            aggs_buckets = x["count"]["buckets"]
            for aggs_bucket in aggs_buckets:
                data['unenroll'] = 0
                if str(aggs_bucket["key"]) == '1':
                    data['enroll'] = aggs_bucket["doc_count"]
                elif str(aggs_bucket['key']) == '0':
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
        query = self.es_query(doc_type="student_courseenrollment")
        query = query.filter("range", **{'event_time': {'lt': '%s%s' %(start,'T00:00:00+08:00')}})
        query = query.filter('term', group_key=self.group_key)
        query = query.filter("term", course_id=self.course_id)
        query = query[:0]
        query.aggs.bucket('value', "terms", field="is_active", size=2)
        results = self.es_execute(query)
        aggs = results.aggregations
        buckets = aggs['value']['buckets']
        enroll = 0
        unenroll = 0
        for x in buckets:
            if str(x["key"]) == '1':
                enroll = x["doc_count"]
            elif str(x['key']) == '0':
                unenroll = x["doc_count"]
        data = sorted(res_dict.values(), key=lambda x: x["date"])
        unused_item = None
        for item in data:
            enroll += item["enroll"]
            unenroll += item["unenroll"]
            item["total_enroll"] = enroll + unenroll
            item["total_unenroll"] = unenroll
            item["enrollment"] = enroll
            if item['date'] == end_1:
                unused_item = item
        for item in data:
            if item['date'] == end:
                if unused_item:
                    item["enrollment"] = unused_item['enrollment']
                    item['total_enroll'] = unused_item['total_enroll']
                break
        if unused_item:
            data.remove(unused_item)
        self.success_response({"data": data})

@route('/course/enrollments_date_realtime')
class CourseEnrollmentsDateRealtime(BaseHandler):
    """
    课程选课退课每日数据
    """
    def get(self):
        start = self.get_param("start")
        end = self.get_param("end")
        enroll_query = self.es_query(index='realtime', doc_type="student_enrollment_info")
        enroll_query = enroll_query.filter("range", **{'enroll_time': {'lte': end, 'gte': start}}) \
                    .filter('term', group_key=self.group_key) \
                    .filter("term", course_id=self.course_id) \
                    .filter("term", is_active=1)[:0]
        enroll_query.aggs.bucket('value', A("date_histogram", field="enroll_time", interval="day"))

        unenroll_query = self.es_query(index='realtime', doc_type="student_enrollment_info")
        unenroll_query = unenroll_query.filter("range", **{'unenroll_time': {'lte': end, 'gte': start}}) \
                    .filter('term', group_key=self.group_key) \
                    .filter("term", course_id=self.course_id) \
                    .filter("term", is_active=0)[:0]
        unenroll_query.aggs.bucket('value', A("date_histogram", field="unenroll_time", interval="day"))

        enroll_results = self.es_execute(enroll_query)
        enroll_aggs = enroll_results.aggregations.value
        enroll_buckets = enroll_aggs['buckets']
        unenroll_results = self.es_execute(unenroll_query)
        unenroll_aggs = unenroll_results.aggregations.value
        unenroll_buckets = unenroll_aggs['buckets']
        res_dict = {}
        for x in enroll_buckets:
            date = str(x["key_as_string"][:10])
            data = {}
            data['enroll'] = x["doc_count"]
            data["date"] = date
            res_dict[date] = data
        for x in unenroll_buckets:
            date = str(x["key_as_string"][:10])
            data = res_dict.get(date, {'date': date, 'enroll': 0})
            data['unenroll'] = x["doc_count"]

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
        enroll_query = self.es_query(index='realtime', doc_type="student_enrollment_info")
        enroll_query = enroll_query.filter("range", **{'enroll_time': {'lt': start}})
        enroll_query = enroll_query.filter("term", course_id=self.course_id)
        enroll_query = enroll_query.filter('term', group_key=self.group_key)
        enroll_query = enroll_query.filter("term", is_active=1)
        enroll_query = enroll_query[:0]
        enroll_result = self.es_execute(enroll_query)
        enroll = enroll_result.hits.total
        Log.create('course')
        Log.info('enroll: %s' % enroll)

        unenroll_query = self.es_query(index='realtime', doc_type="student_enrollment_info")
        unenroll_query = unenroll_query.filter("range", **{'unenroll_time': {'lt': start}})
        unenroll_query = unenroll_query.filter("term", course_id=self.course_id)
        unenroll_query = unenroll_query.filter('term', group_key=self.group_key)
        unenroll_query = unenroll_query.filter("term", is_active=0)
        unenroll_query = unenroll_query[:0]
        unenroll_result = self.es_execute(unenroll_query)
        unenroll = unenroll_result.hits.total
        Log.create('course')
        Log.info('unenroll: %s' % unenroll)

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
    """
    获取给定日期内用户活跃统计
    """
    def get(self):
        end = self.get_param("end")
        start = self.get_param("start")

        query = self.es_query(doc_type="course_active_num") \
                .filter("range", **{'time_d': {'lte': end, 'gte': start}}) \
                .filter("term", course_id=self.course_id)\
                .filter("term", group_key=self.group_key)
        total = self.es_execute(query[:0]).hits.total
        results = self.es_execute(query[:total])
        res_dict = {}
        for item in results.hits:
            res_dict[item.time_d] = {
                "active": getattr(item, 'active_user_num', 0),
                "inactive": getattr(item, 'sleep_user_num', 0),
                "new_inactive": getattr(item, 'sleep_user_num', 0),
                "revival": getattr(item, 'weakup_user_num', 0),
                "date": item.time_d
            }
        item = start
        while item <= end:
            if not item in res_dict:
                res_dict[item] = {
                    "date": item,
                    "active": 0,
                    "inactive": 0,
                    "new_inactive": 0,
                    "revival": 0
                }
            item = datedelta(item, 1)
        data = sorted(res_dict.values(), key=lambda x:x['date'])
        self.success_response({'data': data})

@route('/course/distribution')
class CourseDistribution(BaseHandler):
    """
    获取用户省份统计
    """
    def get(self):
        top = int(self.get_argument('top', 10))
        query = self.es_query(doc_type="course_student_location")\
                    .filter("term", group_key=self.group_key)\
                    .filter("term", course_id=self.course_id)\
                    .filter("term", country='中国')[:0]
        query.aggs.bucket("area", "terms", field="province", size=top)
        results = self.es_execute(query)
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
    """
    获取用户视频观看时段统计
    """
    def get(self):
        start = self.get_param("start")
        end = self.get_param("end")
        query = self.es_query(doc_type="course_video_learning")\
                    .filter("term", course_id=self.course_id)\
                    .filter("term", group_key=self.group_key)\
                    .filter("range", **{'date': {'lte': end, 'gte': start}})[:15]
        results = self.es_execute(query)
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
        data = sorted(res_dict.values(), key=lambda x: x['date'])
        self.success_response({'data': data})


@route('/course/topic_keywords')
class CourseTopicKeywords(BaseHandler):
    """
    获取课程提取关键字
    """
    def get(self):

        query = self.es_query(index='rollup',doc_type='course_keywords') \
                    .filter('term', course_id=self.get_param('course_id'))[:1]
        
        data = self.es_execute(query)
        try:
            source = data.hits[0].to_dict()
        except (IndexError, KeyError):
            source = {}

        self.success_response({'data': source})


@route('/course/student_from_tsinghua')
class CourseTsinghuaStudent(BaseHandler):
    """
    课程绑定清华账号学生数
    """
    def get(self):
        query = self.es_query(doc_type='course_student_location') \
                .filter('term', course_id=self.course_id) \
                .filter('term', binding_org=u'清华大学') \
                .filter('term', group_key=self.group_key)

        size = self.es_execute(query[:0]).hits.total
        data = self.es_execute(query[:size])

        result = [item.to_dict() for item in data.hits]
        user_ids = [item['uid'] for item in result]
        query = self.es_query(doc_type='student_enrollment_info')\
                        .filter('term', course_id=self.course_id)\
                        .filter('term', group_key=self.group_key)\
                        .filter('term', is_active=True)\
                        .filter('terms', user_id=user_ids)

        data = self.es_execute(query)
        self.success_response({'data': data.hits.total})

@route('/course/detail')
class CourseDetail(BaseHandler):
    """
    课程详情页
    """
    def get(self):
        course_ids = self.course_id.split(",")
        query = self.es_query(doc_type='course_community')\
                            .filter('terms', course_id=course_ids)
        size = self.es_execute(query[:0]).hits.total
        data = self.es_execute(query[:size])
        course_data = []
        for i in range(len(data.hits)):
            course_data.append(data.hits[i].to_dict())

        self.success_response({'data': course_data})

@route('/course/group_detail')
class CourseQueryDate(BaseHandler):
    def get(self):
        query = self.es_query(doc_type='course_community')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)
        data = self.es_execute(query[:1])
        course_data = {}
        if data.hits:
            course_data.update(data.hits[0].to_dict())

        self.success_response({'data': course_data})

@route('/course/course_health')
class CourseHealth(BaseHandler):
    def get(self):
        query = self.es_query(doc_type='course_health')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key).source(['active_rate', 'active_user_num', 'active_rank', 'enroll_num', 'enroll_rank', 'reply_rate', 'noreply_num', 'reply_rank', 'interactive_per', 'interactive_rank', 'comment_num', 'comment_rank'])
        total = self.es_execute(query[:0]).hits.total
        result = self.es_execute(query[:total]).hits
        data = {}
        if len(result) !=0:
            data = result[0].to_dict()
        self.success_response({'data': data})

@route('/course/cohort_info')
class CohortInfo(BaseHandler):
    """
    cohort分组的组信息
    """
    def post(self):
        group_keys = json.loads(self.get_argument('group_key'))
        query = self.es_query(doc_type='course_community')\
                    .filter('term', course_id=self.course_id)\
                    .filter('terms', group_key=group_keys)\
                    .filter(Q('range', **{'group_key': {'gte': settings.SPOC_GROUP_KEY, 'lt': settings.TSINGHUA_GROUP_KEY}}) | Q('range',  **{'group_key': {'gte': settings.COHORT_GROUP_KEY, 'lt': settings.ELECTIVE_GROUP_KEY}}))\
                    .sort('group_key')\
                    .source(['group_name', 'enroll_num', 'group_key'])
        total = len(group_keys) if group_keys else 1
        result = self.es_execute(query[:total]).hits
        data = []
        if len(result) !=0:
            data = [item.to_dict() for item in result]
        for item in data:
            item['school'] = item['group_name']
            if item['group_key'] == settings.SPOC_GROUP_KEY:
                item['school'] = u'全部学生'
        self.success_response({'data': data})
