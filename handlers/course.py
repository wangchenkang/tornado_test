#! -*- coding: utf-8 -*-
from __future__ import division
from elasticsearch_dsl import A, F
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
        date = self.get_argument('date')[:10]
        # 拿到所有当前group的所有课程及其注册人数
        course_enrolls = self.get_enroll(group_key=self.group_key)
        courses = course_enrolls.keys()
        # 查询该group的所有课程活跃数据
        query = self.es_query(doc_type = 'active') \
            .filter('term', time_d=date) \
            .filter('terms', course_id=courses) \
            .filter('term', group_key=self.group_key)

        result = self.es_execute(query)
        hits = self.es_execute(query[:result.hits.total]).hits
        result = {'active_user_num': 0, 'percent': 0, 'overcome': 0 }
        # 计算该课程该group的7日活跃率
        for hit in hits:
            if hit.course_id == self.course_id:
                result['active_user_num'] = hit.active_user_num
                result['percent'] = float(hit.active_user_num) / course_enrolls[self.course_id]
                break
        # 计算该group在所有课程中的7日活跃率排名
        overcome = 0
        for hit in hits:
            if hit.course_id == self.course_id:
                continue
            course_activity_rate = float(hit.active_user_num) / course_enrolls[self.course_id]
            if course_activity_rate < result['percent']:
                overcome += 1
        result['overcome'] = float(overcome) / len(courses)
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
            if course_id != self.course_id and value > student_num:
                overcome += 1
        overcome = float(overcome) / len(courses_student_num)
        result = {
                "user_num": student_num,
                "overcome": overcome
                }

        self.success_response({'data': result})


@route('/course/grade_distribution')
class CourseGradeDistribution(BaseHandler):
    """
    课程成绩分布统计
    """
    def get(self):
        users = self.get_problem_users()
        query = self.es_query(index="tap", doc_type="problem_course")\
                .filter("term", course_id=self.course_id)\
                .filter("terms", user_id=users)
        result = self.es_execute(query)
        result = self.es_execute(query[:result.hits.total])
        hits = result.hits
        data = {
                "distribution":[0]*50,
                "student_num": 0,
                "above_average": 0}
        avg = 0
        for hit in hits:
            if hit.current_grade < 0:
                continue
            index = int(hit.current_grade/2)
            if index >= 50:
                index = 49
            data["distribution"][index] += 1
            data["student_num"] += 1
            avg += index
        if data["student_num"] > 0:
            avg /= float(data["student_num"])
            above_avg = sum(data["distribution"][int(avg+1):])
            data["above_average"] = above_avg
        self.success_response({'data': data})


@route('/course/enrollments/count')
class CourseEnrollmentsCount(BaseHandler):
    """
    获取课程当前总选课人数
    """
    def get(self):
        self.success_response({"total": self.get_enroll(self.group_key, self.course_id)})

@route('/course/enrollments/users')
class CourseEnrollments(BaseHandler):
    """
    获取课程当前选课用户列表
    """
    def get(self):
        is_active = self.get_argument("is_active", "")
        query = self.search(index="main", doc_type="enrollment")
        if is_active == "true":
            query = query.filter("term", is_active=True)
        elif is_active == "false":
            query = query.filter("term", is_active=False)
        query = query.filter("term", course_id=self.course_id)

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
        query = self.es_query(index="tap", doc_type="student")\
                .filter("term", course_id=self.course_id)
        if self.group_key:
            query = query.filter('term', group_key=self.group_key)
        query = query.fields(fields=["user_id", "unenroll_time", "enroll_time", "is_active"])

        size = self.es_execute(query[:0]).hits.total
        hits = self.es_execute(query[:size]).hits
        ret = {}
        def default(date):
            return {
                "enroll": 0,    # 当天选课量
                "unenroll": 0,  # 当天退课量
                "date_enrollment": -1, # 当天净选课量
                "total_enroll": 0, # 截止当天总选课量
                "total_unenroll": 0, # 截止当天总退课量
                "enrollment": 0,     # 截止当天正在选课量
                "date": date
                }
        total_enroll, total_unenroll = 0, 0
        for hit in hits:
            total_enroll += 1
            if hit.is_active and hit.is_active[0] == 0:
                total_unenroll += 1
            if getattr(hit, "enroll_time", None):
                enroll_time = hit.enroll_time[0][:10]
            else:
                enroll_time = None
            if getattr(hit, "unenroll_time", None):
                unenroll_time = hit.unenroll_time[0][:10]
            else:
                unenroll_time = None
            if enroll_time >= start and enroll_time <= end:
                if enroll_time not in ret:
                    ret[enroll_time] = default(enroll_time)
                ret[enroll_time]["enroll"] += 1
            if unenroll_time and unenroll_time >= start and unenroll_time <= end:
                if unenroll_time not in ret:
                    ret[unenroll_time] = default(unenroll_time)
                ret[unenroll_time]["unenroll"] += 1
        if end not in ret:
            ret[end] = default(end)

        ret[end]["total_enroll"] = total_enroll
        ret[end]["total_unenroll"] = total_unenroll
        ret[end]["enrollment"] = total_enroll - total_unenroll
        ret[end]["date_enrollment"] = ret[end]["enroll"] - ret[end]["unenroll"]
        prev = datedelta(end, -1)
        curr = end
        yesterday = datedelta(start, -1)
        while prev != yesterday:
            if prev not in ret:
                ret[prev] = default(prev)
            ret[prev]["total_enroll"] = ret[curr]["total_enroll"] - ret[curr]["enroll"]
            ret[prev]["total_unenroll"] = ret[curr]["total_unenroll"] - ret[curr]["unenroll"]
            ret[prev]["enrollment"] = ret[prev]["total_enroll"] - ret[prev]["total_unenroll"]
            ret[prev]["date_enrollment"] = ret[prev]["enroll"] - ret[prev]["unenroll"]
            curr = prev
            prev = datedelta(prev, -1)
        self.success_response({"data": sorted(ret.values(), key=lambda x: x["date"])})

@route('/course/active_num')
class CourseActive(BaseHandler):
    """
    获取给定日期内用户活跃统计
    """
    def get(self):
        end = self.get_param("end")
        start = self.get_param("start")

        query = self.es_query(index="tap", doc_type="active") \
                .filter("range", **{'time_d': {'lte': end, 'gte': start}}) \
                .filter("term", course_id=self.course_id)
        if self.group_key:
            query = query.filter("term", group_key=self.group_key)
        #else:
        #    query = query.filter(~F("exists", field="group_key"))
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
        data = sorted(res_dict.values(), key=lambda x: x["date"])
        self.success_response({'data': data})

@route('/course/distribution')
class CourseDistribution(BaseHandler):
    """
    获取用户省份统计
    """
    def get(self):
        query = self.es_query(index="tap", doc_type="student")
        if self.group_key:
            query = query.filter("term", group_key=self.group_key)
        top = int(self.get_argument("top", 10))
        query = query.filter("term", course_id=self.course_id)\
                .filter("term", country='中国')[:0]
        query.aggs.bucket("area", "terms", field="prov", size=top)
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
        # get student
        users = self.get_users()
        query = self.es_query(index="tap", doc_type="video_daily")\
                .filter("term", course_id=self.course_id)\
                .filter("range", **{'date': {'lte': end, 'gte': start}})\
                .filter("terms", user_id=users)

        size = self.es_execute(query[:0]).hits.total
        hits = self.es_execute(query[:size]).hits
        res_dict = {}
        for hit in hits:
            if hit.date in res_dict:
                wl = list(hit.watch_list)
                for i in range(24):
                    res_dict[hit.date]["hour_watch_num"][i] += int(hit.watch_list[i])
            else:
                res_dict[hit.date] = {"date": hit.date, "hour_watch_num": list(hit.watch_list)}
        item = start
        end_1 = datedelta(end, 1)
        while item != end_1:
            if not item in res_dict:
                res_dict[item] = {
                    "date": item,
                    "hour_watch_num": [0]*24
                }
            item = datedelta(item, 1)
        data = res_dict.values()
        data.sort(key=lambda x: x["date"])
        self.success_response({'data': data})

@route('/course/topic_keywords')
class CourseTopicKeywords(BaseHandler):
    """
    获取课程提取关键字
    """
    def get(self):
        query = self.es_query(index='rollup', doc_type='course_keywords') \
                .filter('term', course_id=self.course_id)[:1]

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
        query = self.es_query(index='tap', doc_type='student') \
                .filter('term', courses=self.course_id) \
                .filter('term', binding_org='tsinghua')[:0]
        if self.group_key:
            query = query.filter('term', group_key=self.group_key)
        data = self.es_execute(query)
        self.success_response({'data': data.hits.total})
