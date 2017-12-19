#! -*- coding: utf-8 -*-
from __future__ import division
from tornado import gen
from .base import BaseHandler
from utils.routes import route
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import A
from elasticsearch_dsl import Q
import json
import settings

client = Elasticsearch(settings.es_cluster)


@route('/mobile/day_list')
class DayListHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        course_id = self.get_param('course_id')  # base.py封装了多个函数，还有get_param
        query = Search(using=client, index="classmate", doc_type="video_rank_daily") \
            .query("term", course_id=course_id) \
            .sort("rank")  # 按照视频学习时长降序取top10
        size = query[:0].execute().hits.total
        result = query[:size].execute().hits
        if not result:
            data = []
        else:
            data = []
            for i in result:
                data_dict = {
                    "rank": i['rank'],  # 排名
                    "user_id": i['user_id'],
                    "study_rate": "%.2f%%" % (i['study_rate'] * 100),  # 昨日学习视频百分比
                    "video_watch_total": str(round((i['video_watch_total'] / 60), 2))+"分钟",  # 昨日视频学习时长
                    "percent": "%.2f%%" % (i['study_rate'] * 100),  # 总进度百分比
                    "from": i['come_from']  # 来自
                }
                data.append(data_dict)
        update_time = yield self.get_updatetime()
        self.success_response({
            "data": data,
            "update_time": update_time
        })


@route('/mobile/week_list')
class WeekListHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        course_id = self.get_param('course_id')  # base.py封装了多个函数，还有get_param
        query = Search(using=client, index="classmate", doc_type="video_rank_weekly") \
                    .query("term", course_id=course_id) \
                    .sort("rank")  # 按照视频学习时长降序取top10
        size = query[:0].execute().hits.total
        result = query[:size].execute().hits
        if not result:
            data = []
        else:
            data = []
            for i in result:
                data_dict = {
                    "rank": i['rank'],  # 排名
                    "user_id": i['user_id'],
                    "study_rate": "%.2f%%" % (i['study_rate'] * 100),
                    "video_watch_total": str(round((i['video_watch_total'] / 60), 2))+"分钟",
                    "percent": "%.2f%%" % (i['study_rate'] * 100),
                    "from": i['come_from']  # 来自
                }
                data.append(data_dict)
        update_time = yield self.get_updatetime()
        self.success_response({
            "data": data,
            "update_time": update_time
        })


@route('/mobile/classmate')
class ClassmateHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        course_id = self.get_param('course_id')
        query = Search(using=client, index="classmate", doc_type="general_statistics") \
            .query("term", course_id=course_id) \
            .query("term", field_name="general")
        size = query[:0].execute().hits.total
        result = query[:size].execute().hits
        update_time = yield self.get_updatetime()
        if not result:
            data = {}
            self.success_response({
                "data": data,
                "student_total": 0,  # 共有学生总数，ok
                "countries": 0,  # 国家个数, ok
                "country_percent": 0,  # 国内占比
                "foreign_percent": 0,  # 国外占比
                "provinces_from_china": 0,  # 中国学生来自的省市个数ok
                "province_most_people": 0,  # 人数最多的省份 ok
                "age_level": 0,  # 年龄分布最多的阶段 ok
                "education_level": 0,  # 学历分布最多的阶段 ok
                "update_time": update_time  # 更新时间,取自于data_conf
            })
        else:
            statistics = {}
            for i in result:
                statistics[i['field_value']] = i['statistics']
            foreign_percent = round(int(statistics["foreigner_count"]) / int(statistics["total_num"]), 10)
            country_percent = round((int(statistics["total_num"]) - int(statistics["foreigner_count"])) / int(statistics["total_num"]), 10)
            total_num = int(statistics["total_num"])
            foreign_num = int(statistics["foreigner_count"])
            chinese_num = int(statistics["total_num"]) - int(statistics["foreigner_count"])
            data = {}
            data = self.get_five_data(course_id, "gender", data)
            data = self.get_five_data(course_id, "education", data)
            data = self.get_five_data(course_id, "age", data)
            data = self.get_country_province(course_id, "country", data, total_num, foreign_num)
            data = self.get_country_province(course_id, "province", data, total_num, chinese_num)

            self.success_response({
                "data": data,
                "student_total": statistics['total_num'],  # 共有学生总数，ok
                "countries": statistics['country_num'],  # 国家个数, ok
                "country_percent": "%.2f%%" % (country_percent * 100),  # 国内占比
                "foreign_percent": "%.2f%%" % (foreign_percent * 100),  # 国外占比
                "provinces_from_china": statistics['province_num'],  # 中国学生来自的省市个数ok
                "province_most_people": statistics['max_num_province'],  # 人数最多的省份 ok
                "age_level": statistics['max_num_age'],  # 年龄分布最多的阶段 ok
                "education_level": statistics['max_num_education'],  # 学历分布最多的阶段 ok
                "update_time": update_time  # 更新时间,取自于data_conf
            })

    def get_five_data(self, course_id, dim, data):
        query = Search(using=client, index="classmate", doc_type="user_distr") \
            .query("term", course_id=course_id) \
            .query("term", field_name=dim)
        size = query[:0].execute().hits.total
        result = query[:size].execute().hits
        data[dim] = {}
        for i in result:
            data[dim][i['field_value']] = "%.2f%%" % (i['distr'] * 100)
        return data

    def get_country_province(self, course_id, dim, data, total_num, self_num):
        query = Search(using=client, index="classmate", doc_type="general_statistics") \
            .query("term", course_id=course_id) \
            .query("term", field_name=dim)
        size = query[:0].execute().hits.total
        result = query[:size].execute().hits
        data[dim] = {}
        num = 0
        compare_list = []
        for i in result:
            compare_list.append(int(i['statistics']))
            num += int(i['statistics'])

        if self_num - num != 0:
            top19 = 0
            for i in result:
                if int(i['statistics']) == min(compare_list):
                    result.remove(i)
                    break
            for i in result:
                top19 += int(i['statistics'])
                percent = round((int(i['statistics'])) / total_num, 10)
                data[dim][i['field_value']] = "%.2f%%" % (percent * 100)
            percent = round((self_num - top19) / total_num, 10)
            data[dim]["其他"] = "%.2f%%" % (percent * 100)
        else:
            for i in result:
                percent = round((int(i['statistics']) / total_num), 10)
                data[dim][i['field_value']] = "%.2f%%" % (percent * 100)
        return data
