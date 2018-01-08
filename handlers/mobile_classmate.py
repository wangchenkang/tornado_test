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


@route('/mobile/day_list')
class DayListHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        course_id = self.get_param('course_id')  # base.py封装了多个函数，还有get_param
        update_time = yield self.get_updatetime()
        query = self.es_query(index='classmate', doc_type='video_rank_daily') \
                    .filter('term', course_id=course_id) \
                    .filter('term', rank_date = update_time) \
                    .sort('rank')  # 按照视频学习时长降序取top10
        results = self.es_execute(query).hits
        data = [] 
        for result in results:
            item = {}
            item['rank'] = result['rank']
            item['study_rate'] = '%.2f%%' % (result['study_rate'] * 100)
            item['video_watch_total'] = '%s分钟' % round(result['video_watch_total']/ 60.0, 2)
            item['percent'] = '%.2f%%' % (result['study_rate'] * 100)
            item['from'] = result['come_from']
            item['user_id'] = result.user_id
            data.append(item)
        update_time = '%s 23:59:59' % update_time
        self.success_response({'data': data, 'update_time': update_time})

@route('/mobile/week_list')
class WeekListHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        course_id = self.get_param('course_id')  # base.py封装了多个函数，还有get_param
        update_time = yield self.get_updatetime()
        query = self.es_query(index='classmate', doc_type='video_rank_weekly') \
                    .filter('term', course_id=course_id) \
                    .filter('term', rank_date = update_time) \
                    .sort('rank')  # 按照视频学习时长降序取top10
        results = self.es_execute(query).hits
        data = [] 
        for result in results:
            item = {}
            item['rank'] = result['rank']
            item['study_rate'] = '%.2f%%' % (result['study_rate'] * 100)
            item['video_watch_total'] = '%s分钟' % round(result['video_watch_total']/60.0, 2)
            item['percent'] = '%.2f%%' % (result['study_rate'] * 100)
            item['from'] = result['come_from']
            item['user_id'] = result.user_id
            data.append(item)
        update_time =  '%s 23:59:59' % update_time
        self.success_response({'data': data, 'update_time': update_time})


@route('/mobile/classmate')
class ClassmateHandler(BaseHandler):
    @gen.coroutine
    def get(self):
        course_id = self.get_param('course_id')
        query = self.es_query(index='classmate', doc_type='general_statistics') \
                    .filter('term', course_id=course_id) \
                    .filter('term', field_name='general')
        total = self.es_execute(query[:0]).hits.total
        total = 10000 if total > 10000 else total
        results = self.es_execute(query[:total]).hits
        update_time = yield self.get_updatetime()
        update_time = '%s 23:59:59' % update_time
        if not results:
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
            for result in results:
                statistics[result['field_value']] = result['statistics']
            foreign_percent = round(int(statistics["foreigner_count"]) / float(statistics["total_num"]), 10)
            country_percent = round((int(statistics["total_num"]) - int(statistics["foreigner_count"])) / float(statistics["total_num"]), 10)
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
        query = self.es_query(index='classmate', doc_type='general_statistics') \
                    .filter('term', course_id=course_id) \
                    .filter('term', field_name=dim)
        total = self.es_execute(query[:0]).hits.total
        total = 10000 if total > 10000 else total
        results = self.es_execute(query[:total]).hits
        data[dim] = {}
        for result in results:
            total += int(result['statistics'])
        for result in results:
            percent = round(int(result['statistics']) /float(total), 10)
            data[dim][result['field_value']] = "%.2f%%" % (percent * 100)
        return data

    def get_country_province(self, course_id, dim, data, total_num, self_num):
        query = self.es_query(index='classmate', doc_type='general_statistics') \
                    .filter('term', course_id=course_id) \
                    .filter('term', field_name=dim)
        total = self.es_execute(query[:0]).hits.total
        total = 10000 if total >10000 else total
        result = self.es_execute(query[:total]).hits
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
                data[dim][i['field_value']] = int(i['statistics'])
            data[dim]["其他"] = self_num - top19
        else:
            for i in result:
                data[dim][i['field_value']] = int(i['statistics'])
        
        return data
