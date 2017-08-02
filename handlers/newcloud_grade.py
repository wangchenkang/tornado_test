#!/usr/bin/env python
# -*- coding: utf-8 -*-

from utils.routes import route
from utils.log import Log
from .base import BaseHandler
import settings
from tornado.web import gen


Log.create('newcloud_grade')

@route('/newcloud_grade/confirm')
class Confirm(BaseHandler):
    """
    课程确认状态（只有新学堂云学分课有确认功能）
    """
    def get(self):
        #TODO
        query = self.es_query(doc_type='course_grade')\
                     .filter('term', course_id=self.course_id)\
                     .filter('term', group_key=self.group_key)
        result = self.es_execute(query[:1])
        data = result.hits
        result = {}
        result['confirm_time'] = ''
        result['confirm_status'] = False
        result['lock_time'] = ''
        result['lock_status'] = False
        if data:
            confirm_time = data[0].confirm_time or ''
            confirm_status = True if confirm_time else False
            lock_time = data[0].lock_time or ''
            lock_status = True if lock_time else False
        self.success_response({'data': result})

@route('/newcloud_grade/indicators')
class Indicator(BaseHandler):
    """
    成绩三大指标（课程平均分，及格人数，不及格人数）
    """
    def get(self):
        #TODO
        query = self.es_query(doc_type='course_grade')\
                    .filter('term', course_id=self.course_id)
                    #.filter('term', group_key=self.group_key)
        query.aggs.metric('average', 'avg', field='grade')
        query.aggs.metric('pass_num', 'range', field='grade', ranges=[{'from': 60}])#passline获取
        query.aggs.metric('not_pass_num', 'range', field='grade', ranges=[{'from': 0, 'to': 60}])
        result = self.es_execute(query)
        data = result
        result = {}
        result['average'] = round(float(data.aggregations.average.value), 2)
        result['pass_num'] = data.aggregations.pass_num.buckets[0].doc_count
        result['not_pass_num'] = data.aggregations.not_pass_num.buckets[0].doc_count
        self.success_response({'data': result})

@route('/newcloud_grade/num_distribution')
class Distribution(BaseHandler):
    """
    成绩人数分布
    """
    @property
    def ranges(self):
        ranges = []
        for i in xrange(10):
            _ = {}
            _['from'] = i*10 + 1
            _['to'] = i*10 + 10
            ranges.append(_)
        return ranges

    @gen.coroutine
    def get(self):
        #TODO
        query_1 = self.es_query(doc_type='course_grade')\
                      .filter('term', course_id=self.course_id)\
                      .filter('term', group_key=self.group_key)
        query_1.aggs.metric('num', 'range', field='grade', ranges=self.ranges)
        query_2 = self.es_query(doc_type='course_grade')\
                      .filter('term', course_id=self.course_id)\
                      .filter('term', group_key=self.group_key)\
                      .filter('range', **{'grade': {'lte':0}})
        result_1 = self.es_execute(query_1)
        result_2 = self.es_execute(query_2).hits
        nums = result_2.total
        aggs = result_1.aggregations.num.buckets
        aggs = [agg.to_dict() for agg in aggs]
        self.success_response({'aggs': aggs, 'zero': nums})
