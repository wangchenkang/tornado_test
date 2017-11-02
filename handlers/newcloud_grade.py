#!/usr/bin/env python
# -*- coding: utf-8 -*-

from utils.routes import route
from .base import BaseHandler
import settings
from tornado.web import gen
from elasticsearch_dsl import MultiSearch
from utils.tools import date_from_new_date

@route('/newcloud_grade/confirm')
class Confirm(BaseHandler):
    """
    课程确认状态（只有新学堂云学分课有确认功能）
    """
    def get(self):
        ms = MultiSearch()
        ms = ms.add(self.search(index=settings.NEWCLOUD_ES_INDEX, doc_type='edx_lock')\
                        .filter('term', course_id=self.course_id)[:1])
        ms = ms.add(self.search(index=settings.NEWCLOUD_ES_INDEX, doc_type='xuetangx_lock')\
                        .filter('term', course_id=self.course_id)\
                        .filter('term', group_key=self.group_key)[:1])
        responses = ms.execute()
        edx = responses[0].hits
        xuetangx = responses[1].hits
        result = {}
        confirm_time = ''
        confirm_status = False
        lock_time = ''
        lock_status = False
        if edx:
            confirm_time = str(date_from_new_date(edx[0].lock_time)) or ''
            confirm_status = True if confirm_time else False
        if xuetangx:
            lock_time = str(date_from_new_date(xuetangx[0].lock_time)) or ''
            lock_status = True if lock_time else False
        
        result['confirm_time'] = confirm_time
        result['confirm_status'] = confirm_status
        result['lock_time'] = lock_time
        result['lock_status'] = lock_status
        self.success_response({'data': result})


@route('/newcloud_grade/platform/confirm')
class PlatConfirm(BaseHandler):
    """
    课程确认状态（只有新学堂云学分课有确认功能）
    """
    def get(self):
        ms = MultiSearch()
        termcourse_id = self.get_param('termcourse_id')
        ms = ms.add(self.search(index=settings.NEWCLOUD_ES_INDEX, doc_type='edx_lock')\
                        .filter('term',course_id=self.course_id)[:1])
        ms = ms.add(self.search(index=settings.NEWCLOUD_ES_INDEX, doc_type='xuetangx_lock')\
                        .filter('term', course_id=termcourse_id)[:1])
        responses = ms.execute()
        edx = responses[0].hits
        xuetangx = responses[1].hits
        result = {}
        confirm_time = ''
        confirm_status = False
        lock_time = ''
        lock_status = False
        if edx:
            confirm_time = str(date_from_new_date(edx[0].lock_time)) or ''
            confirm_status = True if confirm_time else False
        if xuetangx:
            lock_time = str(date_from_new_date(xuetangx[0].lock_time)) or ''
            lock_status = True if lock_time else False

        result['confirm_time'] = confirm_time
        result['confirm_status'] = confirm_status
        result['lock_time'] = lock_time
        result['lock_status'] = lock_status
        self.success_response({'data': result})

@route('/newcloud_grade/indicators')
class Indicator(BaseHandler):
    """
    成绩三大指标（课程平均分，及格人数，不及格人数）
    """
    def get(self):
        query = self.es_query(index=settings.NEWCLOUD_ES_INDEX, doc_type='score_realtime')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)
        query.aggs.metric('average', 'avg', field='final_score')
        query.aggs.metric('pass_num', 'range', field='final_score', ranges=[{'from': self.get_param('passline')}])#passline获取
        query.aggs.metric('not_pass_num', 'range', field='final_score', ranges=[{'from': 0, 'to': self.get_param('passline')}])
        result = self.es_execute(query)
        data = result
        result = {}
        result['average'] = round(float(data.aggregations.average.value or 0), 2)
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
            _['from'] = i*10 + 0.1
            _['to'] = i*10 + 10.01
            ranges.append(_)
        return ranges

    def get(self):
        query = self.es_query(index=settings.NEWCLOUD_ES_INDEX, doc_type='score_realtime')\
                      .filter('term', course_id=self.course_id)\
                      .filter('term', group_key=self.group_key)
        query.aggs.metric('num', 'range', field='final_score', ranges=self.ranges)
        query_2 = query.filter('range', **{'final_score': {'lte':0}})
        result_1 = self.es_execute(query)
        result_2 = self.es_execute(query_2).hits
        nums = result_2.total
        aggs = result_1.aggregations.num.buckets
        aggs = [agg.to_dict() for agg in aggs]
        self.success_response({'aggs': aggs, 'zero': nums})

@route('/newcloud_grade/datetime')
class DateTime(BaseHandler):
    """
    新学堂云成绩最新更新时间
    """
    def get(self):
        query = self.es_query(index=settings.NEWCLOUD_ES_INDEX, doc_type='data_conf')\
            .filter('terms', _id=settings.NEWCLOUD_DATACONF)[:7]
        results = self.es_execute(query).hits
        data = []
        for result in results:
            data.append(result.latest_data_date)
        self.success_response({'data': data})
