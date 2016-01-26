#! -*- coding: utf-8 -*-
from __future__ import division
from .base import BaseHandler
from utils.routes import route


@route('/prediction/certificate')
class CertificatePrediction(BaseHandler):
    """
    证书预测
    """
    def get(self):
        query = self.es_query(index='prediction', doc_type='certificate') \
                .filter('term', course_id=self.course_id) \
                .filter('range', **{'probability': {'gte': 0.5, 'lte': 1}})

        update_date_query = query.sort('-update')[:1]
        update_date_data = self.es_execute(update_date_query)

        try:
            update_date = update_date_data.hits[0]['update']
        except IndexError:
            update_date = None

        data_query = query.filter('term', update=update_date)[:0]
        data_query.aggs.metric('prob_total', 'sum', field='probability')

        data = self.es_execute(data_query)
        prob_avg = round(data.aggregations.prob_total.value / data.hits.total, 4)

        self.success_response({'probability': prob_avg, 'students': data.hits.total})


@route('/prediction/dropout')
class DropoutPrediction(BaseHandler):
    """
    流失预测
    """
    def get(self):
        query = self.es_query(index='prediction', doc_type='dropout') \
                .filter('term', course_id=self.course_id) \
                .filter('range', **{'probability': {'gte': 0.5, 'lte': 1}})

        update_date_query = query.sort('-update')[:1]
        update_date_data = self.es_execute(update_date_query)

        try:
            update_date = update_date_data.hits[0]['update']
        except IndexError:
            update_date = None

        data_query = query.filter('term', update=update_date)[:0]
        data_query.aggs.metric('prob_total', 'sum', field='probability')

        data = self.es_execute(data_query)
        prob_avg = round(data.aggregations.prob_total.value / data.hits.total, 4)

        self.success_response({'probability': prob_avg, 'students': data.hits.total})
