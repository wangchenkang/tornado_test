#! -*- coding: utf-8 -*-
#from __future__ import division
from .base import BaseHandler
from utils.routes import route
from datetime import datetime, timedelta

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
                    .filter('term', course_id=self.get_param('course_id'))\
                    .sort('-update')
        total = self.es_execute(query).hits.total
        hits = self.es_execute(query[:total]).hits
        last = hits[0] if total != 0 else None
        predict_dropout = 0.0
        predict_active = 0.0
        last_week_error = None
        dropout_num = 0
        dropout_ratio = 0.0
        if last:
            predict_dropout = "%.4f" % float(last.predict_dropout if last.predict_dropout else 0.0)
            predict_active = "%.4f" %  float(last.predict_active_num if last.predict_active_num else 0.0)/\
                                      (float(last.predict_active_num if last.predict_active_num else 0.01) + float(last.predict_dropout_num if last.predict_dropout_num else 0.01))
            last_week_error = "%.4f" % float(last.last_bias if last.last_bias else 0.0)
            dropout_num = int(last.predict_dropout_num if last.predict_dropout_num else 0)
            dropout_ratio = "%.4f" % ((int(last.predict_dropout_num if last.predict_dropout_num else 0) - int(last.last_dropout_num if last.last_dropout_num else 0))/\
                                   float(last.last_dropout_num if last.last_dropout_num else 0.01))
        data = []
        for hit in hits:
            date = hit.update
            active = int(hit.last_active_num if hit.last_active_num else 0)
            dropout = int(hit.last_dropout_num if hit.last_dropout_num else 0)
            overcome = "%.4f"%float(hit.last_dropout if hit.last_dropout != "" else 0.0)
            data.append({'date': (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"),
                         'active': active,
                         'dropout': dropout,
                         'overcome': overcome})
        data.append({'date': last.update if last else None,
                    'active': int(last.predict_active_num if last.predict_active_num else 0) if last else 0,
                    'dropout': int(last.predict_dropout_num if last.predict_dropout_num else 0) if last else 0,
                    'overcome': 1-float(predict_active if predict_active else 0.0)}) 
        self.success_response({'predict_dropout': predict_dropout,
                               'predict_active': predict_active,
                               'last_week_error': last_week_error,
                               'dropout_num': dropout_num,
                               'dropout_ratio': dropout_ratio,
                               'data': data})

@route('/prediction/course_status')
class CourseStatus(BaseHandler):

    def get(self):

        course_id = self.get_param('course_id')
        query = self.es_query(index='prediction', doc_type='dropout')\
                    .filter('term', course_id=course_id)
        total = self.es_execute(query).hits.total
        status = True if total != 0 else False
    
        self.success_response({'status': status})
