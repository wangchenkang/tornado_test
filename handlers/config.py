#! -*- coding: utf-8 -*-
from elasticsearch_dsl import Search
from .base import BaseHandler
from utils.routes import route
from utils.log import Log


Log.create('config')

@route('/config/data_date')
class DataDateConfig(BaseHandler):
    """ 
    获取数据有效时间
    """
    def get(self):

        s = Search(using=self.es, index='api1', doc_type='data_conf')
        response = s.execute()

        try:
            config = response[0]

            self.success_response({
                'log_start_date': config.log_start_date,
                'latest_data_date': config.latest_data_date
            })
        except (IndexError, AttributeError):
            self.error_response(101, u'数据时间配置错误')
