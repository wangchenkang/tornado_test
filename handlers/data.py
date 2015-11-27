#! -*- coding: utf-8 -*-
import os
import sys
import hashlib
import time
import random
import codecs
import unicodecsv
import requests
import commands
from cStringIO import StringIO
from tornado.escape import url_unescape
from tornado.web import HTTPError
from elasticsearch import NotFoundError
from .base import BaseHandler
from utils.routes import route
from utils.tools import fix_course_id
from utils.log import Log

reload(sys)
sys.setdefaultencoding('utf-8')

Log.create('data')

@route('/data/export')
class DataExport(BaseHandler):
    """ 
    获取导出文件记录
    """
    def get(self):
        course_id = self.course_id
        data_type = self.get_param('data_type')
        
        data_type = [item.strip() for item in data_type.split(',') if item.strip()]

        data = {}
        for item in data_type:
            data_id = hashlib.md5(course_id + item).hexdigest()
            try:
                record = self.es.get(index='dataimport', doc_type='course_data', id=data_id)
            except NotFoundError:
                data[item] = {}

            data[item] = {
                'id': record['_id'],
                'course_id': record['_source']['course_id'],
                'data_type': record['_source']['data_type'],
                'name': record['_source']['name'],
                'data_link': record['_source']['data_link'],
                'update_time': record['_source']['update_time'],
                'zip_format': record['_source'].get('zip_format', None),
            }

        self.success_response(data)


@route('/data/export/binding_org')
class DataBindingOrg(BaseHandler):
    """
    获取学堂选修课导出文件
    """
    def get(self):
        course_id = self.get_argument('course_id', None)
        org = self.get_argument('org', None)
        data_type = self.get_param('data_type')

        data_type = [item.strip() for item in data_type.split(',') if item.strip()]
        
        filters = [
            {'exists': {'field': 'binding_org'}},
            {'terms': {'data_type': data_type}},
        ]

        if course_id is not None:
            course_id = [fix_course_id(item.strip()) for item in course_id.split(',') if item.strip()]
            filters.append({'terms': {'course_id': course_id}})

        if org is not None:
            filters.append({'term': {'binding_org': url_unescape(org)}})

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'and': {
                            'filters': filters
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='dataimport', doc_type='course_data', body=query)
        if data['hits']['total']> default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='dataimport', doc_type='course_data', body=query)

        courses = {}
        for item in data['hits']['hits']:
            courses.setdefault(item['_source']['data_type'], {})
            courses[item['_source']['data_type']].setdefault(item['_source']['course_id'], []).append({
                'id': item['_id'],
                'name': item['_source']['name'],
                'data_type': item['_source']['data_type'],
                'data_link': item['_source']['data_link'],
                'course_id': item['_source']['course_id'],
                'update_time': item['_source']['update_time'],
                'binding_org': item['_source'].get('binding_org', None),
                'zip_format': item['_source'].get('zip_format', None),
            })

        result = dict.fromkeys(data_type, {})
        result.update(courses)

        self.success_response(result)


@route('/data/download')
class DataDownload(BaseHandler):

    def wget_and_convert(self, data_link, platform):

        def exec_cmd(cmd):
            status, out = commands.getstatusoutput(cmd)
            if status:
                Log.error('exec %s failed: {}, out: {}'.format(cmd, out))
            return status, out 

        tar_file_dir ='/tmp/tapapi/tar_file/{}_{}/'.format(int(time.time()), random.randint(1000, 9999))

        exec_cmd('rm -rf {}'.format(tar_file_dir))
        exec_cmd('mkdir -p {}'.format(tar_file_dir))
        os.chdir(tar_file_dir)
    
        exec_cmd('wget {}'.format(data_link))
        download_file = data_link.rsplit('/', 1)[1]

        if platform == 'unix':
            return tar_file_dir, download_file

        exec_cmd('tar -zxvf {}'.format(download_file))
    
        sub_dir = download_file.rstrip('.tar.gz')
        for _dir in os.walk(sub_dir):
            for f in _dir[2]:
                with codecs.open(os.path.join(sub_dir, f), 'rb', 'utf-8') as fp:
                    _data = fp.read()
                with codecs.open(os.path.join(sub_dir, f), 'wb', 'utf-8') as fp:
                    fp.write(codecs.BOM_UTF8)
                    fp.write(_data)
    
        # exec_cmd('rm -rf {}'.format(download_file))
        exec_cmd('mv {} {}'.format(download_file, 'old_' + download_file))
        exec_cmd('tar -zcvf {} {}'.format(download_file, sub_dir))

        return tar_file_dir, download_file

    def get(self):
        data_id = self.get_param('id')
        platform = self.get_argument('os', 'unix').lower()
    
        if platform not in ['windows', 'unix']:
            platform = 'unix'

        try:
            record = self.es.get(index='dataimport', doc_type='course_data', id=data_id)
        except NotFoundError:
            raise HTTPError(404)

        filename = record['_source']['name']
        data_url = record['_source']['data_link']
        zip_format = record['_source'].get('zip_format', None)
        
        if zip_format != 'tar':
            response = requests.get(data_url)

            self.set_header('Content-Type', 'text/csv')
            self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))

            if platform == 'windows':
                self.write(codecs.BOM_UTF8)
            self.write(response.content)

        else:
            tmp_dir, tmp_file = self.wget_and_convert(data_url, platform)
            self.set_header('Content-Type', 'application/x-compressed-tar')
            self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))
            with open(tmp_dir + tmp_file, 'rb') as f:
                self.write(f.read())
