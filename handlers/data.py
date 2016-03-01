#! -*- coding: utf-8 -*-
import os
import sys
import hashlib
import codecs
import requests
import tarfile
import shutil
import tempfile
import xlsxwriter
from cStringIO import StringIO
from tornado.escape import url_unescape
from tornado.web import HTTPError, Finish
from elasticsearch import NotFoundError
from .base import BaseHandler
from utils.routes import route
from utils.tools import fix_course_id
from utils.log import Log

reload(sys)
sys.setdefaultencoding('utf-8')

Log.create('data')

download_data_type = { 
    'study_progress': u'学习进度数据',
    'video_study_export': u'视频观看记录',
    'problem_info': u'单习题得分情况',
    'unenroll_info': u'退课记录',
    'user_answer_history': u'单习题单次简答题情况',
    'comments_info': u'单次帖子发帖回帖情况',
    'pdf_view': u'教材浏览记录',
    'learn_info': u'课程访问记录',
    'html_view': u'其它页面浏览记录',
    'enrollment_export': u'选课记录',
    'learn_cnt_info': u'课程访问次数',
    'about_enroll': u'特别关注数据',
    'learn_active_export': u'学习习惯',
    'grade_distribution_export': u'得分分布',
    'course_active_export': u'学习活跃数据',
    'ta_activity_export': u'助教考核数据',
    'comment_active_export': u'讨论活跃数据',
    'grade': u'个人总成绩',
}


@route('/data/types')
class DataTypes(BaseHandler):
    def get(self):
        self.success_response({'data_type': download_data_type})

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
                data[item] = {
                    'id': record['_id'],
                    'course_id': record['_source']['course_id'],
                    'data_type': record['_source']['data_type'],
                    'name': record['_source']['name'],
                    'data_link': record['_source']['data_link'],
                    'update_time': record['_source']['update_time'],
                    'zip_format': record['_source'].get('zip_format', None),
                }
            except NotFoundError:
                data[item] = {}

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

        default_size = 0
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
        if data['hits']['total'] > default_size:
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
    """
    课程导出文件下载
    """
    def get_temp_dir(self):
        temp_dir = tempfile.mktemp(prefix='tapapi_', dir='/tmp')
        os.makedirs(temp_dir)
        return temp_dir

    def get(self):
        data_id = self.get_param('id')
        platform = self.get_argument('os', 'unix').lower()
        file_format = self.get_argument('format', 'xlsx').lower()

        if platform not in ['windows', 'unix']:
            platform = 'windows'

        if file_format != 'csv':
            file_format = 'xlsx'

        try:
            record = self.es.get(index='dataimport', doc_type='course_data', id=data_id)
        except NotFoundError:
            raise HTTPError(404)

        filename = record['_source']['name']
        data_url = record['_source']['data_link']
        zip_format = record['_source'].get('zip_format', None)
        es_file_format = record['_source'].get('file_format', None)

        response = requests.get(data_url)
        if not response.content.strip():
            raise HTTPError(404)

        if zip_format != 'tar':
            if file_format == 'xlsx':
                xlsx_file = StringIO()
                workbook = xlsxwriter.Workbook(xlsx_file, {'in_memory': True})
                worksheet = workbook.add_worksheet()
                lines = response.content.strip().split('\n')
                for row, line in enumerate(lines):
                    for col, item in enumerate(line.split(',')):
                        worksheet.write(row, col, item.strip('"'))

                workbook.close()
                xlsx_file.seek(0)

                filename = filename.rsplit('.', 1)[0] + '.xlsx'
                self.set_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))
                self.write(xlsx_file.read())
            else:
                self.set_header('Content-Type', 'text/csv')
                self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))

                if platform == 'windows':
                    self.write(codecs.BOM_UTF8)
                self.write(response.content)

        else:
            # 如果打包文件是xlsx则直接返回给用户
            if es_file_format and es_file_format == 'xlsx':
                self.set_header('Content-Type', 'application/x-compressed-tar')
                self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))
                self.write(response.content)
                raise Finish

            try:
                temp_dir = tempfile.mktemp(prefix='tapapi_', dir='/tmp')
                os.makedirs(temp_dir)
                temp_tarfile = tempfile.mktemp(suffix='.tar.gz', dir=temp_dir)
                with open(temp_tarfile, 'wb') as fp:
                    fp.write(response.content)

                tar = tarfile.open(temp_tarfile, 'r:gz')
                tar.extractall(temp_dir)

                if file_format == 'xlsx':
                    xlsx_file = StringIO()
                    workbook = xlsxwriter.Workbook(xlsx_file, {'in_memory': True})
                    for item in tar:
                        if not item.isreg():
                            continue

                        item_filename = os.path.join(temp_dir, item.name)
                        work_sheet_name = os.path.split(item.name)[-1].rsplit('.', 1)[0]
                        worksheet = workbook.add_worksheet(work_sheet_name[:30])
                        with open(item_filename, 'rb') as fp:
                            lines = fp.read().strip().split('\n')
                            for row, line in enumerate(lines):
                                for col, item_content in enumerate(line.split(',')):
                                    worksheet.write(row, col, item_content.strip('"'))

                    workbook.close()
                    xlsx_file.seek(0)

                    filename = filename.split('.', 1)[0] + '.xlsx'
                    self.set_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))

                    self.write(xlsx_file.read())
                elif platform == 'windows':
                    win_tmp_tarfile = tempfile.mktemp(suffix='.tar.gz', dir=temp_dir)
                    win_tarfile = tarfile.open(win_tmp_tarfile, 'w')
                    for item in tar:
                        if not item.isreg():
                            continue

                        item_filename = os.path.join(temp_dir, item.name)
                        with codecs.open(item_filename, 'rb', 'utf-8') as fp:
                            item_data = fp.read()
                        with codecs.open(item_filename, 'wb', 'utf-8') as fp:
                            fp.write(codecs.BOM_UTF8)
                            fp.write(item_data)

                        win_tarfile.add(item_filename, item.name)
                    win_tarfile.close()

                    self.set_header('Content-Type', 'application/x-compressed-tar')
                    self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))

                    with open(win_tmp_tarfile, 'rb') as fp:
                        self.write(fp.read())
                else:
                    self.set_header('Content-Type', 'application/x-compressed-tar')
                    self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))
                    self.write(response.content)

            finally:
                shutil.rmtree(temp_dir)
