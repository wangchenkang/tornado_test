#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log

Log.create('video')

@route('/video/chapter_stat')
class ChapterVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id

        query = self.es_query(index='rollup', doc_type='video_student_num_ds') \
                .filter('term', course_id=course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('term', video_id='-1')

        default_size = 0
        data = self.es_execute(query[:default_size])
        if data.hits.total > default_size:
            data = self.es_execute(query[:data.hits.total])

        video_stat = {
            'total': data.hits.total,
            'sequentials': {}
        }
        for item in data.hits:
            video_stat['sequentials'][item.seq_id] = {
                'student_num': item.view_user_num,
                'review_num': item.review_user_num
            }

        self.success_response(video_stat)


@route('/video/chapter_student_stat')
class ChapterStudentVideo(BaseHandler):
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        uid = self.get_param('user_id')
        students = [u.strip() for u in uid.split(',') if u.strip()]

        video_query = self.es_query(index='main', doc_type='video') \
                .filter('term', course_id=course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('exists', field='duration') \
                .filter('exists', field='study_rate') \
                .filter('terms', uid=students)

        video_data = self.es_execute(video_query[:0])
        video_data = self.es_execute(video_query[:video_data.hits.total])

        chapter_student_stat = {}
        for item in video_data.hits:
            sequential_id = item.sequential_id
            student_id = item.uid
            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id].setdefault(student_id, [])

            chapter_student_stat[sequential_id][str(student_id)].append({
                'video_id': item.vid,
                'review': item.review,
                'review_rate': item.review_rate,
                'watch_num': item.watch_num,
                'duration': item.duration,
                'study_rate': item.study_rate,
                'la_access': item.la_access
            })

        student_query = self.es_query(index='rollup', doc_type='video_user_avg_percent_ds') \
                .filter('term', course_id=course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('terms', user_id=students)
        student_data = self.es_execute(student_query[:video_data.hits.total])
        students_rate = {}
        for item in student_data.hits:
            students_rate.setdefault(item.seq_id, {})
            students_rate[item.seq_id][str(item.user_id)] = item.avg_watch_percent

        result = {
            'total': video_data.hits.total,
            'sequentials': chapter_student_stat,
            'students_rate': students_rate
        }

        self.success_response(result)


@route('/video/course_study_rate')
class CourseVideo(BaseHandler):
    """
    spoc 接口需求：获取课程用户视频观看覆盖率
    """
    def get(self):
        user_id = self.get_argument('user_id', None)

        query = self.es_query(index='rollup', doc_type='course_video_rate') \
                .filter('term', course_id=self.course_id)

        if user_id is not None:
            max_length = 1000
            user_id = [int(u.strip()) for u in user_id.split(',') if u.strip()][0:max_length]
            query = query.filter('terms', uid=user_id)
            data = self.es_execute(query[:max_length])
        else:
            data = self.es_execute(query[:0])
            data = self.es_execute(query[:data.hits.total])

        result = []
        users_has_study = set()
        for item in data.hits:
            # fix uid: anonymous_id-anonymous_id-e03be04b3e8a30b74b9779ace15e1b50
            try:
                item_uid = int(item.uid)
            except ValueError:
                continue
            result.append({
                'user_id': item_uid,
                'study_rate': float(item.study_rate_open)
            })
            users_has_study.add(item_uid)

        if user_id:
            users_not_study = set(user_id).difference(users_has_study)
            for item in users_not_study:
                result.append({
                    'user_id': item,
                    'study_rate': 0.000
                })

        self.success_response({'data': result})


@route('/video/course_study_detail')
class CourseStudyDetail(BaseHandler):
    """
    spoc 接口需求：课程视频观看详情
    """
    def get(self):
        course_id = self.course_id
        user_id = self.get_argument('user_id', None)

        query = self.es_query(index='main', doc_type='video') \
                .filter('term', course_id=course_id) \
                .filter('exists', field='duration')
        max_length = 1000
        if user_id is not None:
            user_id = [u.strip() for u in user_id.split(',') if u.strip()][0:max_length]
            query = query.filter('terms', uid=user_id)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        results = []
        for item in data.hits:
            try:
                item_uid = int(item.uid)
            except ValueError:
                continue
            results.append({
                'user_id': item_uid,
                'chapter_id': item.chapter_id,
                'sequential_id': item.sequential_id,
                'vertical_id': item.vertical_id,
                'video_id': item.vid,
                'watch_num': item.watch_num,
                'video_length': item.duration,
                'study_rate': float(item.study_rate)
            })

        self.success_response({'total': data.hits.total, 'data': results})


@route('/video/chapter_video_stat')
class ChapterVideoStat(BaseHandler):
    def get(self):
        result = {}
        query = self.es_query(index='rollup', doc_type='video_student_num_ds') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=self.chapter_id) \
                .filter('term', seq_id='-1') \
                .filter('term', video_id='-1')

        data = self.es_execute(query)
        hit = data.hits
        result['course_id'] = self.course_id
        result['chapter_id'] = self.chapter_id
        if hit:
            hit = hit[0]
            result['study_num'] = int(hit.view_user_num)
            result['review_num'] = int(hit.review_user_num)
        else:
            result['study_num'] = 0
            result['review_num'] = 0

        self.success_response(result)

@route('/video/chapter_video_detail')
class CourseChapterVideoDetail(BaseHandler):
    def get(self):
        result = []
        query = self.es_query(index='main', doc_type='video')\
                .filter('term', course_id=self.course_id)\
                .filter('term', chapter_id=self.chapter_id)\
                .filter('exists', field='watch_num')[:0]
        query.aggs.bucket("video_watch", "terms", field="vid", size=0)\
                .metric('num', 'range', field="watch_num", ranges=[{"from": 1, "to": 2}, {"from": 2}])
        results = self.es_execute(query)
        aggs = results.aggregations
        buckets = aggs["video_watch"]["buckets"]
        for bucket in buckets:
            vid = bucket["key"]
            items = bucket["num"]["buckets"]
            num1 = 0
            num2 = 0
            for item in items:
                if item["key"] == "1.0-2.0":
                    num1 = item["doc_count"]
                elif item["key"] == "2.0-*":
                    num2 = item["doc_count"]
            result.append({
                "vid": vid,
                "num1": num1,
                "num2": num2
                })
        self.success_response({"data": result})
