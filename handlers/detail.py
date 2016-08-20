#! -*- coding: utf-8 -*-

from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler

Log.create('detail')

@route('/detail/course_grade_ratio')
class DetailCourseGradeRatio(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='problem_course') \
                .filter('term', course_id=self.course_id)
        problem_users = self.get_problem_users()
        query.filter('terms', user_id=problem_users)

        result = self.es_execute(query)
        result = self.es_execute(query[:result.hits.total])
        hits = result.hits

        problem_user_count = 0
        grade_ratio_sum = 0
        problem_user_grade_ratio_list = []
        for hit in hits:
            if hit.grade_ratio == -2:
                continue
            grade_ratio_sum += hit.grade_ratio
            problem_user_count += 1
            problem_user_grade_ratio_list.append(hit.grade_ratio)

        grade_overview = {'mean': 0, 'variance': 0}
        if problem_user_count:
            grade_overview['mean'] = float(grade_ratio_sum) / problem_user_count
            grade_overview['variance'] = var(problem_user_grade_ratio_list)

        Log.debug(grade_overview)
        self.success_response({'data': grade_overview})


@route('/detail/student_grade_ratio')
class DetailCourseGradeRatioDetail(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='problem_course') \
                .filter('term', course_id=self.course_id) \
                .filter('range', **{'grade_ratio': {'gt': 0}})

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        result = {}
        for user in response.hits:
            result[user.user_id] = user.grade_ratio

        self.success_response({'data': result})


@route('/detail/student_study_ratio')
class DetailCourseStudyRatioDetail(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='video_course') \
            .filter('term', course_id=self.course_id)

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        result = {}
        for user in response.hits:
            result[user.user_id] = float(user.study_rate)

        self.success_response({'data': result})


@route('/detail/course_study_ratio')
class DetailCourseStudyRatio(BaseHandler):
    def get(self):
        query = self.es_query(index='rollup', doc_type='course_video_rate') \
                .filter('term', course_id=self.course_id)
        video_users = self.get_video_users()
        query.filter('terms', user_id=video_users)

        result = self.es_execute(query)
        result = self.es_execute(query[:result.hits.total])
        hits = result.hits

        video_user_count = 0
        video_ratio_sum = 0
        video_user_ratio_list = []
        for hit in hits:
            if float(hit.study_rate_open) <= 0:
                continue
            video_ratio_sum += float(hit.study_rate_open) * 100
            video_user_count += 1
            video_user_ratio_list.append(float(hit.study_rate_open) * 100)

        video_overview = {'mean': 0, 'variance': 0}
        if video_user_count:
            video_overview['mean'] = video_ratio_sum / video_user_count
            video_overview['variance'] = var(video_user_ratio_list)

        self.success_response({'data': video_overview})


@route('/detail/course_discussion')
class DetailCourseDiscussion(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id)
        query.aggs.metric('post_total', 'sum', field='post_num') \
                .aggs.metric('comment_total', 'sum', field='reply_num') \
                .aggs.metric('post_mean', 'avg', field='post_num') \
                .aggs.metric('comment_mean', 'avg', field='reply_num')

        response = self.es_execute(query)
        result = {}
        result['post_total'] = response.aggregations.post_total.value
        result['comment_total'] = response.aggregations.comment_total.value
        result['total'] = result['post_total'] + result['comment_total']
        result['post_mean'] = response.aggregations.post_mean.value
        result['comment_mean'] = response.aggregations.comment_mean.value
        result['total_mean'] = float(result['total']) / response.hits.total

        self.success_response({'data': result})


@route('/detail/student_discussion')
class DetailStudentDiscussion(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id)

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        result = {}
        for item in response.hits:
            result[item.user_id] = item.post_num + item.reply_num

        self.success_response({'data': result})

#TODO
@route('/detail/student_discussion_stat')
class DetailStudentDiscussionStat(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id)
        # need to add post_total field
        query.aggs.bucket('post', 'terms', field='post_total', size=0) \

        response = self.es_execute(query)

        result = []
        for item in response.aggregations.post.buckets:
            result.append({
                'post_num': item['key'],
                'student_num': item.doc_count
            })

        self.success_response({'data': result})


#TODO
@route('/detail/chapter_study_ratio')
class DetailChapterStudyRatio(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='video_chapter') \
                .filter('term', course_id=self.course_id)
        # study_rate is string now, should be numeric value
        query.aggs.bucket('chapter', 'terms', field='chapter_id', size=0) \
            .metric('study_ratio_mean', 'avg', field='study_rate')

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        aggs = response.aggregations.chapter.buckets
        buckets = {}
        for bucket in aggs:
            buckets[bucket['key']] = bucket.study_ratio_mean.value
        self.success_response({'data': buckets})


#TODO
@route('/detail/chapter_study_ratio_detail')
class DetailChapterStudyRatioDetail(BaseHandler):
    def get(self):
        query = self.es_query(index='tap', doc_type='video_chapter') \
                .filter('term', course_id=self.course_id)
        ranges = [{'from': i*0.1, 'to': i*0.1+0.1 } for i in range(0,9)]
        # study_rate is string now, should be numeric value
        query.aggs.bucket('chapter', 'terms', field='chapter_id', size=0) \
            .bucket('study_rate', 'range', field='study_rate', ranges=ranges, size=0) \

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])
        result = []

        for chapter in response.aggregations.chapter.buckets:
            result.append({chapter.key: []})
            for range_study_rate in chapter.study_rate.buckets:
                result[chapter.key].append({
                  'study_rate': range_study_rate.key,
                  'num': range_study_rate.doc_count
                })

        self.success_response({'data': result})


