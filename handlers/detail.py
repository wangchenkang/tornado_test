#! -*- coding: utf-8 -*-

from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler

Log.create('detail')

@route('/detail/course_grade_ratio')
class CourseGradeRatio(BaseHandler):
    def get(self):
        uid = self.get_argument('user_id', '')
        users = [u.strip() for u in uid.split(',') if u.strip()]

        query = self.es_query(index='tap', doc_type='problem_course') \
                .filter('term', course_id=self.course_id)
        if users:
            query.filter('terms', user_id=users)
        else:
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


@route('/detail/course_video_study_ratio')
class CourseVideoStudyRatio(BaseHandler):
    def get(self):
        uid = self.get_argument('user_id', '')
        users = [u.strip() for u in uid.split(',') if u.strip()]

        query = self.es_query(index='rollup', doc_type='course_video_rate') \
                .filter('term', course_id=self.course_id)
        if users:
            query.filter('terms', user_id=users)
        else:
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


@route('/detail/discussion_overview')
class CourseDiscussionOverview(BaseHandler):
    def get(self):
        uid = self.get_argument('user_id', '')
        users = [u.strip() for u in uid.split(',') if u.strip()]

        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id)
        if users:
            query.filter('terms', user_id=users)
        query.aggs.metric('post_total', 'sum', field='post_num') \
                .aggs.metric('comment_total', 'sum', field='reply_num') \
                .aggs.metric('post_mean', 'avg', field='post_num') \
                .aggs.metric('comment_mean', 'avg', field='reply_num')

        response = self.es_execute(query)
        result = {}
        result['post_total'] = response.aggregations.post_total.value
        result['comment_total'] = response.aggregations.comment_total.value
        result['post_mean'] = response.aggregations.post_mean.value
        result['comment_mean'] = response.aggregations.comment_mean.value

        self.success_response({'data': result})

