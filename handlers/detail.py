#! -*- coding: utf-8 -*-

from utils.routes import route
from utils.tools import var
from utils.log import Log
from .base import BaseHandler

Log.create('detail')

@route('/detail/course_grade_ratio')
class DetailCourseGradeRatio(BaseHandler):
    """
    学生在某门课程中的得分率的平均值和标准差
    计算时需要去掉grade_ratio=-2的学生，即参加课程但没有答题的学生
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.1关键指标
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.2关键指标
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap2.0', doc_type='problem_course') \
                .filter('term', course_id=self.course_id) \
                .filter('term', group_key=self.group_key) \
                .filter('range', grade_ratio={'gt': 0}) \
                .filter('terms', user_id=users)
        result = self.es_execute(query)
        result = self.es_execute(query[:result.hits.total])
        hits = result.hits

        grade_list = [hit.grade_ratio for hit in hits]

        grade_overview = {'mean': 0, 'variance': 0}
        if hits.total:
            grade_list = [float(grade) for grade in grade_list]
            grade_overview['mean'] = round(sum(grade_list) / hits.total, 4)
            grade_overview['variance'] = round(var(grade_list), 4)

        self.success_response({'data': grade_overview})


@route('/detail/student_grade_ratio')
class DetailCourseGradeRatioDetail(BaseHandler):
    """
    课程所有学生的得分率(得分率>=0的）
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.1图
    """
    def get(self):
        problem_users = self.get_problem_users()
        query = self.es_query(index='tap2.0', doc_type='problem_course') \
                .filter('term', course_id=self.course_id) \
                .filter('term', group_key=self.group_key) \
                .filter('terms', user_id=problem_users) \
                .filter('range', **{'final_grade': {'gte': 0}})

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        result = {}
        for hit in response.hits:
            result[hit.user_id] = hit.final_grade

        self.success_response({'data': result})


@route('/detail/student_study_ratio')
class DetailCourseStudyRatioDetail(BaseHandler):
    """
    课程所有学生的学习比例
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.1图
    """
    def get(self):
        users = self.get_video_users()
        query = self.es_query(index='tap', doc_type='video_course') \
            .filter('term', course_id=self.course_id) \
            .filter('terms', user_id=users)

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        result = {}
        for user in response.hits:
            result[user.user_id] = float(user.study_rate)

        self.success_response({'data': result})


@route('/detail/course_study_ratio')
class DetailCourseStudyRatio(BaseHandler):
    """
    课程学习比例的均值和方差
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.1关键指标
    """
    def get(self):
        query = self.es_query(index='tap', doc_type='video_course') \
                    .filter('term', course_id=self.course_id)
        video_users = self.get_video_users()
        query = query.filter('terms', user_id=video_users)

        result = self.es_execute(query)
        result = self.es_execute(query[:result.hits.total])
        hits = result.hits

        video_user_count = 0
        video_user_ratio_list = []
        for hit in hits:
            if float(hit.study_rate) <= 0:
                continue
            video_user_count += 1
            video_user_ratio_list.append(float(hit.study_rate))
        video_overview = {'mean': 0, 'variance': 0}
        if video_user_count:
            video_overview['mean'] = round(sum(video_user_ratio_list) / video_user_count, 4)
            video_overview['variance'] = round(var(video_user_ratio_list), 4)

        self.success_response({'data': video_overview})


@route('/detail/course_discussion')
class DetailCourseDiscussion(BaseHandler):
    """
    课程的发帖/回帖总数和平均数
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.1关键指标
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.3关键指标
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap2.0', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id) \
                .filter("term", group_key=self.group_key) \
                .filter('terms', user_id=users)
        query.aggs.metric('post_total', 'sum', field='post_num') \
                .aggs.metric('comment_total', 'sum', field='reply_num') \
                .aggs.metric('post_mean', 'avg', field='post_num') \
                .aggs.metric('comment_mean', 'avg', field='reply_num')

        response = self.es_execute(query)
        result = {}
        result['post_total'] = response.aggregations.post_total.value or 0
        result['comment_total'] = response.aggregations.comment_total.value or 0
        result['total'] = result['post_total'] + result['comment_total']
        result['post_mean'] = round(response.aggregations.post_mean.value or 0 , 4)
        result['comment_mean'] = round(response.aggregations.comment_mean.value or 0, 4)
        result['total_mean'] = round(float(result['total']) / response.hits.total, 4) if response.hits.total else 0

        self.success_response({'data': result})


@route('/detail/homework_grade')
class DetailHomeworkGrade(BaseHandler):
    """
    每次作业的得分率
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.2关键指标 图
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap2.0', doc_type='exam_seq_grade') \
            .filter('term', course_id=self.course_id) \
            .filter('terms', user_id=users)

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        result = {}
        for hit in response.hits:
            seq_id = hit['seq_id']
            if seq_id not in result:
                result[seq_id] = {'sum_grade': 0, 'student_num': 0}
                result[seq_id]['distribution'] = {}
            result[seq_id]['sum_grade'] += hit['grade']
            result[seq_id]['student_num'] += 100
            if hit['grade'] not in result[seq_id]['distribution']:
                result[seq_id]['distribution'][hit['grade']] = 0
            result[seq_id]['distribution'][hit['grade']] += 1

        for seq in result.keys():
            result[seq]['grade_ratio_mean'] = round((float(result[seq]['sum_grade']) / result[seq]['student_num']), 4)
            del result[seq]['sum_grade']
            del result[seq]['student_num']

        self.success_response({'data': result})


@route('/detail/student_discussion')
class DetailStudentDiscussion(BaseHandler):
    """
    课程所有学生的发帖数，回帖数，总帖子数
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.1图
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap2.0', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id) \
                .filter("term", group_key=self.group_key) \
                .filter('terms', user_id=users)
        
        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])
        result = {}
        for item in response.hits:
            result[item.user_id] = {}
            result[item.user_id]['post_num'] = item.post_num or 0
            result[item.user_id]['reply_num'] = item.reply_num or 0
            result[item.user_id]['total_num'] = result[item.user_id]['post_num'] + result[item.user_id]['reply_num']

        self.success_response({'data': result})


@route('/detail/student_discussion_stat')
class DetailStudentDiscussionStat(BaseHandler):
    """
    按总帖子数进行分组，每组的学生人数
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.3图
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap2.0', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id) \
                .filter("term", group_key=self.group_key) \
                .filter('terms', user_id=users)

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])

        result = {}
        for hit in response.hits:
            post_num = hit['post_num'] or 0
            reply_num = hit['reply_num'] or 0
            total_num = post_num + reply_num
            if total_num == 0:
                continue
            if total_num not in result:
                result[total_num] = 0
            result[total_num] += 1

        self.success_response({'data': result})


@route('/detail/chapter_study_ratio')
class DetailChapterStudyRatio(BaseHandler):
    """
    每一章的学习比例均值
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.4关键指标
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='video_chapter') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=users)

        query.aggs.bucket('chapter', 'terms', field='chapter_id', size=1000) \
            .metric('study_ratio_mean', 'avg', field='study_rate')

        response = self.es_execute(query)

        aggs = response.aggregations.chapter.buckets
        buckets = {}
        student_num = len(users)
        for bucket in aggs:
            buckets[bucket['key']] = {}
            buckets[bucket['key']]['study_ratio_mean'] = round(bucket.study_ratio_mean.value, 4)
            buckets[bucket['key']]['unstudy_student_num'] = student_num - bucket.doc_count

        self.success_response({'data': buckets})


@route('/detail/chapter_study_ratio_detail')
class DetailChapterStudyRatioDetail(BaseHandler):
    """
    每一章的学习比例分组(0-10|11-20...)的人数
    http://confluence.xuetangx.com/pages/viewpage.action?pageId=9044555 1.4图
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='video_chapter') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=users)
        ranges = [{'from': i*0.1, 'to': i*0.1+0.1 } for i in range(0, 10)]

        query.aggs.bucket('chapter', 'terms', field='chapter_id', size=1000) \
            .bucket('study_rate', 'range', field='study_rate', ranges=ranges)

        response = self.es_execute(query[:0])
        response = self.es_execute(query[:response.hits.total])
        result = {}

        for chapter in response.aggregations.chapter.buckets:
            chapter_study_rate = {}
            chapter_study_rate['distribution'] = []
            for range_study_rate in chapter.study_rate.buckets:
                range_study_rate_key = '-'.join([key[:3] for key in range_study_rate.key.split('-')])
                chapter_study_rate['distribution'].append({
                    'study_rate': range_study_rate_key,
                    'num': range_study_rate.doc_count
                })
            result[chapter.key] = chapter_study_rate

        self.success_response({'data': result})


