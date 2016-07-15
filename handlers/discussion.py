#! -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from elasticsearch_dsl import Q, F
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
from utils.tools import date_from_query, date_to_str, utc_to_cst
from collections import defaultdict
Log.create('discussion')

@route('/discussion/course_stat')
class CourseDiscussion(BaseHandler):
    """
    获取给定课程讨论区发帖情况统计信息
    包括总体和各group的发帖和回复数统计
    """
    def get(self):

        query = self.es_query(index='main', doc_type='group_daily') \
                .filter('term', course_id=self.course_id)[:0]
        query.aggs.bucket('groups', 'terms', field='group_id', size=0) \
                .metric('posts_total', 'sum', field='post_number') \
                .metric('comments_total', 'sum', field='comment_number') \
                .metric('group_num', 'terms', field='group_member_num', size=1, order={'_term': 'desc'})

        data = self.es_execute(query)
        posts_total = 0
        comments_total = 0
        group_dict = {}
        for group in data.aggregations.groups.buckets:
            try:
                member_count = int(group.group_num.buckets[0].key)
            except (KeyError, IndexError, AttributeError):
                member_count = 0
            group_posts_total = int(group.posts_total.value)
            group_comments_total = int(group.comments_total.value)

            group_dict[group.key] = {
                'posts_total': group_posts_total,
                'comments_total': group_comments_total,
                'members_total': member_count
            }
            posts_total += group_posts_total
            comments_total += group_comments_total

        result = {
            'total': data.hits.total,
            'posts_total': posts_total,
            'comments_total': comments_total,
            'groups': group_dict
        }

        self.success_response(result)


@route('/discussion/course_daily_stat')
class CourseDailyStat(BaseHandler):
    def get(self):
        start = self.get_param('start')
        end = self.get_param('end')

        start = date_from_query(start)
        end = date_from_query(end)
        days = (end - start).days + 1

        query = self.es_query(index='main', doc_type='group_daily') \
                .filter('term', course_id=self.course_id) \
                .filter('range', **{'d_day': {'gte': start, 'lte': end}})
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        date_list = [date_to_str(start + timedelta(days=d)) for d in xrange(0, days)]
        date_result = {d: {} for d in date_list}
        for item in data:
            date_result[item.d_day][item.group_id] = {
                'date': item.d_day,
                'group_id': item.group_id,
                'post_number': item.post_number,
                'comment_number': item.comment_number
            }

        self.success_response({'date': date_result})


@route('/discussion/chapter_stat')
class ChapterDiscussion(BaseHandler):
    """
    获取给定章的讨论信息统计
    """
    def get(self):
        chapter_id = self.get_argument('chapter_id')

        query = self.es_query(index='main', doc_type='discussion') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('exists', field='uid')[:0]
        query.aggs.metric('sequentials', 'terms', field='sequential_id', size=0)

        data = self.es_execute(query)
        discussion_stat = {
            'total': data.hits.total,
            'sequentials': {}
        }
        for item in data.aggregations.sequentials.buckets:
            discussion_stat['sequentials'][item.key] = {
                'student_num': item.doc_count
            }

        self.success_response(discussion_stat)


@route('/discussion/chapter_student_stat')
class ChapterStudentDiscussion(BaseHandler):
    """
    获取给定章的课程讨论统计(所有学生)
    """
    def get(self):
        chapter_id = self.chapter_id
        uid = self.get_param('user_id')
        students = [u.strip() for u in uid.split(',') if u.strip()]

        query = self.es_query(index='main', doc_type='discussion') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('terms', uid=students)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        chapter_student_stat = {}
        for item in data.hits:
            sequential_id = item.sequential_id
            student_id = item.uid
            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id].setdefault(student_id, [])
            student_discussion_item = {
                'item_id': item.item_id,
                'post_num': item.post_num,
                'reply_num': item.reply_num,
                'time': item.time
            }
            chapter_student_stat[sequential_id][student_id].append(student_discussion_item)

        result = {
            'total': data.hits.total,
            'sequentials': chapter_student_stat
        }

        self.success_response(result)


@route('/discussion/no_comment_posts_daily')
class CoursePostsNoCommentDaily(BaseHandler):
    """
    获取给定date范围的无评论帖子数
    """
    def get(self):
        start = self.get_param('start')
        end = self.get_param('end')

        start = date_from_query(start)
        end = date_from_query(end)
        size = (end - start).days + 1

        query = self.es_query(index='main', doc_type='discuss_no_reply_daily') \
                .filter('term', course_id=self.course_id) \
                .filter('range', **{'date': {'gte': start, 'lte': end}})[:size]
        data = self.es_execute(query)

        date_list = [date_to_str(start + timedelta(days=d)) for d in xrange(0, size)]
        date_result = {d: {'date': d, 'num': 0} for d in date_list}
        for item in data.hits:
            date_result.update({item.date: {
                'date': item.date,
                'num': item.daily_num
            }})

        self.success_response({'date': sorted(date_result.values(), key=lambda x: x['date'])})


@route('/discussion/no_comment_posts')
class CoursePostsNoComment(BaseHandler):
    """
    获取没有评论的总帖子数
    """
    def get(self):

        query = self.es_query(index='main', doc_type='discuss_no_reply_all') \
                .filter('term', course_id=self.course_id)
        data = self.es_execute(query)

        try:
            count = data[0]['all_num']
        except (KeyError, IndexError):
            count = 0

        self.success_response({'count': count})


@route('/discussion/students_top_stat')
class StudentPostTopStat(BaseHandler):
    """
    获取学生发帖TOP-N统计
    """
    def get(self):
        top = self.get_argument('top', 5)
        order = self.get_argument('order', 'post')

        order_field = 'comments_total' if order == 'comment' else 'posts_total'

        query = self.es_query(index='main', doc_type='user_daily') \
                .filter('term', course_id=self.course_id)[:0]
        query.aggs.bucket('students', 'terms', field='user_id', size=top, order={order_field: 'desc'}) \
                .metric('posts_total', 'sum', field='post_number') \
                .metric('comments_total', 'sum', field='comment_number')
        data = self.es_execute(query)

        students = {}
        for item in data.aggregations.students.buckets:
            students[item.key] = {
                'user_id': int(item.key),
                'posts_total': int(item.posts_total.value),
                'comments_total': int(item.comments_total.value)
            }

        query = self.es_query(index='main', doc_type='user_daily') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=students.keys())
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        for item in data.hits:
            students[item.user_id].setdefault('detail', {})
            students[item.user_id]['detail'][item.d_day] = {
                    'post_number': item.post_number,
                    'comment_number': item.comment_number,
                    'date': item.d_day
                }

            students[item.user_id].setdefault('user_name', item.user_name)
            students[item.user_id].setdefault('group_id', item.group_id)
            students[item.user_id].setdefault('user_id', int(item.group_id))

        students_list = sorted(students.values(), key=lambda x: x['posts_total'], reverse=True)

        self.success_response({'students': students_list})


@route('/discussion/students_detail')
class StudentDetail(BaseHandler):
    """
    获取所有参与讨论学生统计
    """
    def get(self):

        query = self.es_query(index='main', doc_type='user_sum') \
                .filter('term', course_id=self.course_id) \
                .query(Q('range', **{'post_number': {'gt': 0}}) | Q('range', **{'comment_number': {'gt': 0}}))
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        students_detail = {}
        for item in data.hits:
            students_detail[item.user_id] = {
                'user_id': int(item.user_id),
                'post_number': item.post_number,
                'comment_number': item.comment_number,
                'grade_percent': item.grade_percent,
                'group_id': item.group_id,
                'user_name': item.user_name
            }

        self.success_response({'students': students_detail})


@route('/discussion/students_relation')
class StudentRelation(BaseHandler):
    def get(self):

        query = self.es_query(index='main', doc_type='comment_num') \
                .filter('term', course_id=self.course_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        relations = []
        for item in data.hits:
            relations.append({
                'user_id1': item.user_id1,
                'user_id2': item.user_id2,
                'num': item.num
            })

        self.success_response({'relations': relations})


@route('/discussion/course_rank_stat')
class CourseRankStat(BaseHandler):
    def get(self):
        # 获得course_list的elective
        result = {}
        query = self.es_query(doc_type='course')\
                .filter('range', status={'gte': 0})
        if self.elective:
            query = query.filter('term', elective=self.elective)
        else:
            query = query.filter(~F('exists', field='elective'))
        hits = self.es_execute(query[:1000]).hits
        if hits.total > 1000:
            hits = self.es_execute(query[:hits.total]).hits
        course_list = [hit.course_id for hit in hits]
        # 获得course_list下的所有数据
        query = self.es_query(doc_type='discussion_aggs')\
                .filter('terms', course_id=course_list)
        hits = self.es_execute(query[:0]).hits
        hits = self.es_execute(query[:hits.total]).hits
        # 获得发帖回复率数据
        # 发帖回复率=(帖子总数-零回复总数)/帖子总数
        # 帖子总数=所有人发帖数之和
        def default_func():
            return [0, 0]
        reply_dict = defaultdict(default_func)
        # 第一个是帖子数，第二个是零回复数
        for hit in hits:
            reply_dict[hit.course_id][0] += int(hit.post_num)
            reply_dict[hit.course_id][1] += int(hit.no_reply_num)
        sorted_dict = sorted(reply_dict.items(), 
                key=lambda x: (x[1][0]-x[1][1])/float(x[1][0]),
                reverse=True)
        reply_index = len(sorted_dict)
        reply_ratio = 0
        no_reply_num = 0
        for i, item in enumerate(sorted_dict):
            if item[0] == self.course_id:
                reply_index = i
                reply_ratio = (item[1][0] - item[1][1]) / float(item[1][0])
                no_reply_num = item[1][1]
                break
        if reply_ratio == 1:
            reply_overcome = 1 - 1 / float(len(course_list))
        elif reply_ratio == 0:
            reply_overcome = 0
        else:
            reply_overcome = 1 - (reply_index+1)/float(len(course_list))
        result["reply_ratio"] = reply_ratio
        result["no_reply_num"] = no_reply_num
        result["reply_overcome"] = reply_overcome
        # 讨论区人均互动次数
        # 人均互动次数=(发帖数+回帖数)/总人数
        enroll_dict = self.get_enroll(elective=self.elective)
        total_comment = defaultdict(float)
        for hit in hits:
            total_comment[hit.course_id] += int(hit.post_num) / float(enroll_dict[hit.course_id])
            total_comment[hit.course_id] += int(hit.reply_num) / float(enroll_dict[hit.course_id])
        sorted_dict = sorted(total_comment.items(),
                key=lambda x: x[1],
                reverse=True)
        discussion_avg = 0
        for i, item in enumerate(sorted_dict):
            if item[0] == self.course_id:
                discussion_index = i
                discussion_avg = item[1]
                break
        if discussion_avg == 0:
            discussion_overcome = 0
        else:
            discussion_overcome = 1 - (discussion_index+1)/float(len(course_list))
        result["discussion_avg"] = discussion_avg
        result["discussion_overcome"] = discussion_overcome
        # 参与规模
        # 讨论区参与规模只计人数，不计每人发回帖数
        discussion_student_list = defaultdict(list)
        for hit in hits:
            if hit.user_id not in discussion_student_list[hit.course_id]:
                discussion_student_list[hit.course_id].append(hit.user_id)
        sorted_dict = sorted(discussion_student_list.items(),
                key=lambda x: len(x[1]),
                reverse=True)
        discussion_num = 0
        for i, item in enumerate(sorted_dict):
            if item[0] == self.course_id:
                discussion_num_index = i
                discussion_num = len(item[1])
                break
        if discussion_num == 0:
            discussion_num_overcome = 0
        else:
            discussion_num_overcome = 1 - (discussion_num_index+1)/float(len(course_list))
        result["discussion_num"] = discussion_num
        result["discussion_num_overcome"] = discussion_num_overcome
        self.success_response(result)


@route('/discussion/chapter_discussion_stat')
class CourseDiscussionStat(BaseHandler):
    def get(self):

        result = {}
        query = self.es_query(index='api1', doc_type='comment_problem') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=self.chapter_id) \
                .sort('-date')[:1]
        data = self.es_execute(query)
        hit = data.hits
        result['course_id'] = self.course_id
        result['chapter_id'] = self.chapter_id
        if hit:
            result['students_num'] = int(hit[0].num)
        else:
            result['students_num'] = 0

        self.success_response(result)


@route('/discussion/chapter_discussion_detail')
class CourseChapterDiscussionDetail(BaseHandler):
    def get(self):
        query = self.es_query(index='main', doc_type='discussion') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=self.chapter_id)[:0]
        query.aggs.metric('value', "terms", field="item_id")
        data = self.es_execute(query)
        aggs = data.aggregations
        buckets = aggs['value']['buckets']
        result = []
        for bucket in buckets:
            d = {}
            d["item_id"] = bucket["key"]
            d["count"] = bucket["doc_count"]
            result.append(d)

        self.success_response({"data": result})


@route('/discussion/assistant_activity')
class CourseAssistantActivity(BaseHandler):
    def get(self):
        default_date = date_to_str(utc_to_cst(datetime.utcnow() - timedelta(days=1)))
        start = self.get_argument('start', default_date)
        end = self.get_argument('end', default_date)

        query = self.es_query(index='api1', doc_type='ta_result') \
                .filter('term', course_id=self.course_id) \
                .filter('range', **{'date': {'gte': start, 'lte': end}})

        data = self.es_execute(query[:1000])
        result = []
        for item in data.hits:
            result.append({
                'course_id': item.course_id,
                'date': item.date,
                'assistant_id': item.author_id,
                'assistant_type': item.user_type,
                'reply_num': item.day,
                'total_reply_num': item.total
            })

        self.success_response({'result': sorted(result, key=lambda x: x['date'])})
