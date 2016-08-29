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
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('terms', user_id=users)\
                .filter('term', course_id=self.course_id)[:0]
        query.aggs.bucket('groups', 'terms', field='group_id', size=0) \
                .metric('posts_total', 'sum', field='post_num') \
                .metric('comments_total', 'sum', field='reply_num') #\
                # .metric('group_num', 'terms', field='group_member_num', size=1, order={'_term': 'desc'})

        data = self.es_execute(query)
        posts_total = 0
        comments_total = 0
        group_dict = {}
        for group in data.aggregations.groups.buckets:
            try:
                member_count = group.doc_count
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
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion_daily') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=users)\
                .filter('range', **{'date': {'gte': start, 'lte': end}})
        # data = self.es_execute(query[:0])
        # data = self.es_execute(query[:data.hits.total])
        query.aggs.bucket('date', 'terms', field='date').bucket('groups', 'terms', field='group_id') \
            .metric('post_num', 'sum', field='post_num').metric('reply_num', 'sum', field='reply_num')
        data = self.es_execute(query)
        # date_list = [date_to_str(start + timedelta(days=d)) for d in xrange(0, days)]
        # date_result = {d: {'date': d, 'num': 0} for d in date_list}
        # # print len(data.hits)
        # for item in data.aggregations.date.buckets:
        #     date_result.update({item.key_as_string[:10]: {
        #         'date': item.key_as_string[:10],
        #         'num': int(item.num.value)
        #     }})
        date_list = [date_to_str(start + timedelta(days=d)) for d in xrange(0, days)]
        date_result = {d: {} for d in date_list}
        for item in data.aggregations.date.buckets:
            for j in item.groups.buckets:
                date_result[item.key_as_string[:10]][j.key] = {
                    'date': item.key_as_string[:10],
                    'group_id': j.key,
                    'post_number': j.post_num.value,
                    'comment_number': j.reply_num.value
                }

        self.success_response({'date': date_result})


@route('/discussion/chapter_stat')
class ChapterDiscussion(BaseHandler):
    """
    获取给定章的讨论信息统计
    """
    def get(self):
        chapter_id = self.get_argument('chapter_id')
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('terms', user_id=users)[:0]
        query.aggs.metric('sequentials', 'terms', field='seq_id', size=0)

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

        query = self.es_query(index='tap', doc_type='discussion') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=chapter_id) \
                .filter('terms', user_id=students)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])
        chapter_student_stat = {}
        for item in data.hits:
            sequential_id = item.seq_id
            student_id = item.user_id
            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id].setdefault(student_id, [])
            student_discussion_item = {
                'item_id': item.item_id,
                'post_num': item.post_num,
                'reply_num': item.reply_num,
                'time': getattr(item, "la_access", "2016-01-01")
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
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion_daily') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=users)\
                .filter('range', **{'date': {'gte': start, 'lte': end}})[:size]
        query.aggs.bucket('date', 'terms', field='date')\
            .metric('num', 'sum', field='noreply_num')
        data = self.es_execute(query)
        date_list = [date_to_str(start + timedelta(days=d)) for d in xrange(0, size)]
        date_result = {d: {'date': d, 'num': 0} for d in date_list}
        # print len(data.hits)
        for item in data.aggregations.date.buckets:
            date_result.update({item.key_as_string[:10]: {
                'date': item.key_as_string[:10],
                'num': int(item.num.value)
            }})

        # date_list = [date_to_str(start + timedelta(days=d)) for d in xrange(0, size)]
        # date_result = {d: {'date': d, 'num': 0} for d in date_list}
        # for item in data.hits:
        #     date_result.update({item.date: {
        #         'date': item.date,
        #         'num': item.daily_num
        #     }})

        self.success_response({'date': sorted(date_result.values(), key=lambda x: x['date'])})


@route('/discussion/no_comment_posts')
class CoursePostsNoComment(BaseHandler):
    """
    获取没有评论的总帖子数
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=users)

        data = self.es_execute(query)
        # query.aggs.bucket().metric('noreply_total', 'sum', field='noreply_num')
        count = 0
        for i in data.hits:
            if i.noreply_num != None:
                count = count + i.noreply_num
        # try:
        #     count = data[0]['all_num']
        # except (KeyError, IndexError):
        #     count = 0

        self.success_response({'count': count})


@route('/discussion/students_top_stat')
class StudentPostTopStat(BaseHandler):
    """
    获取学生发帖TOP-N统计
    """
    def get(self):
        top = self.get_argument('top', 5)
        order = self.get_argument('order', 'post')
        users = self.get_users()
        order_field = 'comments_total' if order == 'comment' else 'posts_total'

        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('terms', user_id=users)\
                .filter('term', course_id=self.course_id)[:0]
        query.aggs.bucket('students', 'terms', field='user_id', size=top, order={order_field: 'desc'}) \
                .metric('posts_total', 'sum', field='post_num') \
                .metric('comments_total', 'sum', field='reply_num')
        data = self.es_execute(query)

        students = {}
        usernames = self.get_user_name()
        for item in data.aggregations.students.buckets:
            students[item.key] = {
                'user_id': int(item.key),
                'posts_total': int(item.posts_total.value),
                'comments_total': int(item.comments_total.value),
                "user_name": usernames.get(int(item.key), "")
            }

        query = self.es_query(index='tap', doc_type='discussion_daily') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=students.keys())
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        for item in data.hits:
            students[item.user_id].setdefault('detail', {})
            students[item.user_id]['detail'][item.date] = {
                    'post_number': item.post_num,
                    'comment_number': item.reply_num,
                    'date': item.date
                }
            # TODO
            # students[item.user_id].setdefault('user_name', item.user_name)
            students[item.user_id].setdefault('group_id', item.group_id)
            students[item.user_id].setdefault('user_id', int(item.user_id))

        students_list = sorted(students.values(), key=lambda x: x['posts_total'], reverse=True)

        self.success_response({'students': students_list})


@route('/discussion/students_detail')
class StudentDetail(BaseHandler):
    """
    获取所有参与讨论学生统计
    """
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion_aggs') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=users)\
                .query(Q('range', **{'post_num': {'gt': 0}}) | Q('range', **{'reply_num': {'gt': 0}}))
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])
        usernames = self.get_user_name()
        grades = self.get_grade()
        students_detail = {}
        for item in data.hits:
            students_detail[item.user_id] = {
                'user_id': int(item.user_id),
                'post_number': item.post_num,
                'comment_number': item.reply_num,
                'grade_percent': grades.get(str(item.user_id), 0),
                'group_id': item.group_id,
                'user_name': usernames.get(int(item.user_id), "")
            }

        self.success_response({'students': students_detail})


@route('/discussion/students_relation')
class StudentRelation(BaseHandler):
    def get(self):

        query = self.es_query(index='tap', doc_type='discussion_relation') \
                .filter('term', course_id=self.course_id)
        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])
        users = self.get_users()
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
        # 获得course_list的group_id
        result = {}
        query = self.es_query(index='tap', doc_type='course')\
                .filter('range', status={'gte': 0})
        if self.group_key:
            query = query.filter('term', group_key=self.group_key)
        #else:
        #    query = query.filter(~F('exists', field='group_id'))
        hits = self.es_execute(query[:0]).hits
        if hits.total > 0:
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
        reply_dict = {}
        # 第一个是帖子数，第二个是零回复数
        for hit in hits:
            course_id, post_num, noreply_num = hit.course_id, int(hit.post_num), int(hit.noreply_num) if hit.noreply_num else 0
            if course_id not in reply_dict:
                reply_dict[course_id] = [0,0]
            reply_dict[course_id][0] += int(post_num)
            if noreply_num:
                reply_dict[course_id][1] += int(noreply_num)
        sorted_dict = sorted(reply_dict.items(), 
                key=lambda x: (x[1][0]-x[1][1])/float(x[1][0]) if float(x[1][0]) != 0 else 0,
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
        enroll_dict = self.get_enroll(group_key=self.group_key)
        total_comment = defaultdict(float)
        for hit in hits:
            course_id, post_num, reply_num = hit.course_id, int(hit.post_num), int(hit.reply_num)
            total_comment[course_id] += int(post_num) / float(enroll_dict[course_id])
            total_comment[course_id] += int(reply_num) / float(enroll_dict[course_id])
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
        discussion_student_list = {}
        for hit in hits:
            course_id,user_id = hit.course_id, hit.user_id
            if course_id not in discussion_student_list:
                discussion_student_list[course_id] = set()
            if user_id not in discussion_student_list[course_id]:
                discussion_student_list[course_id].add(user_id)
        sorted_dict = sorted(discussion_student_list.items(),
                key=lambda x: len(x[1]),
                reverse=True)
        discussion_num = 0
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
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion') \
                .filter('term', course_id=self.course_id) \
                .filter('term', chapter_id=self.chapter_id) \
                .filter('terms', user_id=users)
                # .sort('-date')[:1]
        data = self.es_execute(query)
        hit = data.hits
        result['course_id'] = self.course_id
        result['chapter_id'] = self.chapter_id
        # if hit:
        #     result['students_num'] = int(hit[0].num)
        # else:
        result['students_num'] = len(hit)

        self.success_response(result)


@route('/discussion/chapter_discussion_detail')
class CourseChapterDiscussionDetail(BaseHandler):
    def get(self):
        users = self.get_users()
        query = self.es_query(index='tap', doc_type='discussion') \
                .filter('term', course_id=self.course_id) \
                .filter('terms', user_id=users)\
                .filter('term', chapter_id=self.chapter_id)[:0]
        query.aggs.bucket('value', "terms", field="item_id")
        query.aggs.bucket('seq_value', "terms", field="seq_id")\
                .metric('num', 'terms', field='user_id', size=0)
        data = self.es_execute(query)
        aggs = data.aggregations
        buckets = aggs['value']['buckets']
        result = []
        for bucket in buckets:
            d = {}
            d["item_id"] = bucket["key"]
            d["count"] = bucket["doc_count"]
            result.append(d)
        # seq级别发言人次
        seq_buckets = aggs['seq_value']['buckets']
        seq_result = {}
        for bucket in seq_buckets:
            seq_id = bucket["key"]
            num = len(bucket["num"]["buckets"])
            seq_result[seq_id] = num
        self.success_response({"item_result": result, "seq_result": seq_result})


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
