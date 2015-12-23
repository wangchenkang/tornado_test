#! -*- coding: utf-8 -*-
from datetime import timedelta
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
from utils.tools import date_from_query, date_to_str
from elasticsearch_dsl import Search

Log.create('discussion')

@route('/discussion/course_stat')
class CourseDiscussion(BaseHandler):
    """
    获取给定课程讨论区发帖情况统计信息
    包括总体和各group的发帖和回复数统计
    """
    def get(self):
        course_id = self.course_id
        query =  {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                            ]
                        }
                    }
                }
            },
            'aggs': {
                'groups': {
                    'terms': {
                        'field': 'group_id',
                        'size': 0
                    },
                    'aggs': {
                        'posts_total': {
                            'sum': {
                                'field': 'post_number'
                            }
                        },
                        'comments_total': {
                            'sum': {
                                'field': 'comment_number'
                            }
                        },
                        'group_num': {
                            'terms': {
                                'field': 'group_member_num',
                                'size': 1,
                                'order': {
                                    '_term': 'desc'
                                }
                            }
                        }
                    }
                }
            },
            'size': 0
        }

        data = self.es_search(index='main', doc_type='group_daily', search_type='count', body=query)
        posts_total = 0
        comments_total = 0
        group_dict = {}
        for group in data['aggregations']['groups']['buckets']:
            try:
                member_count = int(group['group_num']['buckets'][0]['key'])
                group_posts_total = int(group['posts_total']['value'])
                group_comments_total = int(group['comments_total']['value'])
            except (KeyError, IndexError):
                member_count = 0
            group_dict[group['key']] = {
                'posts_total': group_posts_total,
                'comments_total': group_comments_total,
                'members_total': member_count
            }
            posts_total += group_posts_total
            comments_total += group_comments_total

        result = {
            'total': data['hits']['total'],
            'posts_total': posts_total,
            'comments_total': comments_total,
            'groups': group_dict
        }

        self.success_response(result)


@route('/discussion/course_daily_stat')
class CourseDailyStat(BaseHandler):
    def get(self):
        course_id = self.course_id
        start = self.get_param('start')
        end = self.get_param('end')

        start = date_from_query(start)
        end = date_from_query(end)
        days = (end - start).days + 1

        query = Search(using=self.es, index='main', doc_type='group_daily') \
                .filter('term', course_id=course_id) \
                .filter('range', **{'d_day': {'gte': start, 'lte': end}})
        default_size = 100000
        data = query[:default_size].execute()
        if data.hits.total > default_size:
            data = query[:data.hits.total].execute()

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
        course_id = self.get_argument('course_id')
        chapter_id = self.get_argument('chapter_id')
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'exists': {'field': 'uid'}}
                            ]
                        }
                    }
                }
            },
            'aggs': {
                'sequentials': {
                    'terms': {
                        'field': 'sequential_id',
                        'size': 0
                    }
                }
            },
            'size': 0
        }

        data = self.es_search(index='main', doc_type='discussion', search_type='count', body=query)
        discussion_stat = {
            'total': data['hits']['total'],
            'sequentials': {}
        }
        
        for item in data['aggregations']['sequentials']['buckets']:
            discussion_stat['sequentials'][item['key']] = {
                'student_num': item['doc_count']
            }

        self.success_response(discussion_stat)


@route('/discussion/chapter_student_stat')
class ChapterStudentDiscussion(BaseHandler):
    """
    获取给定章的课程讨论统计(所有学生)
    """
    def get(self):
        course_id = self.course_id
        chapter_id = self.chapter_id
        uid = self.get_param('user_id')
        students = [u.strip() for u in uid.split(',') if u.strip()]

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'term': {'chapter_id': chapter_id}},
                                {'terms': {'uid': students}},
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }

        data = self.es_search(index='main', doc_type='discussion', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['total']
            data = self.es_search(index='main', doc_type='discussion', body=query)

        chapter_student_stat = {}
        for item in data['hits']['hits']:
            sequential_id = item['_source'] ['sequential_id']
            student_id = item['_source']['uid']
            chapter_student_stat.setdefault(sequential_id, {}) 
            chapter_student_stat[sequential_id].setdefault(student_id, [])
            student_discussion_item = {
                'item_id': item['_source']['item_id'],
                'post_num': item['_source']['post_num'],
                'reply_num': item['_source']['reply_num'],
                'time': item['_source']['time'],
            }
            chapter_student_stat[sequential_id][student_id].append(student_discussion_item)

        result = {
            'total': data['hits']['total'],
            'sequentials': chapter_student_stat
        }

        self.success_response(result)


@route('/discussion/no_comment_posts_daily')
class CoursePostsNoCommentDaily(BaseHandler):
    """
    获取给定date范围的无评论帖子数
    """
    def get(self):
        course_id = self.course_id
        start = self.get_param('start')
        end = self.get_param('end')

        start = date_from_query(start)
        end = date_from_query(end)
        size = (end - start).days + 1

        query = Search(using=self.es, index='main', doc_type='discuss_no_reply_daily') \
                .filter('term', course_id=course_id) \
                .filter('range', **{'date': {'gte': start, 'lte': end}})[:size]
        data = query.execute()

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
        course_id = self.course_id
        
        query = Search(using=self.es, index='main', doc_type='discuss_no_reply_all') \
                .filter('term', course_id=self.course_id)
        data = query.execute()

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
        course_id = self.course_id
        top = self.get_argument('top', 5)
        order = self.get_argument('order', 'post')

        order_field = 'comments_total' if order == 'comment' else 'posts_total'

        top_query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                            ]
                        }
                    }
                }
            },
            'aggs': {
                'students': {
                    'terms': {
                        'field': 'user_id',
                        'size': top,
                        'order': {
                            order_field: 'desc'
                        }
                    },
                    'aggs': {
                        'posts_total': {
                            'sum': {
                                'field': 'post_number'
                            }
                        },
                        'comments_total': {
                            'sum': {
                                'field': 'comment_number'
                            }
                        }
                    }
                }
            },
            'size': 0
        }
        data = self.es_search(index='main', doc_type='user_daily', search_type='count', body=top_query)

        students = {}
        for item in data['aggregations']['students']['buckets']:
            students[item['key']] = {
                'user_id': int(item['key']),
                'posts_total': int(item['posts_total']['value']),
                'comments_total': int(item['comments_total']['value'])
            }

        default_size = 100000
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}},
                                {'terms': {'user_id': students.keys()}}
                            ]
                        }
                    }
                }
            },
            'size': default_size
        }
        data = self.es_search(index='main', doc_type='user_daily', body=query)
        if data['hits']['total'] > default_size:
            query['size'] = data['hits']['data']
            data = self.es_search(index='main', doc_type='user_daily', body=query)

        for item in data['hits']['hits']:
            students[item['_source']['user_id']].setdefault('detail', {})
            students[item['_source']['user_id']]['detail'][item['_source']['d_day']] = {
                    'post_number': item['_source']['post_number'],
                    'comment_number': item['_source']['comment_number'],
                    'date': item['_source']['d_day']
                }

            students[item['_source']['user_id']].setdefault('user_name', item['_source']['user_name'])
            students[item['_source']['user_id']].setdefault('group_id', item['_source']['group_id'])
            students[item['_source']['user_id']].setdefault('user_id', int(item['_source']['group_id']))

        students_list = sorted(students.values(), key=lambda x: x['posts_total'], reverse=True)

        self.success_response({'students': students_list})


@route('/discussion/students_detail')
class StudentDetail(BaseHandler):
    """
    获取所有参与讨论学生统计
    """
    def get(self):
        course_id = self.course_id
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}}
                            ],
                            'should': [
                                {'range': {'post_number': {'gt': 0}}},
                                {'range': {'comment_number': {'gt': 0}}}
                            ]
                        }
                    }
                }
            },
            'size': 0
        }

        data = self.es_search(index='main', doc_type='user_sum', search_type='count', body=query)
        total = data['hits']['total']

        query['size'] = total
        data = self.es_search(index='main', doc_type='user_sum', body=query)

        students_detail = {}
        for item in data['hits']['hits']:
            students_detail[item['_source']['user_id']] = {
                'user_id': int(item['_source']['user_id']),
                'post_number': item['_source']['post_number'],
                'comment_number': item['_source']['comment_number'],
                'grade_percent': item['_source']['grade_percent'],
                'group_id': item['_source']['group_id'],
                'user_name': item['_source']['user_name']
            }

        self.success_response({'students': students_detail})


@route('/discussion/students_relation')
class StudentRelation(BaseHandler):
    def get(self):
        course_id = self.course_id
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {'term': {'course_id': course_id}}
                            ]
                        }
                    }
                }
            },
            'size': 0
        }

        data = self.es_search(index='main', doc_type='comment_num', search_type='count', body=query)
        total = data['hits']['total']

        query['size'] = total
        data = self.es_search(index='main', doc_type='comment_num', body=query)

        relations = []
        for item in data['hits']['hits']:
            relations.append({
                'user_id1': item['_source']['user_id1'],
                'user_id2': item['_source']['user_id2'],
                'num': item['_source']['num']
            })

        self.success_response({'relations': relations})


@route('/discussion/course_rank_stat')
class CourseRankStat(BaseHandler):
    def get(self):

        result = {}
        query = Search(using=self.es, index='rollup', doc_type='discuss_active_user_num') \
                .filter('term', course_id=self.course_id)
        active_data = query.execute()

        query = Search(using=self.es, index='rollup', doc_type='discuss_average_active_num') \
                .filter('term', course_id=self.course_id)
        average_data = query.execute()

        query = Search(using=self.es, index='rollup', doc_type='discuss_replied_percent') \
                .filter('term', course_id=self.course_id)
        reply_data = query.execute()

        result['course_num'] = 0
        try:
            active_data = active_data[0]
            result['course_num'] = active_data['owner_course_num']
            result['active_stat'] = {
                'rank': active_data['rank'],
                'user_num': active_data['active_user_num']
            }
        except IndexError:
            result['active_stat'] = {
                'rank': 0,
                'user_num': 0
            }

        try:
            average_data = average_data[0]
            result['course_num'] = average_data['owner_course_num']
            result['average_stat'] = {
                'total_num': average_data['active_num'],
                'post_num': average_data['post_num'],
                'reply_num': average_data['reply_num'],
                'rank': average_data['rank'],
                'user_average_num': average_data['user_average_active_num']
            }
        except IndexError:
            result['average_stat'] = {
                'total_num': 0,
                'post_num': 0,
                'reply_num': 0,
                'rank': 0,
                'user_average_num': 0
            }

        try:
            reply_data = reply_data[0]
            result['course_num'] = reply_data['owner_course_num']
            result['reply_stat'] = {
                'no_reply_post_num': reply_data['no_reply_num'],
                'reply_post_num': reply_data['replied_post_num'],
                'reply_percent': round(reply_data['replied_percent'], 4),
                'rank': reply_data['rank']
            }
        except IndexError:
            result['reply_stat'] = {
                'no_reply_post_num': 0,
                'reply_post_num': 0,
                'reply_percent': 0,
                'rank': 0
            }

        self.success_response(result)
