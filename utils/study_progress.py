#!/usr/bin/env python
# coding:utf-8

'''
see http://confluence.xuetangx.com/pages/viewpage.action?pageId=12684210
'''

import sys
import hashlib
import time
import json
import happybase

'''
以下视频相关数据针对全平台（包括移动端和Web端）

一、对于某用户加入的某个课程：
1、该课程已上传的视频总数/视频总时长（分钟数） = query in mysql 247
2、该课程已看的视频数量/视频时长（分钟数）     = get_study_progress(user_id, course_id)
3、该课程未看的视频数量/视频时长（分钟数）     = 1 - 2
4、每一章已上传的视频总数/视频总时长（分钟数） = query in mysql 247
5、每一章已看的视频数量/视频时长（分钟数）     = get_study_progress(user_id, course_id, items)
6、每一章未看的视频数量/视频时长（分钟数）     = 4 - 5
7、该课程最后一次观看的视频与观看进度          = get_video_last_pos(user_id, course_id)
8、该课程/视频的视频覆盖率                   = 2 / 1

二、对于某用户：
1、在所有端最后一次观看的课程视频与观看进度    = get_user_last_pos(user_id)
'''

class Counter:
    def __init__(self, initial=0):
        self.counter = initial
    def incr(self, amount=1):
        self.counter += amount


class StudyProgress:
    def __init__(self, thrift_server, thrift_port=9090, namespace='test'):
        self.thrift_server, self.thrift_port = thrift_server, thrift_port
        self.namespace = namespace
        self.connection = happybase.Connection(host=self.thrift_server, port=self.thrift_port)

    def md5_bytes(self, value, num_bytes):
        return hashlib.md5(value).hexdigest()[:num_bytes]

    def get_table_names(self):
        '''
        see http://confluence.xuetangx.com/pages/viewpage.action?pageId=11813527
        '''
        tables = []

        table_names_t = self.connection.table('%s:table_names' % self.namespace)
        for rowkey, values in table_names_t.scan():
            tables.append(values['f:name'])

        reverse_sort = lambda a,b: cmp(b, a)
        tables.sort(reverse_sort)
        return tables

    def get_table(self, table_name):
        return self.connection.table(table_name)

    def get_video_progress_detail(self, user_id, course_id, video_duartions):
        """
        get user's detailed data on watched video
        return {video_id: {seg_key: seg_value}}
        """
        sn_user   = str(user_id).zfill(10)[::-1]
        sn_course = self.md5_bytes(course_id, 6)

        rowprefix = sn_user + sn_course

        accept_func = lambda x: x in video_duartions

        watched_duration = {}
        for table_name in self.get_table_names():
            table = self.get_table(table_name)

            for rowkey, d in table.scan(row_prefix=rowprefix):
                v_id = d['info:video_id']
                if not accept_func(v_id): continue
                for k in d:
                    if k.startswith('heartbeat:i'):
                        watched_duration.setdefault(v_id, {}).setdefault(k, Counter()).incr(int(d[k]))

        ret = {}
        for v_id in watched_duration:
            d = {}
            video_data = watched_duration[v_id]
            for k in video_data:
                d[k] = min(5, video_data[k].counter)

            i = 0
            merged = []
            duration = video_duartions[v_id]
            while i * 5 < duration:
                k = 'heartbeat:i%d' % i
                v = d.get(k, 0)
                if merged:
                    last = merged[-1]
                    if last['v'] == v:
                        last['end'] = i
                    else:
                        merged.append({'start':i, 'end':i, 'v':v})
                else:
                    merged.append({'start':i, 'end':i, 'v':v})

                i += 1

            ret[v_id] = merged
        return ret

    def get_user_watched_video(self, user_id):
        """
        get user watched video in all courses
        return course num and total_time
        """
        course_data = self.get_user_watched_video_by_course(user_id)
        course_num = len(course_data)
        total_time = sum(course_data.values())
        return {'watched_courses': course_num, 'watched_duration': total_time}

    def get_user_watched_video_by_course(self, user_id):
        """
        lookup user video study progress in all courses

        Arguments:
        user_id: user's id

        Returns:
        return {course_id1: watched_duration1, course_id2: watched_duration2}
        """
        sn_user   = str(user_id).zfill(10)[::-1]
        rowprefix = sn_user

        watched_courses = set()
        watched_duration = {}
        table_names = self.get_table_names()

        for table_name in table_names:
            table = self.get_table(table_name)

            for rowkey, d in table.scan(row_prefix=rowprefix):
                # print d
                # print rowkey, d
                v_id = d['info:video_id']
                c_id = d['info:course_id']
                if not (c_id and v_id):
                    continue
                if '/' not in c_id and ':' not in c_id:
                    continue
                for k in d:
                    if k.startswith('heartbeat:i'):
                        key = v_id + ':' + k
                        watched_duration.setdefault(c_id, {}).setdefault(key, Counter()).incr(int(d[k]))

        course_watched_duration = {}
        for course_id in watched_duration:
            course_data = watched_duration[course_id]
            items_watched_duration = 0
            for key in course_data:
                items_watched_duration += min(5, course_data[key].counter)
            course_watched_duration[course_id] = items_watched_duration

        return course_watched_duration


    def get_study_progress(self, user_id, course_id, items=None):
        """
        lookup user video study progress in a course
        items: if items is None, get study progress in this course
               if items is a list of item ids, get study progress of these items

        Arguments:
        user_id: user's id
        course_id: course's id
        items: video item IDs

        Returns:
        return watched_num, watched_duration
        """
        sn_user   = str(user_id).zfill(10)[::-1]
        sn_course = self.md5_bytes(course_id, 6)
        rowprefix = sn_user + sn_course

        if items:
            item_set = set(items)
            accept_func = lambda x: x in item_set
        else:
            accept_func = lambda x: True

        watched_videos = set()
        watched_duration = {}
        table_names = self.get_table_names()

        for table_name in table_names:
            table = self.get_table(table_name)

            for rowkey, d in table.scan(row_prefix=rowprefix):
                # print d
                # print rowkey, d
                v_id = d['info:video_id']
                if not accept_func(v_id):
                    continue
                print rowkey, d
                watched_videos.add(v_id)
                for k in d:
                    if k.startswith('heartbeat:i'):
                        key = v_id + ':' + k
                        watched_duration.setdefault(key, Counter()).incr(int(d[k]))

        items_watched_duration = 0
        for key in watched_duration:
            items_watched_duration += min(5, watched_duration[key].counter)

        return len(watched_videos), items_watched_duration

    def get_study_progress_not_class_level(self, user_id, course_id, items={}):
        """
        lookup user video study progress in a course
        items: if items is None, get study progress in this course
               if items is a dict of {item: item_group}, get study progress of these item_groups

        Arguments:
        user_id: user's id
        course_id: course's id
        items: item and item_groups

        Returns:
        return {item_group: {'watched_num':0, 'watched_duration':0}}
        """
        sn_user   = str(user_id).zfill(10)[::-1]
        sn_course = self.md5_bytes(course_id, 6)
        rowprefix = sn_user + sn_course

        watched_videos = {}
        watched_duration = {}
        table_names = self.get_table_names()

        for table_name in table_names:
            table = self.get_table(table_name)

            for rowkey, d in table.scan(row_prefix=rowprefix):
                # print d
                # print rowkey, d
                v_id = d['info:video_id']
                if v_id not in items:
                    continue
                item_group = items[v_id]
                watched_videos.setdefault(item_group, set()).add(v_id)
                for k in d:
                    if k.startswith('heartbeat:i'):
                        key = v_id + ':' + k
                        watched_duration.setdefault(item_group, {}).setdefault(key, Counter()).incr(int(d[k]))

        result = {}
        for item_group in watched_videos:
            items_watched_duration = 0
            for key in watched_duration[item_group]:
                items_watched_duration += min(5, watched_duration[item_group][key].counter)
            result[item_group] = {'watched_num': len(watched_videos[item_group]),
                                  'watched_duration': items_watched_duration}
        return result

    def get_video_last_pos(self, user_id, course_id, item_id):
        """ 获取一个学生在某个视频上最后观看到的位置 
            Returns: cur_pos 
                     or None if this user has not watched this video
        """
        tables = self.get_table_names()
        sn_user   = str(user_id).zfill(10)[::-1]
        sn_course = self.md5_bytes(course_id, 6)
        sn_item   = self.md5_bytes(item_id, 8)
        rowkey = ''.join([sn_user, sn_course, sn_item])
        for table_name in tables:
            t = self.get_table(table_name)
            values = t.row(rowkey, ['info:current_point'])
            if values:
                return values['info:current_point']

    def get_course_last_pos(self, user_id, course_id):
        """ 获取一个学生在一门课上的最后观看的视频和位置
        Returns: course_id, item_id, cur_pos
        """
        table_name = '%s:user_last_video' % self.namespace
        table = self.get_table(table_name)
        sn_user   = str(user_id).zfill(10)[::-1]
        sn_course = self.md5_bytes(course_id, 6)
        rowkey = sn_user + sn_course
        row = table.row(rowkey)
        if row:
            return course_id, row['f:item_id'], float(row['f:cur_pos'])

    def get_user_last_pos(self, user_id):
        """ 获取一个学生在所有课上的最后观看的视频和位置
        Returns: course_id, item_id, cur_pos
        """
        table_name = '%s:user_last_video' % self.namespace
        table = self.get_table(table_name)
        sn_user   = str(user_id).zfill(10)[::-1]

        ret = {}
        last_update = 0
        for rowkey, d in table.scan(row_prefix=sn_user):
            if 'f:course_id' not in d or 'f:last_update' not in d:
                continue
            if int(d['f:last_update']) > last_update:
                last_update = int(d['f:last_update'])
                ret = d

        if ret:
            return ret['f:course_id'], ret['f:item_id'], float(ret['f:cur_pos'])

    def _get_total_time(self, row_prefix, value_mapper):
        """ 一门课观看的总时长，做法是把学生在本课程所有视频每个小段的数据相加
            考虑到覆盖时长和累积时长不同，使用不同的value_mapper来表示
            如果是覆盖时长，每个小段的数据最大是5秒，即 value_mapper = lambda v: min(5, int(v))
            如果是累积时长，每个小段的数据就是本身的值，即 value_mapper = lambda v: int(v)， 简单写value_mapper = int
        """
        tables = self.get_table_names()
        total_time = 0
        for table_name in tables:
            t = self.get_table(table_name)
            for rowkey, d in t.scan(row_prefix=row_prefix):
                total_time += sum([value_mapper(v) for k, v in d.iteritems() if k.startswith('heartbeat:')])
        return total_time

    def close(self):
        self.connection.close()


from contextlib import contextmanager
@contextmanager
def study_progress(thrift_server, thrift_port):
    sp = StudyProgress(thrift_server=thrift_server,
        thrift_port=thrift_port, namespace='heartbeat')
    try:
        yield sp
    finally:
        sp.close()


if __name__ == '__main__':
    host = sys.argv[1]
    action = sys.argv[2]
    user_id = sys.argv[3]
    course_id = sys.argv[4]

    with study_progress(thrift_server=host, thrift_port=9090) as sp:
        print 'get_study_progress'
        print sp.get_study_progress(user_id, course_id)

        print 'get_course_last_pos'
        print sp.get_course_last_pos(user_id, course_id)

        print 'get_user_last_pos'
        print sp.get_user_last_pos(user_id)

