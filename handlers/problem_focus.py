#! -*- coding: utf-8 -*-
from .base import BaseHandler
from utils.routes import route
from utils.log import Log
import settings
from elasticsearch_dsl import Q

Log.create('problem_focus')


class ProblemFocus(BaseHandler):

    def query_video_seeking_event(self, event_type):
        
        user_ids = self.get_users()
        query = self.es_query(index='problems_focused',doc_type='video_seeking_event')\
                                .filter('term', course_id=self.course_id)\
                                .filter('term', event_type=event_type)\
                                .filter('terms', user_id=user_ids)

        query.aggs.metric('num', 'cardinality',field='user_id')
        
        query.aggs.bucket('user_ids', 'terms', field='user_id', size=len(user_ids))\
                  .metric('num', 'cardinality',field='video_id')
        
        return query

    def get_video_seek(self, event_type):

        #拖拽漏看人数，人均拖拽漏看视频数
        result = self.es_execute(self.query_video_seeking_event(event_type))
        aggs = result.aggregations
        seek_persons = aggs.num.value
        
        buckets = aggs.user_ids.buckets
        seek_video_avg = 0
        for bucket in buckets:
            seek_video_avg += bucket.num.value

        seek_video_avg = round(float(seek_video_avg)/seek_persons, 2) if seek_persons != 0 else 0
        return seek_persons, seek_video_avg

    def get_video_not_watch(self, event_type):

        #未看完人数，人均未看玩视频数和,有观看行为
        result = self.es_execute(self.query_video_seeking_event(event_type))
        aggs = result.aggregations
        not_watch_total = aggs.num.value    
        buckets = aggs.user_ids.buckets
        not_watch_avg = 0
        for bucket in buckets:
            not_watch_avg += bucket.num.value

        return not_watch_total, not_watch_avg

    @property
    def get_video_seeking_event_user_ids(self):
        
        #查拖拽漏看表中所有的user_id
        user_ids = self.get_users()
        query = self.es_query(index='problems_focused',doc_type='video_seeking_event')\
                    .filter('term', course_id=self.course_id)\
                    .filter('terms', user_id=user_ids)
        query.aggs.bucket('user_ids', 'terms', field='user_id', size=len(user_ids))
        result_seek_user_ids = self.es_execute(query)
        aggs = result_seek_user_ids.aggregations
        buckets = aggs.user_ids.buckets
        seek_user_ids = []
        for bucket in buckets:
            seek_user_ids.append(bucket.key)

        return seek_user_ids

    @property
    def get_video_seek_avg(self):
        
        result = self.es_execute(self.query_video_seeking_event('seek_video'))
        aggs = result.aggregations
        buckets = aggs.user_ids.buckets
        seek_avg = {}
        for bucket in buckets:
            seek_avg[bucket.key] = bucket.num.value
        return seek_avg

    @property
    def get_video_rate(self):
        query = self.es_query(doc_type='study_video')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .filter('range', **{'study_rate': {'gte': 0.9}})
        query.aggs.bucket('user_ids', 'terms', field='user_id', size=len(self.get_users()))
        result = self.es_execute(query)
        aggs = result.aggregations.user_ids.buckets
        total = len(aggs)
        return total
            

@route('/problem_focus/seek_video_overview')
class SeekVideoOverview(ProblemFocus):

    def get(self):

        #课程选课人数
        enroll_num = len(self.get_users())
        
        #查出本门课选课状态中的所有user_id
        user_ids = self.get_users()

        #course_video_open,课程视频发布数
        open_num = self.course_open_num

        #seek_video拖拽漏看视频人数,无拖拽漏看视频数，人均拖拽漏看视频数，对比
        seek_persons, seek_video_avg = self.get_video_seek('seek_video')
        seek_persons_percent = round(float(seek_persons)/enroll_num, 4) if enroll_num != 0 else 0
        
        #无拖拽漏看人数（观看比例大于等于90%的人数）
        not_seek_persons = self.get_video_rate

        not_seek_persons_percent = round(float(not_seek_persons)/enroll_num, 4) if enroll_num != 0 else 0
        seek_video_avg_percent = round(float(seek_video_avg)/open_num, 4) if open_num != 0 else 0

        #not_watch有观看行为
        not_watch_total, not_watch_avg = self.get_video_not_watch('not_watch')

        #查拖拽漏看表中所有的user_id
        seek_user_ids = self.get_video_seeking_event_user_ids
        
        #总的未观看人数
        not_watch_total = len(user_ids) - len(seek_user_ids) + not_watch_total
        #没有观看行为的人
        not_watch_action_persons = [user_id for user_id in user_ids if user_id not in seek_user_ids]
        not_watch_avg = not_watch_avg + len(not_watch_action_persons) * open_num
        not_watch_avg = round(float(not_watch_avg)/not_watch_total, 2) if not_watch_total != 0 else 0

        #人均未看完视频数/已发布视频数
        not_watch_avg_percent = round(float(not_watch_avg)/open_num, 4) if open_num !=0  else 0

        result = {
            'seek_persons': seek_persons,
            'seek_persons_percent': seek_persons_percent,
            'not_seek_persons': not_seek_persons,
            'not_seek_persons_percent': not_seek_persons_percent,
            'seek_video_avg': seek_video_avg,
            'seek_video_avg_percent': seek_video_avg_percent,
            'not_watch_avg': not_watch_avg,
            'not_watch_avg_percent': not_watch_avg_percent
        }
        self.success_response({'data': result})

@route('/problem_focus/seek_video_study')
class SeekVideoStudy(ProblemFocus):

    def get(self):

        seek_avg = self.get_video_seek_avg
        user_ids = self.get_users()
        result = {
            '0': 0,
            '1-10': 0,
            '11-20': 0,
            '21-30': 0,
            '31-40': 0,
            '41-50': 0,
            '51-60': 0,
            '61-70': 0,
            '71-80': 0,
            '81-90': 0,
            '91-100': 0,
            '>100': 0
        }
        for k, v in seek_avg.items():
            for key in result.keys():
                value = key.split('-')
                if len(value) > 1:
                    if v >= int(value[0]) and v <= int(value[1]):
                        result[key] +=1
                if v == 0:
                     result['0'] +=1
                if v >100:
                     result['>100'] +=1
            if k in user_ids:
                user_ids.remove(k)
        result['0'] += len(user_ids)


        self.success_response({'data': result})

@route('/problem_focus/personal_study')
class PersonalStudy(BaseHandler):

    def get(self):
   
        #用户在课程级别拖拽漏看视频数量,未观看视频数量，课程发布视频数量
        query= self.es_query(index='problems_focused',doc_type='video_seek_summary')\
                   .filter('term', course_id=self.course_id)\
                   .filter('term', user_id=self.user_id)
        
        result = self.es_execute(query[:1]).hits[0]
        course_seek_video = result.seek_total
        course_not_watch = result.not_watch_total
        course_open_num = result.video_open_num
        _ut = result._ut.replace('T', ' ').split('.')[0]
    
        #课程级别学习比例
        query = self.es_query(doc_type='video_course')\
                    .filter('term', user_id=self.user_id)\
                    .filter('term', course_id=self.course_id)

        total = self.es_execute(query).hits.total
        course_study_rate = self.es_execute(query[:total]).hits
        course_study_rate = round(course_study_rate[0].study_rate, 4) if len(course_study_rate) != 0 else 0
        
        #课程学习比例为0/不为0
        result_data = {
                'course_open_num': course_open_num,
                'course_seek_video': course_seek_video,
                'course_not_watch': course_not_watch,
                'chapter_study_list': [],
                '_ut': _ut
        }
        result_data['status'] = 1 if course_study_rate >= 0.9 else 0
        if course_study_rate == 0:
            result_data['chapter_study_list'].extend([{'chapter_id': chapter['chapter_id'], 'open_num': chapter['open_num'], 'seek_video':0, 'not_watch':chapter['open_num'], 'status': 0, 'study_rate': 1} for chapter in self.chapter_open_num])
        else:    
            #章级别学习比例
            query = self.es_query(doc_type='video_chapter')\
                        .filter('term', course_id=self.course_id)\
                        .filter('term', user_id=self.user_id)
            query.aggs.bucket('chapter_ids', 'terms', field='chapter_id', size=1000)\
                      .metric('num', 'avg', field='study_rate')

            result = self.es_execute(query)
            aggs = result.aggregations

            chapters_study_rate = {}
            for chapter in [{'chapter_id':bucket.key, 'study_rate': bucket.num.value}for bucket in aggs.chapter_ids.buckets]:
                chapters_study_rate[chapter['chapter_id']] = chapter['study_rate']
            
            chapters_open_num = {}
            for chapter in self.chapter_open_num:
                chapters_open_num[chapter['chapter_id']] = chapter['open_num']

            for chapter_id, open_num  in chapters_open_num.items():
                if chapter_id not in chapters_study_rate:
                    result_data['chapter_study_list'].extend([{'chapter_id': chapter_id, 'open_num': open_num, 'seek_video':0, 'not_watch': open_num, 'study_rate':1, 'status': 0}])

                else:
                    #章级别有学习比例的视频数
                    query = self.es_query(doc_type='study_video')\
                                .filter('term', course_id=self.course_id)\
                                .filter('term', user_id=self.user_id)\
                                .filter('term', chapter_id=chapter_id)\
                                .filter('term', group_key=self.group_key)
                    total = self.es_execute(query).hits.total

                    #用户在章级别拖拽漏看视频数量
                    query = self.es_query(index='problems_focused',doc_type='video_seeking_event')\
                                 .filter('term', course_id=self.course_id)\
                                 .filter('term', user_id=self.user_id)\
                                 .filter('term', event_type='seek_video')\
                                 .filter('term', chapter_id=chapter_id)
                    query.aggs.metric('num', 'cardinality',field='video_id')
            
                    result = self.es_execute(query)
                    aggs = result.aggregations
                    seek_total = aggs.num.value

                    #用户在章级别未观看视频数量
                    query = self.es_query(index='problems_focused',doc_type='video_seeking_event')\
                                 .filter('term', course_id=self.course_id)\
                                 .filter('term', user_id=self.user_id)\
                                 .filter('term', event_type='not_watch')\
                                 .filter('term', chapter_id=chapter_id)
                    query.aggs.metric('num', 'cardinality', field='video_id')
                    result = self.es_execute(query)
                    aggs = result.aggregations
                    not_watch_total = aggs.num.value

                    not_watch_total += open_num - total
                    study_rate = round(chapters_study_rate[chapter_id], 4)
                    status = 1 if study_rate >= 0.9 else 0
                    study_rate = round(1-study_rate, 4) if study_rate < 0.9 else study_rate
                    chapter_seek_not_watch = [{'chapter_id': chapter_id, 'open_num': open_num, 'seek_video': seek_total, 'not_watch': not_watch_total, 'study_rate': study_rate, 'status':status}]
                     
                    result_data['chapter_study_list'].extend(chapter_seek_not_watch)

        
        course_study_rate = round(1-course_study_rate, 4) if course_study_rate < 0.9 else course_study_rate
        result_data['course_study_rate'] = course_study_rate

        self.success_response({'data': result_data})

@route('/problem_focus/study_chapter')
class StudyChapter(BaseHandler):

    def get(self):

        #节级别视频发布数量
        seq_open_num = self.seq_open_num
        #节级别有学习比例的视频数
        query = self.es_query(doc_type='study_video')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', chapter_id=self.chapter_id)\
                    .filter('term', user_id=self.user_id)
        query.aggs.bucket('seq_ids', 'terms', field='seq_id',size=1000)
        seq_study = self.es_execute(query)
        buckets = seq_study.aggregations.seq_ids.buckets

        seq_study_total = [{'seq_id': bucket.key, 'total': bucket.doc_count} for bucket in buckets]

        #节级别拖拽漏看，未观看视频数
        query = self.es_query(index='problems_focused', doc_type='video_seeking_event')\
                        .filter('term', course_id=self.course_id)\
                        .filter('term', chapter_id=self.chapter_id)\
                        .filter('term', user_id=self.user_id)
        
        query_seq_seek_video = query.filter('term', event_type='seek_video')
        query_seq_seek_video.aggs.bucket('seq_ids', 'terms', field='seq_id', size=1000)\
                                    .metric('num', 'cardinality', field='video_id')

        query_seq_not_watch = query.filter('term', event_type='not_watch')
        query_seq_not_watch.aggs.bucket('seq_ids', 'terms', field='seq_id', size=1000)\
                                   .metric('num', 'cardinality', field='video_id')

        result_seq_seek_video = self.es_execute(query_seq_seek_video)
        seq_video_aggs = result_seq_seek_video.aggregations
        seq_seek_video = [{'seq_id':bucket.key, 'seek_video':bucket.num.value} for bucket in seq_video_aggs.seq_ids.buckets]

        result_seq_not_watch = self.es_execute(query_seq_not_watch)
        seq_watch_aggs = result_seq_not_watch.aggregations
        seq_not_watch = [{'seq_id':bucket.key, 'not_watch':bucket.num.value} for bucket in seq_watch_aggs.seq_ids.buckets]
        
        #根据user_id, course_id, group_key, chapter_id,从study_video表中查出每个视频的学习情况（视频学习比例,至于那些没有观看行为的视频放在tap层进行处理）
        query_video_rate = self.es_query(doc_type='study_video')\
                                .filter('term', course_id=self.course_id)\
                                .filter('term', chapter_id=self.chapter_id)\
                                .filter('term', user_id=self.user_id)\
                                .source(['item_id', 'study_rate'])
        
        #观看比例大于等于90%的视频id以及观看比例                        
        query_more = query_video_rate.filter('range', **{'study_rate': {'gte': 0.9}})
        more_total = self.es_execute(query_more).hits.total
        rate_more = self.es_execute(query_more[:more_total]).hits
        result_rate_more = []
        if len(rate_more) != 0:
            result_rate_more.extend([{'video_id':hit.item_id, 'study_rate': round(float(hit.study_rate),4)} for hit in rate_more])

        #观看比例小于90%的视频id以及未观看比例
        query_less = query_video_rate.filter('range', **{'study_rate': {'lt': 0.9}})
        less_total = self.es_execute(query_less).hits.total
        rate_less = self.es_execute(query_less[:less_total]).hits
        rate_less_id = []
        result_video_rate_less = []
        if len(rate_less) != 0:
            rate_less_id.extend([ hit.item_id for hit in rate_less])
            result_video_rate_less.extend([ {'video_id': hit.item_id, 'study_rate': round(float(hit.study_rate), 4)}for hit in rate_less])

        #判断是否有观看比例小于90%的视频（只判断有观看行为的）
        seq_seek_video_action = []
        if len(rate_less_id) != 0:
            query = self.es_query(index='problems_focused',doc_type='video_seeking_event')\
                                       .filter('term', course_id=self.course_id)\
                                       .filter('term', chapter_id=self.chapter_id)\
                                       .filter('term', user_id=self.user_id)\
                                       .filter('terms', video_id=rate_less_id)\
                                       .source(settings.SEEK_FIELD)
            total = self.es_execute(query).hits.total
            result_seek = self.es_execute(query[:total])
            #视频拖拽漏看记录
            seq_seek_video_action = [hit.to_dict()for hit in result_seek.hits]
        
        result = {
            'seq_open_num': seq_open_num,
            'seq_seek_video': seq_seek_video,
            'seq_study_total': seq_study_total,
            'seq_not_watch': seq_not_watch,
            'result_video_rate_more': result_rate_more,
            'seq_seek_video_action': seq_seek_video_action,
            'result_video_rate_less': result_video_rate_less

        }

        self.success_response({'data': result})

@route('/problem_focus/school_info')
class SchoolInfo(BaseHandler):

    def get(self):
        
        #查用户的学校
        query_org = self.es_query(doc_type='course_student_location')\
                    .filter('term', uid=self.user_id)\
                    .source(['binding_org'])
        total = self.es_execute(query_org).hits.total
        result = self.es_execute(query_org[:total]).hits
        binding_org = result[0].binding_org if len(result) != 0 else ''
        #查用户的姓名，学号，院系， 专业
        query_person_info = self.es_query(doc_type='student_enrollment_info')\
                                .filter('term', user_id=self.user_id)\
                                .source(['rname', 'binding_uid', 'faculty', 'major', '_ut'])
    
        total = self.es_execute(query_person_info).hits.total
        result = self.es_execute(query_person_info[:total]).hits
        result = [hit.to_dict()for hit in result]
        result[0]['binding_org'] = binding_org

        self.success_response({'data': result[0]})

@route('/problem_focus/study_warning_overview')
class StudyWarningOverview(BaseHandler):

    def get(self):
        
        #enroll_num
        user_ids = self.get_users()
        enroll_num = len(user_ids)

        field = ['warning_num', 'least_2_week_num', 'low_video_rate_num', 'low_grade_rate_num', 'warning_date']
        
        query = self.es_query(index='problems_focused', doc_type='study_warning')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .source(field)\
                    .sort('-_ut')
        result = self.es_execute(query)
        data = [hit.to_dict() for hit in result.hits[:2]] 
        self.success_response({'data': data, 'enroll_num': enroll_num})


@route('/problem_focus/study_warning_chart')
class StudyWarningChart(BaseHandler):

    def get(self):
        query = self.es_query(index='problems_focused', doc_type='study_warning_person')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .sort('_ut')
        query.aggs.bucket('study_weeks', 'terms', field='study_week', size=1000)
        result = self.es_execute(query)
        aggs = result.aggregations
        buckets = aggs.study_weeks.buckets
        data = [{'study_week': bucket.key, 'warning_num': bucket.doc_count}for bucket in buckets]
        self.success_response({'data': data})
