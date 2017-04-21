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

        query = self.es_query(doc_type='study_video_rollup') \
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
        video_query = self.es_query(doc_type='study_video') \
                .filter('term', course_id=course_id) \
                .filter('term', group_key=self.group_key) \
                .filter('term', chapter_id=chapter_id) \
                .filter('terms', user_id=students)

        video_data = self.es_execute(video_query[:0])
        video_data = self.es_execute(video_query[:video_data.hits.total])

        chapter_student_stat = {}
        for item in video_data.hits:
            sequential_id = item.seq_id
            student_id = item.user_id
            chapter_student_stat.setdefault(sequential_id, {})
            chapter_student_stat[sequential_id].setdefault(student_id, [])

            chapter_student_stat[sequential_id][str(student_id)].append({
                'video_id': item.item_id,
                'review_rate': item.review_rate,
                'watch_num': item.watch_num,
                'duration': item.duration,
                'study_rate': item.study_rate,
                'la_access': getattr(item, "la_access", None)
            })

        result = {
            'sequentials': chapter_student_stat,
        }

        self.success_response(result)


@route('/video/course_study_rate')
class CourseVideo(BaseHandler):
    """
    spoc 接口需求：获取课程用户视频观看覆盖率
    """
    def get(self):
        user_id = self.get_argument('user_id', None)

        query = self.es_query(doc_type='video_course') \
                .filter('term', course_id=self.course_id)
        
        if user_id is not None:
            max_length = 1000
            user_id = [int(u.strip()) for u in user_id.split(',') if u.strip()][0:max_length]
            query = query.filter('terms', user_id=user_id)
            data = self.es_execute(query[:max_length])
        else:
            data = self.es_execute(query[:0])
            data = self.es_execute(query[:data.hits.total])

        result = []
        users_has_study = set()
        for item in data.hits:
            # fix uid: anonymous_id-anonymous_id-e03be04b3e8a30b74b9779ace15e1b50
            try:
                item_uid = int(item.user_id)
            except ValueError:
                continue
            result.append({
                'user_id': item_uid,
                'user_sec': round(item.user_sec,4),
                'study_rate': round(float(item.study_rate),4)
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
        users = self.get_users()

        query = self.es_query(doc_type='study_video') \
                .filter('term', course_id=course_id) \
                .filter('exists', field='duration') \
                .filter('terms', user_id=users)

        max_length = 1000
        if user_id is not None:
            user_id = [u.strip() for u in user_id.split(',') if u.strip()][0:max_length]
            query = query.filter('terms', uid=user_id)

        data = self.es_execute(query[:0])
        data = self.es_execute(query[:data.hits.total])

        results = []
        for item in data.hits:
            try:
                item_uid = int(item.user_id)
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
                'study_rate': float(item.study_rate),
                'la_access': item.la_access
            })

        self.success_response({'total': data.hits.total, 'data': results})


@route('/video/chapter_video_stat')
class ChapterVideoStat(BaseHandler):

    def get(self):
        result = {}
        query = self.es_query(doc_type='study_video_rollup') \
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
        result = {}
        users = self.get_users()
        # TODO
        # 将main该为tap，uid改为user_id
        query = self.es_query(doc_type='study_video')\
                .filter('term', course_id=self.course_id)\
                .filter('term', chapter_id=self.chapter_id)\
                .filter('term', group_key=self.group_key)\
                .filter('terms', user_id=users)\
                .filter('exists', field='watch_num')[:0]
        query.aggs.bucket("video_watch", "terms", field="item_id", size=1000)\
                .metric('num', 'range', field="watch_num", ranges=[{"from": 1, "to": 2}, {"from": 2}])
        query.aggs.bucket("video_seq_watch", "terms", field="seq_id", size=1000)\
                  .metric('num', 'cardinality', field="user_id")
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
            result[vid] = {
                "vid": vid,
                "num1": num1,
                "num2": num2
                }
        seq_buckets = aggs["video_seq_watch"]["buckets"]
        seq_result = {}
        for bucket in seq_buckets:
            seq_result[bucket['key']] = bucket.num.value
        self.success_response({"vid_result": result, "seq_result": seq_result})


@route('/video/seek_video_overview')
class SeekVideoOverview(BaseHandler):

    def get(self):
        
        #拖拽漏看视频数需要去重
        #course_enroll_num
        query_enroll_num = self.es_query(doc_type='course_community')\
                                .filter('term', course_id=self.course_id)\
                                .filter('term', group_key=self.group_key)
        total = self.es_execute(query_enroll_num).hits.total
        result_enroll_num = self.es_execute(query_enroll_num[:total]).hits
        enroll_num = 1
        if len(result_enroll_num) != 0:
            enroll_num = result_enroll_num[0].enroll_num
        #course_video_open_num
        query_open_num = self.es_query(doc_type='course_video_open_num')\
                                .filter('term', course_id=self.course_id)\
                                .filter('term', group_key=self.group_key)
        total = self.es_execute(query_open_num).hits.total
        result_open_num = self.es_execute(query_open_num[:total]).hits
        open_num = 0
        if len(result_open_num) != 0:
            open_num = result_open_num[0].video_open_num
        #seek_video
        #TODO
        query_seek_video = self.es_query(doc_type='video_seeking_event')\
                                .filter('term', course_id=self.course_id)\
                                .filter('term', group_key=self.group_key)\
                                .filter('term', event_type='seek_video')
        query_seek_video.aggs.bucket('video_ids', 'terms', field='video_id')\
                            .metric('num', 'cardinality',field='user_id' )
        #TODO
        query_seek_video.aggs.bucket('user_ids', 'terms', field='user_id')\
                                .metric('num', 'cardinality',field='video_id')
        result_seek_video = self.es_execute(query_seek_video)
        aggs = result_seek_video.aggregations
        buckets_total = aggs['video_ids']['buckets']
        buckets_avg = aggs['user_ids']['buckets']
        seek_video_total = 0
        seek_video_avg = 0
        for bucket in buckets_total:
            seek_video_total += bucket.num.value
        for bucket in buckets_avg:
            seek_video_avg += bucket.num.value

        seek_video_total_percent = seek_video_total/enroll_num
        seek_video_avg = seek_video_avg/seek_video_total
        seek_video_avg_percent = seek_video_avg/open_num if open_num != 0 else 0
        #not_watch
        query_not_watch = self.es_query(doc_type='video_seeking_event')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .filetr('term', event_type='not_watch')
        query_not_watch.aggs.bucket('not_watch_avg', 'avg', field='event_type')
        result_not_watch = self.es_execute(query_not_watch)
        aggs = result_not_watch.aggregations
        not_watch_avg = aggs['not_watch_avg']['value']
        not_watch_avg_percent = not_watch_avg/open_num if open_num != 0 else 0
        result = {
            'seek_video_total': seek_video_total,
            'seek_video_total_percent': seek_video_total_percent,
            'not_seek_video_total': enroll_num-seek_video_total,
            'not_seek_video_total_percent': 1-seek_video_total_percent,
            'seek_video_avg': seek_video_avg,
            'seek_video_avg_percent': seek_video_avg_percent,
            'not_watch_avg': not_watch_avg,
            'not_watch_avg_percent': not_watch_avg_percent
        }
        self.success_response({'data': result})

@route('/video/seek_video_study')
class SeekVideoStudy(BaseHandler):

    def get(self):
        query = self.es_query(doc_type='video_seeking_event')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .filter('term', event_type='seek_video')
        #需要考虑size的大小，user_id过多会不会影响
        query.aggs.bucket('user_ids','terms', field='user_id')\
                    .metric('num', 'cardinality', field='video_id')
        result_video_study = self.es_execute(query)
        aggs = result_video_study.aggregations
        buckets = aggs['user_ids']['buckets']
        temp_result = {}
        for bucket in buckets:
            result[bucket.key] = bucket.num.value
        #temp_list1 = ['0', '1-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90', '91-100', '100+']
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
        for k, v in temp_result.items():
            if v == 0:
                result['0'] +=1
            elif v >= 1 and v<= 10:
                result['1-10'] +=1
            elif v >= 11 and v <= 20:
                result['11-20'] +=1
            elif v >= 21 and v <= 30:
                result['21-30'] +=1
            elif v >= 31 and v<= 40:
                result['31-40'] +=1
            elif v >= 41 and v <= 50:
                result['41-50'] +=1
            elif v >= 51 and v <= 60:
                result['51-60'] +=1
            elif v >= 61 and v <= 70:
                result['61-70'] +=1
            elif v >= 71 and v <= 80:
                result['71-80'] +=1
            elif v >= 81 and v<= 90:
                result['81-90'] +=1
            elif v >= 91 and v <= 100:
                result['91-100'] +=1
            elif v >100:
                result['>100'] +=1
        self.success_response({'data': result})

@route('/video/seek_video_table')
class SeekVideoTable(BaseHandler):

    def get(self):
        
        #search tapgrade/grade_overview
        #排序
        field = ['nickname', 'xid', 'user_id', 'grade', 'total_grade_rate']
        if self.group_key != settings.MOOC_GROUP_KEY:
            field.extend(['rname', 'binding_uid', 'faculty', 'major'])
        query_grade = self.es_query(index='tapgrade', doc_type='grade_overview')\
                            .filter('term',course_id=self.course_id)\
                            .filter('term',group_key=self.group_key)\
                            .source(field)\
                            .sort('-grade')
        total = self.es_execute(query_grade).hits.total
        result_grade = self.es_execute(query_grade[:total]).hits
        result_grade = [hit.to_dict for hit in result_grade]
        
        #课程已发布视频数
        query = self.es_query(doc_type='video_seeking_event')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)
        query_course_video_open_num = self.es_query(doc_type='course_video_open_num')\
                                            .filter('term', course_id=self.course_id)
        total = self.es_execute(query_course_video_open_num).hits.total
        result_course_video_open_num = self.es_execute(query_course_video_open_num[:total]).hits
        open_num = 0
        if len(result_course_video_open_num) != 0:
            open_num = result_course_video_open_num[0].video_open_num
        
        #课程拖拽漏看视频数
        query_seek_video = query.filter('term', event_type='seek_video')
        query_seek_video.aggs.bucket('user_ids', 'terms', field='user_id')\
                            .metric('num', 'cardinality',field='video_id' )
                            
        result_seek_video = self.es_execute(query_seek_video)
        aggs = result_seek_video.aggregations
        buckets_total = aggs['user_ids']['buckets']
        user_seek_video = {}
        for bucket in buckets_total:
            user_seek_video[bucket.key]= bucket.num.value

        #not_watch
        query_not_watch = query.filter('term', event_type='not_watch')
        query_not_watch.aggs.bucket('user_ids', 'terms', field='user_id')\
                            .metric('num', 'cardinality', field='video_id')
        result_not_watch = self.es_execute(query_not_watch)
        aggs = result_not_watch.aggregations
        not_watch = aggs['user_ids']['buckets']
        user_not_watch = {}
        for bucket in not_watch:
            user_not_watch[bucket.key] = bucket.num.value
        
        #聚合数据
        for grade in result_grade:
            grade['is_seek_video'] = 0
            if grade['user_id'] in user_seek_video:
                grade['is_seek_video'] = 1
                grade['seek_video'] = user_seek_video[grade['user_id']]
            grade['is_not_watch'] = 0
            if grade['user_id'] in user_not_watch:
                grade['is_not_watch'] =1
                grade['not_watch'] = user_not_watch[grade['user_id']]
            grade['course_open_num'] = open_num
        pass

@route('/video/personal_study')
class PersonalStudy(BaseHandler):

    def get(self):
        #获取用户头像,tap做获取用户姓名，学堂号， 课程名， 课程id
        #课程发布总视频数量
        course_open_num = self.course_open_num
        #章发布视频总数量
        chapter_open_num = self.chapter_open_num
        #用户在课程级别拖拽漏看视频的数量
        query = self.es_query(doc_type='video_seeking_event')\
                    .filter('term', course_id=self.course_id)\
                    .filter('term', group_key=self.group_key)\
                    .filter('term', user_id=self.user_id)
        query_course_seek_video = query.filter('term', event_type='seek_video')\
                                        .aggs.metric('num', 'cardinality', field='item_id')
        result_course_seek_video_total = self.es_execute(query_course_seek_video)
        aggs = result_course_seek_video_total.aggregations
        course_seek_video_total = aggs.num.value
        #用户在课程级别未观看视频的数量
        query_course_not_watch = query.filter('term', event_type='not_watch')\
                                        .aggs.metric('num', 'cardinality', field='item_id')
        result_course_not_watch = self.es_execute(query_course_not_watch)
        aggs = result_course_not_watch.aggregations
        course_not_watch = aggs.num.value

        #课程级别视频未观看比例
        course_not_watch_percent = course_not_watch/open_num if open_num !=0 else 0

        #用户在章级别拖拽漏看视频数量
        query_chapter_seek_video_total = query.filter('term', event_type='seek_video')\
                                                .aggs.bucket('chapter_ids', 'terms', field='chapter_id')\
                                                .metric('num', 'cardinality',field='video_id')
        result_chapter_seek_video_total = self.es_execute(query_chapter_seek_video_total)
        aggs = result_chapter_seek_video_total.aggregations
        buckets = aggs.chapter_ids.buckets
        chapter_seek_video_total = [{bucket.key:bucket.num.value}for bucket in buckets]
        #用户在章级别未观看视频数量
        query_chapter_not_watch = query.filter('term', event_type='not_watch')\
                                        .aggs.bucket('chapter_ids', 'terms', field='chapter_id')\
                                        .metric('num', 'cardinality', field='video_id')
        result_chapter_not_watch = self.es_execute(query_chapter_not_watch)
        aggs = result_chapter_not_watch.aggregations
        buckets = aggs.chapter_ids.buckets
        chapter_not_watch = [{bucket.key: bucket.num.value } for bucket in buckets]
        #章级别未观看比例
        chapter_not_watch_percent = chapter_not_watch/chapter_open_num if chapter_open_num !=0 else 0
        
        result = {
            'course_open_num': course_open_num,
            'course_seek_video_total': course_seek_video_total,
            'course_not_watch': cours_not_watch,
            'course_not_watch_percent': course_not_watch_percent,
            'chapter_seek_video_total': chapter_seek_video_total,
            'chapter_not_watch': chapter_not_watch,
            'chapter_not_watch_percent': chapter_not_watch_percent
        }
        pass

@route('video/personal_study_chapter')
class PersonalStudyChapter(BaseHandler):

    def get(self):
        #点击章然后显示以下视频信息

        #节级别视频发布数量
        seq_open_num = self.seq_open_num

        #根据user_id, course_id, group_key, chapter_id,从study_video表中查出每个视频的学习情况（视频学习比例,至于那些没有观看行为的视频放在tap层进行处理）
        query_video_rate = self.es_query(doc_type='study_video')\
                                .filter('term', course_id=self.course_id)\
                                .filter('term', group_key=self.group_key)\
                                .filter('term', chapter_id=self.chapter_id)\
                                .filter('term', user_id=self.user_id)\
                                .source(['item_id', 'study_rate'])
        
        #观看比例大于等于90%的视频id以及观看比例                        
        query_more = query_video_rate.filter('range', **{'study_rate': {'gte': 0.09}})
        more_total = self.es_execute(query_more).hits.total
        rate_more = self.es_execute(query_more[:more_total]).hits
        result_rate_more = []
        if len(rate_more) != 0:
            result_rate_more.extend([{hit.item_id:hit.study_rate} for hit in rate_more])

        #观看比例小于90%的视频id以及未观看比例
        query_less = query_video_rate.filter('range', **{'study_rate': {'lt': 0.09}})
        less_total = self.es_execute(query_less).hits.total
        rate_less = self.es_execute(query_less[:less_total]).hits
        rate_less_id = []
        #必须有观看行为，未观看比例
        result_rate_less = []
        if len(rate_less) != 0:
            video_less_id.extend([ hit.item_id for hit in rate_less])
            result_rate_less.extend({hit.item_id: round(1-hit.study_rate, 4)} for hit in rate_less)

        #判断是否有观看比例小于90%的视频（只判断有观看行为的）
        seq_seek_video = []
        seq_seek_video_action = []
        seq_not_watch = = []
        seq_not_watch_percent = []
        seq_not_watch_action = []
        if len(rate_less_id) != 0:
            query = self.es_query(doc_type='video_seeking_event')\
                        .filter('term', course_id=self.course_id)\
                        .filter('term', group_key=self.group_key)\
                        .filter('term', chapter_id=self.chapter_id)\
                        .filter('terms', video_id=rate_less_id)\
                        .filter('term', user_id=self.user_id)
        
            query_seq_seek_video = query.filter('term', event_type='seek_video')\
                                        .aggs.bucket('seq_ids', 'terms', field='seg_id')\
                                        .metric('num', 'cardinality', field='video_id')

            query_seq_not_watch = query.filter('term', event_type='not_watch')\
                                       .aggs.bucket('seq_ids', 'terms', field='seq_id')\
                                       .metric('num', 'cardinality', field='video_id')
            #节级别拖拽漏看视频数量
            result_seq_seek_video = self.es_execute(query_seq_seek_video)
            seq_video_aggs = result_seq_seek_video.aggregations
            seq_seek_video = [{bucket.key: bucket.num.value} for bucket in seq_video_aggs.seq_ids.buckets]
            #视频拖拽漏看记录
            seq_seek_video_action = [{'video_id':hit.video_id,'event_time': hit.event_time, 'platform': hit.platform, 'video_st': hit.video_st, 'video_et': hit.video_et, 'duration': hit.duration, 'percent': hit.percent} for hit in result_seq_seek_video.hits]
            #节级别未看视频数量
            result_seq_not_watch = self.es_execute(query_seq_not_watch)
            seq_watch_aggs = result_seq_not_watch.aggregations
            seq_not_watch = [{bucket.key: bucket.num.value} for bucket in seq_watch_aggs.seq_ids.buckets]
            #节级别未观看视频比例
            seq_not_watch_percent = seq_not_watch/seq_open_num if seq_open_num !=0 else 0
            #视频未观看记录
            seq_not_watch_action = [{'video_id': hit.video_id, 'event_time': hit.event_time, 'platform': hit.platform, 'video_last': hit.video_last, 'duration': hit.duration, 'percent': hit.percent}for hit in result_seq_not_watch.hits]
    
        result = {
            'seq_open_num': seq_open_num,
            'seq_seek_video': seq_seek_video,
            'seq_not_watch': seq_not_watch,
            'seq_not_watch_percent': seq_not_watch_percent,
            'result_video_rate_more': result_video_rate_more,
            'seq_seek_video_action': seq_seek_video_action,
            'seq_not_watch_action': seq_not_watch_action,
            'seq_not_watch_percent': result__rate_less

        }
        self.success_response({'data': result})
