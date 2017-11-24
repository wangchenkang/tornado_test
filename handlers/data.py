#! -*- coding: utf-8 -*-
import os
import sys
import hashlib
import json
import codecs
import requests
import tarfile
import shutil
import tempfile
import xlsxwriter
import csv
from collections import defaultdict
from datetime import datetime
from cStringIO import StringIO
from dateutil.relativedelta import relativedelta
from tornado.escape import url_unescape
from tornado.web import HTTPError, Finish
from elasticsearch import NotFoundError
from .base import BaseHandler
from utils.routes import route
from utils.tools import fix_course_id, datedelta
from utils import mysql_connect
import settings

reload(sys)
sys.setdefaultencoding('utf-8')


download_data_type = { 
    'study_progress': u'学习进度数据',
    'video_study_export': u'视频观看记录',
    'problem_info': u'单习题得分情况',
    'unenroll_info': u'退课记录',
    'sort_info': u'单习题单次简答题情况',
    'comments_info': u'单次帖子发帖回帖情况',
    'pdf_view': u'教材浏览记录',
    'learn_info': u'课程访问记录',
    'html_view': u'其它页面浏览记录',
    'enrollment_export': u'选课记录',
    'learn_cnt_info': u'课程访问次数',
    'about_enroll': u'选课名单',
    'learn_active_export': u'学习习惯',
    'grade_distribution_export': u'得分分布',
    'course_active_export': u'学习活跃数据',
    'ta_activity_export': u'助教考核数据',
    'comment_active_export': u'讨论活跃数据',
    'grade': u'个人总成绩',
    'final_score': u'期末结课数据'
}


@route('/data/types')
class DataTypes(BaseHandler):
    def get(self):
        self.success_response({'data_type': download_data_type})

@route('/data/export')
class DataExport(BaseHandler):
    """
    获取导出文件记录
    """
    def get(self):
        course_id = self.course_id
        data_type = self.get_param('data_type')

        data_type = [item.strip() for item in data_type.split(',') if item.strip()]

        data = {}
        for item in data_type:
            data_id = hashlib.md5(course_id + item).hexdigest()
            try:
                record = self.es.get(index='download', doc_type='course_data', id=data_id)
                data[item] = {
                    'id': record['_id'],
                    'course_id': record['_source']['course_id'],
                    'data_type': record['_source']['data_type'],
                    'name': record['_source']['name'],
                    'data_link': record['_source']['data_link'],
                    'update_time': record['_source']['update_time'],
                    'zip_format': record['_source'].get('zip_format', None),
                }
            except NotFoundError:
                data[item] = {}

        self.success_response(data)


@route('/data/export/binding_org')
class DataBindingOrg(BaseHandler):
    """
    获取学堂选修课导出文件
    """
    def get(self):
        course_id = self.get_argument('course_id', None)
        org = self.get_argument('org', None)
        data_type = self.get_param('data_type')

        data_type = [item.strip() for item in data_type.split(',') if item.strip()]

        default_size = 0
        query = self.es_query(index='download', doc_type='course_data')\
                .filter('exists', field='binding_org')\
                .filter('terms', data_type=data_type)
        
        if course_id is not None:
            course_id = [fix_course_id(item.strip()) for item in course_id.split(',') if item.strip()]
            query = query.filter('terms', course_id=course_id)

        if org is not None:
            query = query.filter('term', binding_org=url_unescape(org))
        
        data = self.es_execute(query[:0])
        size = data.hits.total
        data = self.es_execute(query[:size])

        courses = {}
        for item in data.hits:
            courses.setdefault(item.data_type, {})
            courses[item['data_type']].setdefault(item['course_id'], []).append({
                'id': item.meta.id,
                'name': item['name'],
                'data_type': item['data_type'],
                'data_link': item['data_link'],
                'course_id': item['course_id'],
                'update_time': item['update_time'],
                'binding_org': getattr(item, 'binding_org', None),
                'zip_format': getattr(item, 'zip_format', None),
            })

        result = dict.fromkeys(data_type, {})
        result.update(courses)

        self.success_response(result)

@route('/data/moocap/student')
class MOOCAPStudent(BaseHandler):
    def get(self):
        user_id = self.get_param('user_id')
        query = self.es_query(index="moocap", doc_type="student_sum")\
                .filter("term", user_id=user_id)
        hits = self.es_execute(query[:10]).hits
        if hits.total == 0:
            self.success_response({"course_list": []})
        if hits.total > 10:
            hits = self.es_execute(query[:hits.total]).hits
        self.success_response({"course_list": [hit.course_id for hit in hits]})
        


@route('/data/moocap')
class DataMOOCAP(BaseHandler):
    """
    获取MOOCAP数据
    API: http://confluence.xuetangx.com/pages/viewpage.action?pageId=10486183
    """
    def get(self):
        user_id = self.get_param('user_id')
        query = self.es_query(index="moocap", doc_type="student_sum")\
                .filter("term", course_id=self.course_id)\
                .filter("term", user_id=user_id)
        result = self.es_execute(query)
        hits = result.hits
        if hits.total == 0:
            self.error_response(201, u'没有查到该学生数据')
            return
        user_info = result.to_dict()["hits"]["hits"][0]["_source"]
        # user_info
        # course_id, user_id, habbit, grade, ref, summary
        query = self.es_query(index="moocap", doc_type="course_sum")\
                .filter("term", course_id=self.course_id)
        result = self.es_execute(query)
        course_info = result.to_dict()["hits"]["hits"][0]["_source"]
        # course_info
        # course_id, habbit, grade, ref
        def fix_percent(dic):
            percents = dic["percents"]
            fix_percent = [int(percent) for percent in percents]
            sum_percent = sum(fix_percent)
            delta = sum_percent - 100
            fix_percent[-1] -= delta
            dic["percents"] = fix_percent
        all_type = ["SJ", "TR", "JL", "XL", "JZ"]
        for u_type in all_type:
            user_info["habbit"][u_type].update(course_info["habbit"][u_type])
            fix_percent(user_info["habbit"][u_type])
        user_info["ref"]["effect"].update(course_info["ref"]["effect"])
        user_info["ref"]["speed"].update(course_info["ref"]["speed"])
        user_info["grade"]["result"].update(course_info["grade"]["result"])
        user_info["grade"]["exam"].update(course_info["grade"]["exam"])
        user_info["grade"]["homework"].update(course_info["grade"]["homework"])
#        user_info["summary"].update(course_info["summary"])
        def get_context(dic, key, avg):
            if dic[avg] == 0:
                return "与均值持平"
            val = int(round((dic[key] - dic[avg]) / float(dic[avg]) * 100))
            if key == "attempts":
                val = -val
            if val > 0:
                return "高于均值{}%".format(val)
            elif val < 0:
                return "低于均值{}%".format(-val)
            else:
                return "与均值持平"
        # 时间偏好
        def get_percent(dic):
            for i, k in enumerate(dic["keys"]):
                if k == dic["value"]:
                    return "%.0f"%float(dic["percents"][i])
        sj = user_info["habbit"]["SJ"]
        if user_info["habbit"]["TR"]["category"] == u"无法判定":
            sj["category"] = u"无法判定"
        if sj["category"] == u"无法判定":
            desc = "<p><strong>本学生学习行为不足，无法进行类型判定。</strong></p>"
        elif sj["category"] == u"突击型":
            st = sj["num_date"]
            et = datedelta(st, sj["num_days"])
            desc = "<p><strong>本学生的时间偏好为突击型，学习时间集中在{}至{}中。</strong></p>".format(st, et)
        elif sj["category"] == u"规律型":
            if sj["weekday"]:
                weeknum = {
                    0: "一",
                    1: "二",
                    2: "三",
                    3: "四",
                    4: "五",
                    5: "六",
                    6: "日"}
                param = "、".join(["周" + weeknum[item[0]] for item in sorted(sj["weekday"], key=lambda x: [0])])
                desc = "<p><strong>本学生的时间偏好为规律型，学习时间段较有规律，固定在每{}。</strong></p>".format(param)
            elif sj["hour"]:
                hours = sorted([item[0] for item in sj["hour"]])
                param = []
                curr = {}
                for hour in hours:
                    if not curr:
                        curr["start"] = hour
                        curr["end"] = hour+1
                    else:
                        if hour == curr["end"]:
                            curr["end"] += 1
                        else:
                            param.append(curr)
                            curr = {"start": hour, "end": hour+1}
                param.append(curr)
                param_content = []
                for item in param:
                    if item["start"] < 4:
                        start = "0" + str(item["start"]*3)
                    else:
                        start = str(item["start"]*3)
                    if item["end"] < 4:
                        end = "0" + str(item["end"]*3)
                    else:
                        end = str(item["end"]*3)
                    param_content.append(start + ":00-"+end+":00")

                param = "、".join(param_content)
                desc = "<p><strong>本学生的时间偏好为规律型，学习时间段较有规律，固定在每天{}时间段。</strong></p>".format(param)
        elif sj["category"] == u"散点型":
            desc = "<p><strong>本学生的时间偏好为散点型，无固定学习时间段</strong></p>"

        #fix bug        
        sj["value"] = sj["category"]
        sj.pop("category")
        user_info["habbit"]["SJ"]["desc"] = desc + "<p>本期课程中，有{}%的合格成员也属于{}。</p>".format(get_percent(sj), sj["value"])
        # 投入偏好
        tr = user_info["habbit"]["TR"]
        if tr["category"] == u"无法判定":
            desc = "<p><strong>本学生学习行为不足，无法进行类型判定</strong></p>"
        elif tr["category"] == u"均衡型":
            video_context = get_context(tr, "video_seconds", "video_avg")
            hw_context = get_context(tr, "homework_seconds", "homework_avg")
            exam_context = get_context(tr, "exam_seconds", "exam_avg")
            desc = "<p><strong>本学生的投入偏好为均衡型，各个学习模块的投入总时间较为平均。</strong></p><p>与同期学生相比，各学习模块投入时长：视频学习{}小时，{}；作业{}小时，{};测试{}小时，{}。</p>".format("%.1f"%(tr["video_seconds"]/3600.0), video_context, "%.1f"%(tr["homework_seconds"]/3600.0), hw_context, "%.1f"%(tr["exam_seconds"]/3600.0), hw_context)
        elif tr["category"] == u"倾斜型":
            video_context = get_context(tr, "video_seconds", "video_avg")
            hw_context = get_context(tr, "homework_seconds", "homework_avg")
            exam_context = get_context(tr, "exam_seconds", "exam_avg")
            conce = max((tr["video_seconds"], "视频学习"), (tr["homework_seconds"], "作业"), (tr["exam_seconds"], "测试"))
            desc = "<p><strong>本学生的投入偏好为倾斜型，学习时间主要集中于{}。</strong></p><p>与同期学生相比，各学习模块投入时长：视频学习{}小时，{}；作业{}小时，{};测试{}小时，{}。</p>".format(conce[1], "%.1f"%(tr["video_seconds"]/3600.0), video_context, "%.1f"%(tr["homework_seconds"]/3600.0), hw_context, "%.1f"%(tr["exam_seconds"]/3600.0), exam_context)
       
        #fix bug
        tr["value"] = tr["category"]
        tr.pop("category")
        user_info["habbit"]["TR"]["desc"] = desc + "<p>本期课程中，有{}%的合格成员也属于{}。</p>".format(get_percent(tr), tr["value"])

        # 交流
        jl = user_info["habbit"]["JL"]
        if jl["category"] == u"离群型":
            desc = "<p><strong>本学生的交流偏好为离群型，无任何发言和互动。</strong></p>"
        elif jl["category"] == u"积极参与型":
            desc = "<p><strong>本学生的交流偏好为积极参与型，喜欢集体学习，会主动与他人交流问题。</strong></p>"
        elif jl["category"] == u"逃避型":
            desc = "<p><strong>本学生的交流偏好为逃避型，主动提问较少，同时与他人交流较少。</strong></p>"

        #fix bug
        jl["value"] = jl["category"]
        jl.pop("category")
        user_info["habbit"]["JL"]["desc"] = desc + "<p>本期课程中，有{}%的合格成员也属于{}。</p>".format(get_percent(jl), jl["value"])
        # 序列
        xl = user_info["habbit"]["XL"]
        if xl["category"] == u"无法判定":
            desc = "<p><strong>本学生学习行为不足，无法进行类型判定</strong></p>"
        elif xl["category"] == u"序列型":
            desc = "<p><strong>本学生的序列偏好为序列型，主要按课程内容顺序学习。</strong></p>"
        elif xl["category"] == u"逃避型":
            desc = "<p><strong>本学生的交流偏好为逃避型，主动提问较少，同时与他人交流较少。</strong></p>"

        #fix bug
        xl["value"] = xl["category"]
        xl.pop("category")
        user_info["habbit"]["XL"]["desc"] = desc + "<p>本期课程中，有{}%的合格成员也属于{}。</p>".format(get_percent(xl), xl["value"])
        # 节奏
        jz = user_info["habbit"]["JZ"]
        if jz["category"] == u"无法判定":
            desc = "<p><strong>本学生学习行为不足，无法进行类型判定</strong></p>"
        elif jz["category"] == u"张弛型":
            if jz["is_12_gt_12345"]:
                param = "根据学习情况"
            else:
                param = ""
            desc = "<p><strong>本学生的节奏偏好为张弛型，在学完一个知识点后，会{}停顿以反思。</strong></p>".format(param)
        elif jz["category"] == u"紧凑型":
            desc = "<p><strong>本学生的节奏偏好为紧凑型，大部分情况下，学习完一个知识点后，会立即投入到下一个知识点的学习。</strong></p>"

        #fix bug
        jz["value"] = jz["category"]
        jz.pop("category")

        user_info["habbit"]["JZ"]["desc"] = desc + "<p>本期课程中，有{}%的合格成员也属于{}。</p>".format(get_percent(jz), jz["value"])
        # grade
        # 1, result
        
        #fix bug
        result = user_info["grade"]["result"]
        result["desc"] = "<p><strong>本学生顺利完成本课程的学习，并通过了考核。</strong></p><p>结果性成绩是基于课程最终得分线性折算的。</p>"
        r = result["range"]        
        result["range"] = range(r[0], r[1]+1)
        if result.has_key("grade_value"):
            result["value"] = result["grade_value"]
            result.pop("grade_value")

        #user_info["grade"]["result"]["desc"] = "<p><strong>本学生顺利完成本课程的学习，并通过了考核。</strong></p><p>结果性成绩是基于课程最终得分线性折算的。</p>"
        #r = user_info["grade"]["result"]["range"]
        #user_info["grade"]["result"]["range"] = range(r[0], r[1]+1)
        
        def get_rel_avg(dic):
            if "value" in dic and "avg" in dic:
                v1, v2 = int(dic["value"]), int(dic["avg"])
                if v1 >= v2:
                    return "+" + str(v1-v2) 
                else:
                    return "-" + str(v2-v1)
            else:
                return "+0"
        user_info["grade"]["result"]["rel_avg"] = get_rel_avg(user_info["grade"]["result"])
        # 2, exam
        exam = user_info["grade"]["exam"]
        #fix bug
        if exam.has_key("grade_value"):
            exam["value"] = exam["grade_value"]
            exam.pop("grade_value")
        # 2.0 exam value
        if "value" in exam and "avg" in exam:
            if exam["value"] > exam["avg"]:
                exam_val = "高于平均水平"
            elif exam["value"] < exam["avg"]:
                exam_val = "低于平均水平"
            else:
                exam_val = "与均值持平"
        else:
            exam_val = "与均值持平"
        exam_eff = get_context(exam, "effice", "effice_avg")
        exam_com = get_context(exam, "complete", "complete_avg")
        exam_time = get_context(exam, "timeline", "timeline_avg")
        user_info["grade"]["exam"]["desc"] = "<p><strong>本学生在完成测试过程中的综合表现{}。</strong></p><p>其中各子计分项情况分布为：效能[2]{}；完成率{}；及时性{}。</p>".format(exam_val, exam_eff, exam_com, exam_time)
        user_info["grade"]["exam"]["rel_avg"] = get_rel_avg(user_info["grade"]["exam"])
        r = user_info["grade"]["exam"]["range"]
        user_info["grade"]["exam"]["range"] = range(r[0], r[1]+1)

        # 3, homework
        hw = user_info["grade"]["homework"]
        #fix bug
        if hw.has_key("grade_value"):
            hw["value"] = hw["grade_value"]
            hw.pop("grade_value")
        if "value" in hw and "avg" in hw:
            if hw["value"] > hw["avg"]:
                hw_val = "高于平均水平"
            elif hw["value"] < hw["avg"]:
                hw_val = "低于平均水平"
            else:
                hw_val = "与均值持平"
        else:
            hw_val = "与均值持平"
        hw_eff = get_context(hw, "effice", "effice_avg")
        hw_com = get_context(hw, "complete", "complete_avg")
        hw_time = get_context(hw, "timeline", "timeline_avg")
        hw_atte = get_context(hw, "attempts", "attempts_avg")
        user_info["grade"]["homework"]["desc"] = "<p><strong>本学生在完成测试过程中的综合表现{}。</strong></p><p>其中各子计分项情况分布为：效能[2]{}；完成率{}；及时性{}；尝试次数{}。</p>".format(hw_val, hw_eff, hw_com, hw_time, hw_atte)
        user_info["grade"]["homework"]["rel_avg"] = get_rel_avg(user_info["grade"]["homework"])
        r = user_info["grade"]["homework"]["range"]
        user_info["grade"]["homework"]["range"] = range(r[0], r[1]+1)
        
        speed = user_info["ref"]["speed"]
        user_info["ref"]["speed"]["value"] = int(speed["origin_value"]/speed["origin_max"]*10)
        user_info["ref"]["speed"]["range"] = range(11)
        if speed["origin_value"] > speed["origin_avg"]:
            speed_val = ("高于平均水平", "较少")
        elif speed["origin_value"] < speed["origin_avg"]:
            speed_val = ("低于平均水平", "较多")
        else:
            speed_val = ("与均值持平", "与均值持平")
        user_info["ref"]["speed"]["desc"] = "<p><strong>本学生学习速度{}。在学习内容一定的情况下，学习所用时间{}。</strong></p><p>学习速度是基于课程教学时长和学生的学习时长计算的。</p>".format(*speed_val)
        effect = user_info["ref"]["effect"]
        user_info["ref"]["effect"]["value"] = int(effect["origin_value"]/effect["origin_max"]*10)
        user_info["ref"]["effect"]["range"] = range(11)
        if effect["origin_value"] > effect["origin_avg"]:
            effect_val = ("高于平均水平", "较少")
        elif effect["origin_value"] < effect["origin_avg"]:
            effect_val = ("低于平均水平", "较多")
        else:
            effect_val = ("与均值持平", "与均值持平")
        user_info["ref"]["effect"]["desc"] = "<p><strong>本学生学习效度{}。在投入时间{}的情况下，取得了较好的成绩。</strong></p><p>学习效度是基于学生的学习速度和课程最终得分计算的。</p>".format(*effect_val)

        user_info["authentication"] = "2016" + "年"
        self.success_response(user_info)




@route('/data/monthly_report')
class DataMonthlyReport(BaseHandler):
    """
    获取学堂选修课教学周报
    API: http://confluence.xuetangx.com/pages/viewpage.action?pageId=9869181
    """
    def get(self):
        school = self.get_argument('org_name', None)
        plan_name = self.get_argument('plan_name', None)
        pn = int(self.get_argument('page', 1))
        num = int(self.get_argument('psize', 10))
        data_type = "monthly_report"
        query = self.es_query(index='monthly_report', doc_type='tap_elective_course_plan_m')
        if school:
            query = query.filter('term', school=school)
        else:
            query = query.filter('exists', field="school")
        if plan_name:
            query = query.filter('term', plan_name=plan_name)
        else:
            query = query.filter('exists', field="plan_name")

        date = datetime.today()-relativedelta(months=1)
        last_month = "%s%02d"%(date.year,date.month)
        
        if school == '中国民航大学' and plan_name == '2017年春季学期':
            months = ['201703','201704', '201705', '201706', '201707', '201708']
            query = query.filter('terms', months=months)
        else:
            query = query.filter('term',months=last_month)
        size = query[:0].execute().hits.total
        data = query[:size].execute().hits

        all_coursenames = defaultdict(list)
        all_courseids = defaultdict(list)
        view_info = {}
        plan_info = {}
        update_date = {}
        update_info = {}
        #import reg
        #reg.compile('^\d+年\d月\d日$')
        for d in data:
            all_coursenames[d.school].append(d.course_name)
            #all_courseids[d.school].append(d.cours_id)
            plan_info[d.school] = d.plan_name
            update_date[d.school] = d.update_date
            view_info[d.school] = d.zip_url if hasattr(d,"zip_url") else ""
        all_info = {}
        for school in all_coursenames.iterkeys():
            #all_info["%s"%school] = {"course_name":all_coursenames.get(school,[])}
            all_info["%s"%school.encode('utf-8')] = {"plan_name":plan_info.get(school,'')}
            all_info["%s"%school.encode('utf-8')].update({"org_name":school})
            all_info["%s"%school.encode('utf-8')].update({"course_list":all_coursenames.get(school,[])})
            all_info["%s"%school.encode('utf-8')].update({"update_time":update_date.get(school)})
            all_info["%s"%school.encode('utf-8')].update({"zip_url":view_info.get(school,'')})

        size = len(all_info.keys())
        values = all_info.values()
        data = values[(pn-1)*num: pn*num]

        pages = {
                "recordsPerPage": num,
                "totalPage": (size - 1) / num + 1,
                "page": pn,
                "totalRecord": size
                }
        results = []
        for hit in data:
            results.append({
                "org_name": hit.get("org_name",""),
                "plan_name": hit.get("plan_name",""),
                "course_list": hit.get("course_list",""),
                "update_time": hit.get("update_time",""),
                "pdf_url": "",
                "zip_url": hit.get("zip_url")
                })
        self.success_response({"pages": pages, "results": results})


@route('/data/weekly_report')
class DataWeeklyReport(BaseHandler):
    """
    获取学堂选修课教学周报
    API: http://confluence.xuetangx.com/pages/viewpage.action?pageId=9869181
    """
    def get(self):
        org = self.get_argument('org_name', None)
        plan = self.get_argument('plan_name', None)
        pn = int(self.get_argument('page', 1))
        num = int(self.get_argument('psize', 10))
        data_type = "weekly_report"

        query = self.es_query(index='download', doc_type='org_data')\
                .filter('term', data_type=data_type)
        if org:
            query = query.filter('term', binding_org=org)
        else:
            query = query.filter('exists', field="binding_org")
        if plan:
            query = query.filter('term', plan=plan)
        else:
            query = query.filter('exists', field="plan")
        size = query[:0].execute().hits.total
        data = query[:size].execute().hits
        date_dic = {}
        for item in data:
            key = hashlib.md5(item.binding_org+item.plan).hexdigest()
            if key in date_dic:
                if item.update_time > date_dic[key].update_time:
                    date_dic[key] = item
            else:
                date_dic[key] = item
        data = date_dic.values()
        size = len(data)
        data.sort(key=lambda x: x.plan, reverse=True)
        data = data[(pn-1)*num: pn*num]
        pages = {
                "recordsPerPage": num,
                "totalPage": (size - 1) / num + 1,
                "page": pn,
                "totalRecord": size
                }
        results = []
        for hit in data:
            results.append({
                "org_name": hit.binding_org,
                "plan_name": hit.plan,
                "course_list": list(hit.course_name),
                "update_time": hit.update_time,
                "pdf_url": hit.pdf_url,
                "zip_url": hit.zip_url
                })

        self.success_response({"pages": pages, "results": results})




@route('/data/download')
class DataDownload(BaseHandler):
    """
    课程导出文件下载
    """
    def get_temp_dir(self):
        temp_dir = tempfile.mktemp(prefix='tapapi_', dir='/tmp')
        os.makedirs(temp_dir)
        return temp_dir

    def get(self):
        data_id = self.get_param('id')
        platform = self.get_argument('os', 'unix').lower()
        file_format = self.get_argument('format', 'xlsx').lower()

        if platform not in ['windows', 'unix']:
            platform = 'windows'

        if file_format != 'csv':
            file_format = 'xlsx'

        try:
            record = self.es.get(index='download', doc_type='course_data', id=data_id)
        except NotFoundError:
            raise HTTPError(404)

        filename = record['_source']['name']
        data_url = record['_source']['data_link']
        zip_format = record['_source'].get('zip_format', None)
        es_file_format = record['_source'].get('file_format', None)

        response = requests.get(data_url)
        if not response.content.strip():
            raise HTTPError(404)

        if zip_format != 'tar':
            if file_format == 'xlsx':
                xlsx_file = StringIO()
                workbook = xlsxwriter.Workbook(xlsx_file, {'in_memory': True})
                worksheet = workbook.add_worksheet()
                lines = csv.reader(response.content.strip().split('\n'), dialect=csv.excel)
                row = 0
                for line in lines:
                    for col, item in enumerate(line):
                        if len(item) > 0 and item[0] == '=':
                            item = "'" + item
                        worksheet.write(row, col, item)
                    row += 1
                workbook.close()
                xlsx_file.seek(0)
                filename = filename.rsplit('.', 1)[0] + '.xlsx'
                self.set_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))
                self.write(xlsx_file.read())
                xlsx_file.close()
            else:
                self.set_header('Content-Type', 'text/csv')
                self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))

                if platform == 'windows':
                    self.write(codecs.BOM_UTF8)
                self.write(response.content)

        else:
            # 如果打包文件是xlsx则直接返回给用户
            if es_file_format and es_file_format == 'xlsx':
                self.set_header('Content-Type', 'application/x-compressed-tar')
                self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))
                self.write(response.content)
                raise Finish

            try:
                temp_dir = tempfile.mktemp(prefix='tapapi_', dir='/tmp')
                os.makedirs(temp_dir)
                temp_tarfile = tempfile.mktemp(suffix='.tar.gz', dir=temp_dir)
                with open(temp_tarfile, 'wb') as fp:
                    fp.write(response.content)

                tar = tarfile.open(temp_tarfile, 'r:gz')
                tar.extractall(temp_dir)

                if file_format == 'xlsx':
                    xlsx_file = StringIO()
                    workbook = xlsxwriter.Workbook(xlsx_file, {'in_memory': True})
                    for item in tar:
                        if not item.isreg():
                            continue

                        item_filename = os.path.join(temp_dir, item.name)
                        work_sheet_name = os.path.split(item.name)[-1].rsplit('.', 1)[0]
                        worksheet = workbook.add_worksheet(work_sheet_name[:30])
                        with open(item_filename, 'rb') as fp:
                            lines = fp.read().strip().split('\n')
                            for row, line in enumerate(lines):
                                for col, item_content in enumerate(line.split(',')):
                                    worksheet.write(row, col, item_content.strip('"'))

                    workbook.close()
                    xlsx_file.seek(0)

                    filename = filename.split('.', 1)[0] + '.xlsx'
                    self.set_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))

                    self.write(xlsx_file.read())
                    xlsx_file.close()
                elif platform == 'windows':
                    win_tmp_tarfile = tempfile.mktemp(suffix='.tar.gz', dir=temp_dir)
                    win_tarfile = tarfile.open(win_tmp_tarfile, 'w')
                    for item in tar:
                        if not item.isreg():
                            continue

                        item_filename = os.path.join(temp_dir, item.name)
                        with codecs.open(item_filename, 'rb', 'utf-8') as fp:
                            item_data = fp.read()
                        with codecs.open(item_filename, 'wb', 'utf-8') as fp:
                            fp.write(codecs.BOM_UTF8)
                            fp.write(item_data)

                        win_tarfile.add(item_filename, item.name)
                    win_tarfile.close()

                    self.set_header('Content-Type', 'application/x-compressed-tar')
                    self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))

                    with open(win_tmp_tarfile, 'rb') as fp:
                        self.write(fp.read())
                else:
                    self.set_header('Content-Type', 'application/x-compressed-tar')
                    self.set_header('Content-Disposition', u'attachment;filename={}'.format(filename))
                    self.write(response.content)

            finally:
                shutil.rmtree(temp_dir)


@route('/data/student_courseenrollment')
class StudentCourseEnrollment(BaseHandler):
     """
     课程累计选课人数
     """
     def get_course_ids(self):
         course_ids = self.get_argument('course_id', None)
         if not course_ids:
             self.error_response(502, u'缺少课程id参数')
         course_ids = course_ids.split(',')
         new_course_ids = []
         for course_id in course_ids:
             course_id = fix_course_id(course_id)
             if course_id not in new_course_ids:
                new_course_ids.append(course_id)
         return new_course_ids

     def get_course_enrollments(self, course_id_pc, course_id_cp, course_enrollment):
         parent_enrollment_num = {}
         for parent, children in course_id_pc.items():
             num = 0
             for course in course_enrollment:
                 if course['course_id'] in children:
                     num += course['enrollment_num']
             parent_enrollment_num[parent] = num
         data = []
         for child, parent in course_id_cp.items():
             data_ = {}
             for parent_, enrollment_num in parent_enrollment_num.items():
                 if parent == parent_:
                     data_['course_id'] = child
                     data_['acc_enrollment_num'] = enrollment_num
                     data.append(data_)
         data.sort(lambda x,y: -cmp(x['acc_enrollment_num'], y['acc_enrollment_num']))
         return data

     def get_course_2_ids(self, course_ids, sign, hits=[]):
         """
         子对父，父对子字典关系
         """
         #sign:1 子对父
         #sign:2 父对子
         course_ = {}
         for course_id in set(course_ids):
            course_[course_id] = '' if sign == 1 else []
         if hits:
            for hit in hits:
                if sign == 1:
                    if hit.course_id in course_:
                        course_[hit.course_id] = hit.parent_id
                else:
                    if hit.parent_id in course_:
                        course_[hit.parent_id].append(hit.course_id)
         else:
            for course_id in set(course_ids):
                if sign == 1:
                    course_[course_id] = course_id
                else:
                    course_[course_id].append(course_id)
         return course_

     def get(self):
         """
         先查出课程的parent_id，再根据parent_id查出所有的子课程，再拿这些子课程去查选课人数，进行聚合
         """
         course_ids = self.get_course_ids()

         #查parent_id
         query = self.es_query(index='course_ancestor', doc_type='course_ancestor')\
                     .filter('terms', course_id=course_ids)
         total = self.es_execute(query[:0]).hits.total
         result = self.es_execute(query[:total])
         parent_course_ids = [hit.parent_id for hit in result.hits] if result.hits else course_ids
         
         #子对父
         course_id_cp = self.get_course_2_ids(course_ids, 1, result.hits)

         #根据parent_id查出所有的子课程id
         query = self.es_query(index='course_ancestor', doc_type='course_ancestor')\
                     .filter('terms', parent_id=parent_course_ids)\
                     .filter('range', **{'status': {'gte': -1}})
         result = self.es_execute(query[:10000])
         children_course_ids = [hit.course_id for hit in result.hits] if result.hits else course_ids

         #父对子
         course_id_pc = self.get_course_2_ids(parent_course_ids, 2, result.hits)
         #查询这些子课程的数据然后聚合
         enrollments = mysql_connect.MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).get_enrollment(children_course_ids)
         course_enrollment = [{'course_id': enrollment['course_id'], 'enrollment_num': enrollment['enroll_all']} for enrollment in enrollments]
         data = self.get_course_enrollments(course_id_pc, course_id_cp, course_enrollment)
         actual_course_ids = [i['course_id'] for i in data]
         for i in course_ids:
            if i not in actual_course_ids:
                enrollments = mysql_connect.MysqlConnect(settings.MYSQL_PARAMS['teacher_power']).get_enrollment([i])
                course_enrollment = [{'course_id': enrollment['course_id'], 'acc_enrollment_num': enrollment['enroll_all']} for enrollment in enrollments]
                data.extend(course_enrollment)
         self.success_response({'data': data})

