#! -*- coding: utf-8 -*-
import os
import sys
import json
import hashlib
import datetime
import elasticsearch
from urllib import unquote
from utils.routes import route
from base import BaseHandler
from tornado.web import gen
from collections import defaultdict
from elasticsearch import Elasticsearch
from dateutil.relativedelta import relativedelta


reload(sys)
sys.setdefaultencoding('utf-8')

#es_index = 'elective_grade_report'
es_index='monthly_report'

request_mapping = {"cover_info":"tap_elective_course_plan_m",\
                   "base_info":"tap_elective_course_m",\
                   "ending_info":"tap_elective_course_ending_info_m",\
                   "analysis_base_info":"tap_elective_course_base_info_m",\
                   "analysis_check_plan":"tap_elective_course_check_plan_m",\
                   "analysised_post_info":"tap_elective_course_post_info_m",\
                   "analysis_grade_info":"tap_elective_course_grade_m",\
                   "pass_rate":"tap_elective_course_pass_rate_m"}

object_json = {"ending_info":["course_id","course_name","category_name","status","borderline"],\
               "analysis_base_info":["course_id","course_name","months","duration""owner_school","subject"],\
               "analysis_check_plan":["course_id","course_name","months","check_plan","check_plan_weight"]}


hosts = [{"host":'10.0.2.128',"port":9200},{"host":'10.0.2.130',"port":9200},{"host":'10.0.2.132',"port":9200}, {"host":'10.0.2.133',"port":9200}, {"host":'10.0.2.135',"port":9200}]

def check_token(token):
    checkstr = ""
    md5str = hashlib.md5(checkstr).hexdigest()
    return md5str==token

def get_datestr():
    interval = relativedelta(months=1)
    date = datetime.datetime.today()-interval
    #date = datetime.datetime.today()-datetime.timedelta(days=28)
    datestr = '%04d%02d'%(date.year,date.month)
    return datestr


import traceback
import json
class jsonObject():
    def __init__(self, data):
        self.__dict__ = data


def get_es_data(handlerobj,doc_type,month,school,index='monthly_report',sortkey="course_id"):
    data_return = []
    client = Elasticsearch(hosts)
    response = client.search(\
    index="%s"%index,\
    doc_type="%s"%doc_type,\
    size=50000,\
    body = {"query": { "bool": {"filter": [{"term": {"months": "%s"%month}},{"term": {"school":"%s"%school}}]}},"sort": [{"%s"%sortkey: {"order": "desc"}}]})

    #print response['hits']['hits']
    for hit in response['hits']['hits']:
        converted = {}
        for key,value in hit[u'_source'].iteritems():
            converted[key.encode('utf-8')]=value
        converted = json.loads(json.dumps(converted), object_hook=jsonObject)
        data_return.append(converted)

    return data_return




def get_es_course_data(handlerobj,doc_type,month,school,course_id,index='monthly_report'):
    data_return = []
    client = Elasticsearch(hosts)
    response = client.search(\
    index="%s"%index,\
    doc_type="%s"%doc_type,\
    size=50000,\
    body = {"query": { "bool": {"filter": [{"term": {"months": "%s"%month}},{"term": {"course_id": "%s"%course_id}},{"term": {"school":"%s"%school}}]}}})

    #print response['hits']['hits']
    for hit in response['hits']['hits']:
        converted = {}
        for key,value in hit[u'_source'].iteritems():
            converted[key.encode('utf-8')]=value
        converted = json.loads(json.dumps(converted), object_hook=jsonObject)
        data_return.append(converted)

    return data_return




def get_video_rate(handlerobj,index,doc_type,course_id):
    data_return = []
    client = Elasticsearch(hosts)
    response = client.search(\
    index="%s"%index,\
    doc_type="%s"%doc_type,\
    size=50000,\
    body = {"query": { "bool": {"filter": [{"term": {"course_id": "%s"%course_id}}]}}})

    #print response['hits']['hits']
    for hit in response['hits']['hits']:
        converted = {}
        for key,value in hit[u'_source'].iteritems():
            converted[key.encode('utf-8')]=value
        converted = json.loads(json.dumps(converted), object_hook=jsonObject)
        data_return.append(converted)

    return data_return



def get_check_plan(l1,l2):
    l1 = l1.encode('utf-8').strip() if type(l1)==unicode else l1.strip()
    l2 = l2.encode('utf-8').strip() if type(l2)==unicode else l2.strip()
    import re
    pattern = re.compile(r"^\[(.*)\]$")
    l1 = pattern.match(l1).groups()[0].split(',')
    l2 = json.loads(l2)
    return zip(l1,l2)


@route("/elective_course/course_info")
class ElectiveCourseBase(BaseHandler):
    '''获取选修课课程的相关信息'''
    def get(self):
        datestr = get_datestr()
        try:
            school = self.get_argument("school")
            info_type = self.get_argument("info_type")
            info_quantity = self.get_argument("info_quantity")
        except Exception,e:
            self.error_response(error_code=502,error_msg="参数缺少")
        if info_type not in request_mapping.keys():
            self.error_response(error_code=503,error_msg="参数错误")
        
        
        #school = unquote(school)
        school = school
        doc_type = request_mapping.get(info_type)
        infos = get_es_data(self,doc_type,datestr,school,index=es_index,sortkey='course_id')
        
         
        if info_type == "base_info":
            category_info = defaultdict(dict)
            category_list = []
            for category in infos:
                if category.series not in category_list:
                    category_list.append(category.series)
            for object in infos:
                category_name = object.series
                if not category_info.has_key(category_name):
                    category_info[category_name] = {"series":category_name}
                    category_info[category_name]["course_list"] = []
                category_info[category_name]["course_list"].append(object.course_name)
                category_info[category_name]["course_num"] = len(category_info[category_name]["course_list"])
            category_info = dict(category_info)
            result_json = []
            for category in category_list:
                result_json.append(category_info.get(category))

        elif info_type == "ending_info":
            result_json = [{"months":object.months,"course_id":object.course_id,"series":object.series,\
                            "course_name":object.course_name,"borderline":object.borderline,\
                            "school":object.school,"status":object.status} for object in infos]
        else:
            result_json = {}


        if info_quantity=="all":#返回全部数据
            self.success_response({'data':result_json})
        else:#返回分页数据
            try:
                page_size = self.get_argument("page_size")
                page_num = self.get_argument("page_num")
                page_size = int(page_size.encode('utf-8')) if type(page_size)==unicode else int(page_size)
                page_num = int(page_num.encode('utf-8')) if type(page_num)==unicode else int(page_num)
            except Exception ,e:
                self.error_response(error_code=503,error_msg="参数错误")
            course_num = len(result_json)
            start_index = (page_num-1)*page_size
            if start_index>=course_num:
                self.error_response(error_code=504,error_msg="未有相关页面信息")
            end_index = course_num if start_index+page_size>course_num else start_index+page_size
            self.success_response({'data':result_json[start_index:end_index]})

def get_info(key,school,video_keys,dict1,dict2,dict3):
    temp = dict1.get(key,{})
    temp.update(dict2.get(key,{}))
    end = key.rfind('_')
    video_key = key[:end]
    if video_key in video_keys:
        #print video_keys
        temp.update(dict3.get(video_key))
    #temp.update(dict3.get(key,{}))
    return temp


def str_to_json(teacher_str):
     
    return_lines = []
    lines = teacher_str.split(',')
    #print lines
    for line in lines:
        info = line.split('_')
        json_obj = {'name':info[0],'school':info[1],'department':info[2],'position':info[3]}
        return_lines.append(json_obj)
    return return_lines

@route("/elective_course/detail_info")
class ElectiveCourseDataReport(BaseHandler):
    '''课程数据报告接口'''
    def get(self):
        '''已列表形式返回学校下全部的信息'''
        #info_quantity = self.get_argument("info_quantity")
        school = self.get_argument("school")
        courseid_list = [course.get("course_id") for course in get_course_list(self,school)]

        datestr = get_datestr()
        base_info_type = request_mapping.get("analysis_base_info")
        check_plan_type = request_mapping.get("analysis_check_plan")
        pass_rate_type = request_mapping.get("pass_rate")

        

        return_list = []
        for course_id in courseid_list:
            ending_info_type = request_mapping.get("ending_info")
            ending_info = get_es_course_data(self,doc_type=ending_info_type,month=datestr,school=school,course_id=course_id,index=es_index)
            try:
                ending_status = ending_info[0].status
            except Exception,e:
                ending_status = ''
            ending_info={"status":ending_status}           
 
            base_info = get_es_course_data(self,doc_type=base_info_type,month=datestr,\
                                           school=school,course_id=course_id,index=es_index)
            print course_id
            #print base_info[0].teachers.encode('utf-8')
            if len(base_info)==0:
                base_info={}
       
            else:
                base_info = base_info[0]
                #print base_info.duration
                #"duration":base_info.duration,\
                #import pdb
                #pdb.set_trace()
                base_info = {"months":base_info.months,"course_id":base_info.course_id,\
                             "course_name":base_info.course_name,\
                             "owner_school":'' if 'None'==base_info.owner_school else base_info.owner_school,"school":base_info.school,\
                             "subject":base_info.subject,"course_about":base_info.course_about,\
                             "teachers":str_to_json(base_info.teachers)}
                base_info.update(ending_info)

            check_plan = get_es_course_data(self,doc_type=check_plan_type,month=datestr,\
                                            school=school,course_id=course_id,index=es_index)
            if len(check_plan)==0:
                check_plan={}
            else:
                check_plan = check_plan[0]
                check_plan = {"months":check_plan.months,"course_id":check_plan.course_id,\
                              "course_name":check_plan.course_name,\
                              "check_plan":get_check_plan(check_plan.check_plan,check_plan.check_plan_weight)}
            pass_rate = get_es_course_data(self,doc_type=pass_rate_type,month=datestr,\
                                           school=school,course_id=course_id,index=es_index)
            if len(pass_rate)==0:
                pass_rate = {}
            else:
                pass_rate = pass_rate[0]
                pass_rate = {"months":pass_rate.months,"course_id":pass_rate.course_id,\
                             "course_name":pass_rate.course_name,"all_num":pass_rate.all_num,"fail_num":pass_rate.fail_num}
            
            report_info = self.get_report_data_info(school,course_id,info_quantity="all")
            return_list.append({"base_info":base_info,"check_plan":check_plan,\
                                "pass_rate":pass_rate,"report_info":report_info})
        self.success_response({'data':return_list})


    def get_report_data_info(self,school,course_id,info_quantity):

        #import pdb
        #pdb.set_trace()


        doc_type = "tap_elective_course_post_info_m"
        datestr = get_datestr()
        post_infos = get_es_course_data(self,doc_type,datestr,school,course_id,index=es_index)
        #doc_id months course_name user_id student_id name post_count reply_count school
        post_dict = {"%s_%s_%s"%(object.course_id,object.user_id,object.school):\
                          {"course_id":object.course_id,"user_id":object.user_id,\
                              "school":object.school,"month":object.months,\
                              "course_name":object.course_name,"student_id":object.student_id,\
                              "name":object.name,"post_count":object.post_count,\
                              "reply_count":object.reply_count} \
                     for object in post_infos}
        #doc_id months user_id name course_name hw_type grade school
        doc_type = "tap_elective_course_grade_m"
        
        #import pdb
        #pdb.set_trace()
        grade_infos = get_es_course_data(self,doc_type,datestr,school,course_id,index=es_index)
        
        grade_infos = [(object.course_id,object.user_id,object.school,\
                        object.course_name,object.months,object.hw_type,\
                        object.grade,object.grade_final,object.grade_level,object.name,object.student_id) \
                       for object in grade_infos]
        
        
        grade_dict = defaultdict(dict)
        for item in grade_infos:
            key = "_".join([item[0],item[1],item[2]])
            if not grade_dict.has_key(key):
                grade_dict[key] = {"course_name":item[3],"months":item[4],"user_id":item[1],\
                                   "school":item[2],"name":item[9],"student_id":item[10],\
                                   "grade_final":item[7],"grade_level":item[8],\
                                   "grade_detail":{item[5]:'' if item[6]==None or item[6]=='null' or item[6]<0 else item[6]}}
            else:
                grade_dict[key]["grade_detail"].update({item[5]:'' if item[6]==None or item[6]=='null' or item[6]<0 else item[6]})


        
        #添加视频学习比例
        pass
        #/tap/tap_video_course_all/_search
        video_infos = get_video_rate(self,index="tap",doc_type="video_course",course_id=course_id)
        video_dict = {"%s_%s"%(info.course_id,info.user_id):{"video_rate":info.study_rate} for info in video_infos}
        #import pdb
        #pdb.set_trace()        

        all_keys = list(set(post_dict.keys()+grade_dict.keys()))
        video_keys = list(set(video_dict.keys()))


        data_list = [get_info(key,school,video_keys,post_dict,grade_dict,video_dict)  for key in all_keys]
        #data_list = list(set(data_list))
        data_all = []
        for data in data_list:
            if data:
                data_all.append(data)
        data_list=data_all

        if info_quantity=="all":
            #data_info = json.dumps(data_list)
            #self.success_response({"data":data_list})
            return data_list
        else:
            #分页请求
            page_size = self.get_argument("page_size")
            page_num = self.get_argument("page_num")
            try:
                page_size = int(page_size.encode('utf-8')) if type(page_size)==unicode else int(page_size)
                page_num = int(page_num.encode('utf-8')) if type(page_num)==unicode else int(page_num)
            except Exception, e:
                print e
            num = len(data_list)
            start_index = (page_num-1)*page_size
            if start_index>=num:
                print "未有相关页面信息"
            end_index = num if start_index+page_size>num else start_index+page_size
            return data_list[start_index:end_index]


def get_course_list(handler,school):
    datestr = get_datestr()
    infos = get_es_data(handler,request_mapping.get("base_info"),datestr,school,index=es_index,sortkey="course_id")
    coursename_list = [{"course_id":object.course_id,\
                        "course_name":object.course_name,\
                        "category_name":object.series} for object in infos]
    return coursename_list



@route("/elective_course/course_list")
class ElectiveCourseList(BaseHandler):
    '''返回按课程类别分类排序下面的课程列表'''
    def get(self):
        school = self.get_argument("school")
        coursename_list = get_course_list(self,school)
        self.success_response({'data':coursename_list})


@route("/elective_course/cover")
class ElectiveCourseCover(BaseHandler):
    '''获取封面信息'''
    def  get(self):
        school = self.get_argument("school")
        datestr = get_datestr()
        infos = get_es_data(self,request_mapping.get("cover_info"),datestr,school,index=es_index)
        if len(infos)==0:
            cover_info = {}
        else:
            cover_info = infos[0]
            #print cover_info.plan_name
            #print cover_info.update_date
            #generate_date es内部字段？
            cover_info = {"plan_name":cover_info.plan_name,\
                          "school":cover_info.school,\
                          "update_date":cover_info.update_date}
        self.success_response({'data':cover_info})



@route("/elective_course/catalogue")
class ElectiveCourseCatalogue(BaseHandler):
    '''返回课程目录'''
    def get(self):
        #返回课程目录
        school = self.get_argument("school")
        course_list = get_course_list(self,school)
        course_list = [course.get('course_name') for course in course_list]
        catalogue = {"catalogue":[\
            {	"title":"选课信息概览"},\
            {\
                "title":"相关信息说明",\
                "section":["课程结课情况汇总","相关学习说明"]\
            },\
            {\
                "title":"课程成绩分析",\
                "section":course_list\
            }\
            ]}
        self.success_response({'data':catalogue})

