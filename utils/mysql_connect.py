#! -*- coding: utf-8 -*-
import settings
import MySQLdb
from MySQLdb.cursors import DictCursor
import sys
from utils.log import Log


reload (sys)
sys.setdefaultencoding('utf-8')

class MysqlConnect(object):

    def __init__(self,params):
        self.host = params['host']
        self.db = params['db']
        self.user = params['user']
        self.password = params['password']
    
    def get_db(self):
        db = MySQLdb.connect(self.host, self.user, self.password, self.db)
        #加一个ping
        db.ping(True)
        return db

    def get_cursor(self):
        db = self.get_db()
        cursor = db.cursor(cursorclass=DictCursor)
        return db,cursor

    def execute_query(self, query):
        db, cursor = self.get_cursor()
        cursor.execute('set names utf8')
        cursor.execute(query)
        results = cursor.fetchall()
        db.close()
        return results

    def get_user_info(self, xid):
        query = """
                select user_id, avatar, nickname from auth_userprofile where unique_code = {0}
                """.format(xid)
        results = self.execute_query(query)
        user_id = results[0]['user_id']
        avatar = results[0]['avatar']
        nickname = results[0]['nickname']
        return user_id, avatar, nickname

    def get_rname_image(self, user_id):
        query = """
                select * from auth_userprofile where user_id = {0}
                """.format(user_id)
        results = self.execute_query(query)
        if results:
            avatar = results[0]['avatar']
            rname = results[0]['name']
        else:
            avatar = ''
            rname = ''
        return avatar, rname

    def get_role(self, user_id, host):
        query = """
                select distinct(mode) from teacher_power where user_id= {0} and host= '{1}'
                """.format(user_id, host)
        results = self.execute_query(query)
        return results

    def get_course_group_keys(self, user_id, host):
        query = """
                select distinct(course_id), group_key from teacher_power where user_id={0} and host='{1}'
                """.format(user_id, host)
        results = self.execute_query(query)
        data = {}
        for result in results:
            if result['course_id'] not in data:
                data[result['course_id']] = []
            data[result['course_id']].append(result['group_key'])
        course_ids = data.keys()
        return data,course_ids
    
    def get_cohort_group_keys(self, host, user_id, course_id):
        query = """
                select distinct(group_key) from teacher_power where host='{0}' and user_id='{1}' and course_id='{2}'
                """.format(host, user_id, course_id)
        results = self.execute_query(query)
        data = results if results else []
        return data

    def course_permission(self, user_id, course_id, group_key, host):
        query = """
                select * 
                from teacher_power 
                where user_id='{0}' and course_id='{1}' and group_key='{2}' \
                and host='{3}'
                """.format(user_id, course_id, group_key, host)
        results = self.execute_query(query)
        if len(results) > 0:
            return True
        return False

    def course_group_key(self, user_id, course_id, host):
        query = """
                select group_key 
                from teacher_power 
                where user_id='{0}' and course_id='{1}' and host = '{2}'
                order by group_key
                """.format(user_id, course_id, host)
        results = self.execute_query(query)
        if len(results)>0:
            return results[0]
        return ''

    def get_courses(self, user_id):
        query = """
                select distinct(course_id), group_key from teacher_power where user_id={0}
                """.format(user_id)
        results = self.execute_query(query)
        data = {}
        for result in results:
            if result['course_id'] not in data:
                data[result['course_id']] = []
            data[result['course_id']].append(result['group_key'])
        
        return data

    def get_enrollment(self, course_ids):
        course_ids = [course_id.encode('utf-8') for course_id in course_ids]
        course_ids = tuple(course_ids)
        if len(course_ids) == 1:
            course_id = course_ids[0]
            query = """
                    SELECT course_id, enroll_all
                    FROM enroll_count
                    WHERE course_id = '{0}'
                    """.format(course_id)
        else:
            query = """
                    SELECT course_id, enroll_all
                    FROM enroll_count
                    where course_id in {0}
                    """.format(course_ids)
        try:
            results = self.execute_query(query)
            if not results:
                data = []
                for course_id in set(course_ids):
                    data.append({'course_id': course_id, 'enroll_all': 0})
                results = data
        except Exception as e:
            #Log.create('mysql')
            Log.error(e)
        
        return results


    def get_exclude_tiny_mooc_course_ids(self, course_ids):
        course_ids = [course_id.encode('utf-8') for course_id in course_ids]
        course_ids = tuple(course_ids)
        if len(course_ids) != 1: 
            sql = """
                  SELECT course_id
                  FROM course_info
                  WHERE course_id in {}
                  AND course_mode != 2
                  """.format(course_ids)
        else:
            course_id == course_ids[0]
            sql = """
                  SELECT course_id
                  FROM course_info
                  WHERE course_id = '{0}'
                  AND course_mode != 2
                  """.format(course_id)
        results = self.execute_query(sql)
        return results
    
    def get_tiny_mooc_course_ids(self, course_ids):
        course_ids = [course_id.encode('utf-8') for course_id in course_ids]
        course_ids = tuple(course_ids)
        if len(course_ids) != 1: 
            sql = """
                  SELECT course_id
                  FROM course_info
                  WHERE course_id in {}
                  AND course_mode = 2
                  """.format(course_ids)
        else:
            course_id == course_ids[0]
            sql = """
                  SELECT course_id
                  FROM course_info
                  WHERE course_id = '{0}'
                  AND course_mode = 2
                  """.format(course_id)
        results = self.execute_query(sql)
        
        return results
