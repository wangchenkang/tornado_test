#! -*- coding: utf-8 -*-
import settings
import MySQLdb
from MySQLdb.cursors import DictCursor
import sys
from utils.log import Log

Log.create('mysql')

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

    def course_permission(self, user_id, course_id, group_key):
        query = """
                select * from teacher_power where user_id='{0}' and course_id='{1}' and group_key='{2}'
                """.format(user_id, course_id, group_key)
        results = self.execute_query(query)
        if len(results) > 0:
            return True
        return False

    def course_group_key(self, user_id, course_id):
        query = """
                select group_key from teacher_power where user_id='{0}' and course_id='{1}' order by group_key
                """.format(user_id, course_id)
        results = self.execute_query(query)
        if len(results)>0:
            return results[0]
        return ''

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
        except Exception as e:
            Log.error(e)
        return results
