#! -*- coding: utf-8 -*-
import pytz
from datetime import datetime, timedelta
from tornado.escape import url_unescape
import math
import settings
import email
import smtplib

timezone = 'Asia/Chongqing'

def utc_to_cst(dt):
    dt = dt.replace(tzinfo=pytz.utc)
    tz = pytz.timezone(timezone)
    return dt.astimezone(tz)

def date_to_str(dt, time_format="%Y-%m-%d"):
    return dt.strftime(time_format)

def date_from_query(dt_string, time_format='%Y-%m-%d'):
    return datetime.strptime(dt_string, time_format)

#新的时间格式加了个+08:00
def date_from_new_date(time_string, time_format="%Y-%m-%dT%X+08:00"):
    try:
        return datetime.strptime(time_string, time_format)
    except (AttributeError, TypeError):
        return None

def is_ended(end_time, now=None):
    if now is None:
        now = datetime.utcnow()
    if end_time == 'now':
        return False
    end_time = date_convert(end_time)
    return end_time and end_time < now
    
def date_convert(date):
    if isinstance(date, basestring):
        return date_from_string(date)
    return date

def date_from_string(time_string, time_format='%Y-%m-%dT%X'):
    try:
        return datetime.strptime(time_string, time_format)
    except(AttributeError, TypeError):
        return None

def datedelta(dt, day):
    return date_to_str(datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=day))

def fix_course_id(course_id):
    return url_unescape(course_id).replace(' ', '+')

def var(data, total=None):
    if not total:
        total = len(data)
    data += [0]*(total-len(data))
    avg = sum(data)/total
    return math.sqrt(sum([(i-avg)*(i-avg) for i in data])/total)

def get_group_type(group_key):
    course_group_key = group_key
    group_keys = [('elective', settings.ELECTIVE_GROUP_KEY), \
                  ('cohort', settings.COHORT_GROUP_KEY),\
                  ('elective', settings.ELECTIVE_ALL_GROUP_KEY),\
                  ('tsinghua', settings.TSINGHUA_GROUP_KEY), \
                  ('spoc', settings.SPOC_GROUP_KEY), \
                  ('mooc', settings.MOOC_GROUP_KEY)]

    for group_key in group_keys:
        if course_group_key >= group_key[1]:
            return group_key[0]

class Mailer:
    def __init__(self, smtp_host, smtp_user, smtp_passwd, smtp_port = 25):
        self.smtp_host = smtp_host
        self.smtp_user = smtp_user
        self.smtp_passwd = smtp_passwd
        self.smtp_port = smtp_port
        self.mail = email.MIMEMultipart.MIMEMultipart('related')
        self.alter = email.MIMEMultipart.MIMEMultipart('alternative')
        self.mail.attach(self.alter)
        self.attachments = []
        self.mail['from'] = settings.MAIL_LOGIN['user']
        self._from = settings.MAIL_LOGIN['user']
    def mailto(self, mail_to) :
        """
         mail_to : comma separated emails▫
        """
        self._to = mail_to
        if type(mail_to) == list:
            self.mail['to'] = ','.join(mail_to)
        elif type(mail_to) == str :
            self.mail['to'] = mail_to
        else:
            raise Exception('invalid mail to')
    def mailsubject(self, mail_subject):
        self.mail['subject'] = mail_subject 
    def text_body(self, body, encoding = 'utf-8'):
        self.alter.attach(email.MIMEText.MIMEText(body, 'html', encoding))
    def send(self):
        self.mail['Date'] = email.Utils.formatdate( )
        smtp = False
        try:
            smtp = smtplib.SMTP()
            smtp.set_debuglevel(0)
            smtp.connect(self.smtp_host, self.smtp_port)
            smtp.login(self.smtp_user, self.smtp_passwd)
            smtp.sendmail(self._from, self._to, self.mail.as_string())
            return  True 
        except Exception, e:
            return False

class feedback:

    def __init__(self, index, doc_type, course_id):
        self.index = index
        self.doc_type = doc_type
        self.course_id = course_id

    def set_email(self):
        try:
            mailer = Mailer('smtp.xuetangx.com',settings.MAIL_LOGIN['user'],settings.MAIL_LOGIN['password'])
            mailto = settings.MAIL_TO
            mail_subject = 'TAP%s' % (u'结课快照问题反馈')
            body = '<html><h3>index:</h3>%s<h3>doc_type:</h3>%s<h3>课程代码:</h3>%s<h3></html>' % (self.index, self.doc_type, self.course_id)
            mailer.mailto(mailto)
            mailer.text_body(body)
            mailer.mailsubject(mail_subject)
            mailer.send()
        except Exception as e:
            Log.create('feedback')
            Log.error(e)

































      
