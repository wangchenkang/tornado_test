#! -*- coding: utf-8 -*-
import settings
from tornado import gen 
from core.log import Log
import json
from utils.tools import Mailer


class QuestionFeedBack:

    def __init__(self, course_id, index, doc_type):
        self.course_id = course_id
        self.index = index
        self.doc_type = doc_type
    
    def set_email(self):
         mailer = Mailer('smtp.xuetangx.com',settings.MAIL_LOGIN['user'],settings.MAIL_LOGIN['password'])
         mailto = settings.MAIL_TO
         mail_subject = 'TAP%s' % (u'快照问题反馈')
         body = '<html><h3>index:</h3>%s<h3>doc_type:<h3>%s<h3>课程代码:</h3>%s</html>' % (self.index, self.doc_type, self.course_id)
         mailer.mailto(mailto)
         mailer.text_body(body)
         mailer.mailsubject(mail_subject)
         mailer.send()
        
        self.success_response({'data': status})

