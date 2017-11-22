#! -*- coding: utf-8 -*-
import os
import logging
import settings
from logging.handlers import RotatingFileHandler

class Log(object):
    instance = None

    def __init__(self, logname):
        logfile = settings.logging['log_file']
        dirname, filename = os.path.split(logfile)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)

        self.file_logger = logging.getLogger(logname)
        self.file_logger.setLevel(logging.DEBUG)
        rf = RotatingFileHandler(logfile, maxBytes=1024*1024, backupCount=30)
        rf.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(levelname)s %(message)s'))
        self.file_logger.addHandler(rf)
        self.file_logger.setLevel(logging.DEBUG)
        
    def __new__(cls, *args, **kwargs):
        if '_instance' not in cls.__dict__:
            cls._instance = object.__new__(cls, *args, **kwargs)

        return cls._instance

    def message(self, level, message):
        getattr(self.file_logger, level)(message)

    @classmethod
    def create(cls, logname='app'):
        cls.instance = Log(logname)

    @classmethod
    def info(cls, message):
        cls.instance.message('info', message)

    @classmethod
    def debug(cls, message):
        cls.instance.message('debug', message)

    @classmethod
    def error(cls, message):
        cls.instance.message('error', message)

    @classmethod
    def critical(cls, message):
        cls.instance.message('critical', message)
