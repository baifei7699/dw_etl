#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
from datetime import datetime
from datetime import timedelta
import logging  
import logging.handlers

fmt = '%(asctime)s - %(lineno)s - %(levelname)s - %(message)s'  

class log:

	def __init__(self,conf):

		t_conf = conf
		#t_now = datetime.now()
		#ts_date = t_now.strftime("%Y%m%d")
		#ts_time = t_now.strftime("%Y%m%d%H%M%S")
		#self.f_time = ts_time
		
		logfile_path = conf.FOLDER_LOG
		logfile = conf.LOG_FILE
		
		if not(os.path.exists(logfile_path)):
			os.makedirs(logfile_path)

		handler = logging.handlers.RotatingFileHandler(logfile, maxBytes = 1024*1024, backupCount = 5)
		formatter = logging.Formatter(fmt)
		handler.setFormatter(formatter)

		console = logging.StreamHandler()
		console.setFormatter(formatter)

		self.logger = logging.getLogger('log')
		self.logger.setLevel(logging.DEBUG)
		self.logger.addHandler(handler)
		self.logger.addHandler(console)
