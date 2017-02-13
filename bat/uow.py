#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
#from conf import conf
from datetime import datetime
from datetime import timedelta

def AddMonths(d,x):
    newmonth = ((( d.month - 1) + x ) % 12 ) + 1
    newyear  = d.year + ((( d.month - 1) + x ) // 12 )
    return datetime(newyear, newmonth, d.day, d.hour, d.minute, d.second)

class uow:

	def __init__(self,conf):
		self.FOLDER_UOW = conf.FOLDER_UOW

		self.__FOLDER_IN = conf.FOLDER_IN
		self.__FOLDER_BAT = conf.FOLDER_BAT
		self.__FOLDER_CFG = conf.FOLDER_CFG
		self.__FOLDER_DAT = conf.FOLDER_DAT
		self.__FOLDER_DBC = conf.FOLDER_DBC
		self.__FOLDER_DONE = conf.FOLDER_DONE
		self.__FOLDER_LOG = conf.FOLDER_LOG
		self.__FOLDER_SQL = conf.FOLDER_SQL
		self.__FOLDER_TMP = conf.FOLDER_TMP

	def __getNextUOW(self,UOW_FROM,FREQ,INTERVAL):
		UOW_TO = 0
		t_interval = int(INTERVAL)

		t_time = datetime.strptime(UOW_FROM, "%Y%m%d%H%M%S")
		if FREQ == 'YY' :
			UOW_TO = (AddMonths(t_time,12*t_interval)).strftime("%Y%m%d%H%M%S")
		elif FREQ == 'MM' :
			UOW_TO = (AddMonths(t_time,1*t_interval)).strftime("%Y%m%d%H%M%S")
		elif FREQ == 'WE' :
			addTime = timedelta(weeks=1*t_interval)
			UOW_TO = (t_time + addTime).strftime("%Y%m%d%H%M%S")
		elif FREQ == 'DD' :
			addTime = timedelta(days=1*t_interval)
			UOW_TO = (t_time + addTime).strftime("%Y%m%d%H%M%S")
		elif FREQ == 'HH' :
			addTime = timedelta(hours=1*t_interval)
			UOW_TO = (t_time + addTime).strftime("%Y%m%d%H%M%S")
		elif FREQ == 'MI' :
			addTime = timedelta(seconds=60*t_interval)
			UOW_TO = (t_time + addTime).strftime("%Y%m%d%H%M%S")

		return UOW_TO

	def getUOW_FROM(self,UOW_ID):
		with open(self.FOLDER_UOW + os.sep + UOW_ID, 'rt') as f:
			t_content = f.read()
		t_uow = t_content.split(',')[0]
		return t_uow

	def getUOW_TO(self,UOW_ID):
		with open(self.FOLDER_UOW + os.sep + UOW_ID, 'rt') as f:
			t_content = f.read()

		t_interval = 1 
		if len(t_content.split(','))==2:
			t_uow,t_freq = t_content.split(',')
		else:
			t_uow,t_freq,t_interval = t_content.split(',')
		t_uow = self.__getNextUOW(t_uow,t_freq,t_interval)
		return t_uow

	def initUOW(self,UOW_ID,FREQ,UOW_VALUE,INTERVAL):
		if not(os.path.exists(self.FOLDER_UOW)):
			os.makedirs(self.FOLDER_UOW)

		if not(os.path.exists(self.__FOLDER_IN)):
			os.makedirs(self.__FOLDER_IN)

		if not(os.path.exists(self.__FOLDER_BAT)):
			os.makedirs(self.__FOLDER_BAT)

		if not(os.path.exists(self.__FOLDER_CFG)):
			os.makedirs(self.__FOLDER_CFG)

		if not(os.path.exists(self.__FOLDER_DAT)):
			os.makedirs(self.__FOLDER_DAT)

		if not(os.path.exists(self.__FOLDER_DONE)):
			os.makedirs(self.__FOLDER_DONE)

		if not(os.path.exists(self.__FOLDER_LOG)):
			os.makedirs(self.__FOLDER_LOG)

		if not(os.path.exists(self.__FOLDER_SQL)):
			os.makedirs(self.__FOLDER_SQL)

		if not(os.path.exists(self.__FOLDER_TMP)):
			os.makedirs(self.__FOLDER_TMP)

		with open(self.FOLDER_UOW + os.sep + UOW_ID,'wt') as f:
			f.write(UOW_VALUE+','+FREQ+','+INTERVAL)
		return UOW_VALUE

	def setUOW_TO(self,UOW_ID):
		with open(self.FOLDER_UOW + os.sep + UOW_ID, 'rt') as f:
			t_content = f.read()

		t_interval = 1 
		if len(t_content.split(','))==2:
			t_uow,t_freq = t_content.split(',')
		else:
			t_uow,t_freq,t_interval = t_content.split(',')
		to_time = self.__getNextUOW(t_uow,t_freq,t_interval)

		with open(self.FOLDER_UOW + os.sep + UOW_ID,'wt') as f:
			f.write(to_time+','+t_freq+','+t_interval)

		return to_time

	def delUOW(self,UOW_ID):
		os.remove(self.FOLDER_UOW + os.sep + UOW_ID)
		return 1
