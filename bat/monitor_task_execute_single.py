#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#file   : monitor_task_execute_single.py
#author : bofei
#date   : 2017-02-08

import os
import sys
import subprocess
import re
from datetime import datetime

from conf import conf
#from uow import uow
from log import log
from db import db
#from cfg import cfg

from multiprocessing import Process, Lock
from multiprocessing.sharedctypes import Value, Array
from ctypes import Structure, c_double

class monitor_task_execute_single:

	def __init__(self,t_id,t_uow,t_host_name,t_script_type,t_script):
		self.task_info={'id':t_id,'uow':t_uow,'host_name':t_host_name,'script_type':t_script_name,'script':t_script}
		t_data_source_type = split(t_script_type,'-')[0]

		

		if (t_data_source_type == 'mysql'):
			self.task_name = 'func_mysql_exec'
		elseif (t_data_source_type == 'impala'):
			self.task_name = 'func_impala_exec'
		elseif (t_data_source_type == 'hive'):
			self.task_name = 'func_hive_exec'
		elseif (t_data_source_type == 'network'):
			self.task_name = 'func_network_exec'
		elseif (t_data_source_type == 'script'):
			self.task_name = 'func_script_exec'


	def getPID(self):
		t_PID = os.getpid()
		self.task_info['pid']=t_PID
		return t_PID

	def start(self):
		self.task = Process(target=self.task_name, args=self.args())
		self.task.start()
		getPID()

	def join(self):
		self.join()

	def _func_mysql_exec(self):


	def _func_impala_exec(self):
		

	def _func_hive_exec(self):
		

	def _func_network_exec(self):
		

	def _func_script_exec(self):
		

		

