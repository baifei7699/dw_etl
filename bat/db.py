#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys

class db:

	def __init__(self,conf):
		self.__dbc_file = conf.DBC_FILE
		self.__getDBC(self)
		
		
	def __getDBC(self):
		with open(self.__dbc_file, 'rt') as f:
			t_content = f.read().strip('\n')
		t_array = t_content.split(':')
		if t_array[0] == 'postgresql': 
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
		elif t_array[0] == 'mysql':
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
			self.password = t_array[5]
		elif t_array[0] == 'hive':
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
			self.password = t_array[5]
		elif t_array[0] == 'impala':
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
			self.password = t_array[5]


	def setDBC(self,DBC_FILE):
		self.__dbc_file = DBC_FILE
		self.__getDBC(self)


