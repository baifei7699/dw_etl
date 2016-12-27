#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys

class db:

	def __init__(self,conf):
		self.__dbc_file = conf.DBC_FILE

		with open(self.__dbc_file, 'rt') as f:
			t_content = f.read().strip('\n')
		(self.host,self.port,self.database,self.user)= t_content.split(':')
		
	def setDBC(self,DBC_FILE):
		self.__dbc_file = DBC_FILE
		with open(self.__dbc_file, 'rt') as f:
			t_content = f.read().strip('\n')
		(self.host,self.port,self.database,self.user)= t_content.split(':')
