#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys
import pymysql
import impala

class db:

	def __init__(self,conf):
		self.__dbc_file = conf.DBC_FILE
		if os.path.exists(self.__dbc_file):
			self.__getDBC()
		
		
	def __getDBC(self):
		with open(self.__dbc_file, 'rt') as f:
			t_content = f.read().strip('\n')
		t_array = t_content.split(':')
		if t_array[0] == 'postgresql': 
			self.db_type = 'postgresql'
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
		elif t_array[0] == 'mysql':
			self.db_type = 'mysql'
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
			self.password = t_array[5]
		elif t_array[0] == 'hive':
			self.db_type = 'hive'
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
			self.password = t_array[5]
		elif t_array[0] == 'impala':
			self.db_type = 'impala'
			self.host = t_array[1]
			self.port = t_array[2]
			self.database = t_array[3]
			self.user = t_array[4]
			self.password = t_array[5]


	def setDBC(self,DBC_FILE):
		self.__dbc_file = DBC_FILE
		self.__getDBC()

	def setCONN(self):
		if self.db_type == 'mysql':
			self.conn = pymysql.connect(host=self.host,port=int(self.port),user=self.user,passwd=self.password,db=self.database)
		elif self.db_type == 'impala' or self.db_type == 'hive':
			self.conn = impala.dbapi.connect(host=self.host,port=int(self.port),database=self.database,auth_mechanism='NOSASL')
			

	#insert/update/DDL
	def db_execute(self,t_sql):
		t_sql_list = t_sql[:-1].split(";")
		if self.db_type == 'mysql':
			t_cur = self.conn.cursor()
			for t_s in t_sql_list:
				#try:
					t_cur.execute(t_s)
				#except:
				#	self.conn.rollback()
			#t_cur.close()
			self.conn.commit()
		elif self.db_type == 'impala':
			t_cur = self.conn.cursor()
			for t_s in t_sql_list:
				t_cur.execute(t_s)
		elif self.db_type == 'hive':
			t_cur = self.conn.cursor()
			for t_s in t_sql_list:
				t_cur.execute(t_s)
			

	def db_query(self,t_sql):
		if self.db_type == 'mysql':
			t_cur = self.conn.cursor()
			t_cur.execute(t_sql)
			self.cur = t_cur
		elif self.db_type == 'impala':
			t_sql_list = t_sql[:-1].split(";")
			t_cur = self.conn.cursor()
			for t_s in t_sql_list:
				t_cur.execute(t_s)
			self.cur = t_cur
		elif self.db_type == 'hive':
			t_sql_list = t_sql[:-1].split(";")
			t_cur = self.conn.cursor()
			for t_s in t_sql_list:
				t_cur.execute(t_s)
			self.cur = t_cur
			

	def closeCONN(self):
		self.conn.close()