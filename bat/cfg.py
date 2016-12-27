#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys
import configparser

class cfg:

	def __init__(self,conf):

		self.__cfg_file = conf.CFG_FILE

		cf = configparser.ConfigParser()
		cf.read(self.__cfg_file)

		#general
		self.FILE_TYPE = cf.get('general','file_type')

		#extract
		self.EX_DB_TYPE = cf.get('extract','db_type')
		self.EX_DBC_FILE = cf.get('extract','dbc_file')

		#load
		self.LD_DB_TYPE = cf.get('load','db_type')
		self.WORKING_TABLE = cf.get('load','working_table')
		self.RETENTION = cf.get('load','retention')
		self.EXTRACT_ETL_ID = cf.get('load','extract_etl_id',fallback = conf.ETL_ID)
		self.RECORD_VERIFICATION = cf.get('load','record_verification')
