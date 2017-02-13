#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import configparser
from datetime import datetime
from datetime import timedelta

working_folder = '/home/bofei/worksapce/dw_etl'
conf_folder = 'conf'
base_conf_name = 'base.conf'

class conf:

    def __init__(self,ETL_ID,handler_name):

        (LEVEL_ID,TABLE_ID)=ETL_ID.split('.')
        self.ETL_ID = ETL_ID

        t_now = datetime.now()
        self.f_time = t_now.strftime("%Y%m%d%H%M%S")
        self.f_date = t_now.strftime("%Y%m%d")

        conf_file = working_folder + os.sep + conf_folder + os.sep + base_conf_name
        cf = configparser.ConfigParser()
        cf.read(conf_file)

        self.WORKING_FOLDER = working_folder

        #folders
        self.FOLDER_IN = working_folder + os.sep + cf.get('folder','IN') + os.sep + LEVEL_ID
        self.FOLDER_CFG = working_folder + os.sep + cf.get('folder','CFG') + os.sep + LEVEL_ID
        self.FOLDER_BAT = working_folder + os.sep + cf.get('folder', 'BAT') + os.sep + LEVEL_ID
        self.FOLDER_DBC = working_folder + os.sep + cf.get('folder', 'DBC') 
        self.FOLDER_LOG = working_folder + os.sep + cf.get('folder', 'LOG') + os.sep + LEVEL_ID + os.sep + ETL_ID + os.sep + self.f_date
        self.FOLDER_SQL = working_folder + os.sep + cf.get('folder', 'SQL') + os.sep + LEVEL_ID
        self.FOLDER_DAT = working_folder + os.sep + cf.get('folder', 'DAT') + os.sep + LEVEL_ID
        self.FOLDER_DONE = working_folder + os.sep + cf.get('folder', 'DONE')
        self.FOLDER_UOW = working_folder + os.sep + cf.get('folder', 'UOW') + os.sep + LEVEL_ID
        self.FOLDER_TMP = working_folder + os.sep + cf.get('folder', 'TMP') + os.sep + LEVEL_ID

        #files
        self.DBC_FILE = self.FOLDER_DBC + os.sep + LEVEL_ID.split('.')[0] + '.dbc'
        self.CFG_FILE = self.FOLDER_CFG + os.sep + ETL_ID + '.cfg'
        self.LOG_FILE = self.FOLDER_LOG + os.sep + handler_name + '_' + self.f_time + '.log'

        # self.FILE_UOW = working_folder + os.sep + self.FOLDER_UOW + os.sep + cf.get('uow','FREQ_FILE')
