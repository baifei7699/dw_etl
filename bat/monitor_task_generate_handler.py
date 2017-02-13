#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#file   : monitor_task_generate_handler.py
#author : bofei
#date   : 2017-02-08

import os
import sys
import subprocess
import re
from datetime import datetime
import shutil

from conf import conf
from uow import uow
from log import log
from db import db
from cfg import cfg
from cron import cron

handle_name = sys.argv[0].split(os.sep)[-1]
etl_id = sys.argv[1]
level_id = etl_id.split('.')[0]
table_id = etl_id.split('.')[1]

t_conf = conf(etl_id,handle_name)
t_uow = uow(t_conf)
t_log = log(t_conf)
t_db = db(t_conf)

uow_from = t_uow.getUOW_FROM(etl_id)
uow_to = t_uow.getUOW_TO(etl_id)

t_monitor_task_sql = 'select ID,HOST_LIST,SCRIPT_TYPE,SCRIPT, \
                    EXPECTED_RESULT,SCHEDULE_INTERVAL \
                    from MONITOR_CASE \
                    WHERE IS_ENABLED=1 \
                    AND SCHEDULE_START_TIME <= STR_TO_DATE(\'' + uow_from + '\',\'%Y%m%d%h%i%s\') \
                    AND SCHEDULE_END_TIME >= STR_TO_DATE(\'' + uow_to + '\',\'%Y%m%d%h%i%s\')'


t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('monitor_task_sql:' + t_monitor_task_sql)
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))

t_uow_from = datetime.strptime(uow_from,'%Y%m%d%H%M%S')
t_uow_to = datetime.strptime(uow_to,'%Y%m%d%H%M%S')

t_db.setCONN()
t_db.db_query(t_monitor_task_sql)

t_cron = cron()
t_num = 0
for row in t_db.cur:
    t_id,t_host_list,t_script_type,t_script,t_expected_result,t_schedule_interval= row
    if t_cron.matchtime(t_schedule_interval,t_uow_from):
        if t_host_list != None :
            for t_host in str(t_host_list).split(';'):
                t_sql = "insert into MONITOR_CASE_RESULT \
                        (MONITOR_CASE_ID,UOW,HOST_NAME,SCRIPT_TYPE, \
                        SCRIPT,EXPECTED_RESULT,TASK_STATUS,IS_PASSED,CREATE_TIME) VALUES \
                        (" + str(t_id) + ",'" + uow_from + "','" + t_host + "', \
                        '" + t_script_type + "','" + t_script + "', \
                        '" + t_expected_result + "',0,1,now());"
                print(t_sql)
                t_db.db_execute(t_sql)
                t_num = t_num + 1
        else :
            t_sql = "insert into MONITOR_CASE_RESULT \
                        (MONITOR_CASE_ID,UOW,HOST_NAME,SCRIPT_TYPE, \
                        SCRIPT,EXPECTED_RESULT,TASK_STATUS,IS_PASSED,CREATE_TIME) VALUES \
                        (" + str(t_id) + ",'" + uow_from + "','localhost', \
                        '" + t_script_type + "','" + t_script + "', \
                        '" + t_expected_result + "',0,1,now());"
            print(t_sql)
            t_db.db_execute(t_sql)
            t_num = t_num + 1

t_log.logger.info('generate task:' + str(t_num))
