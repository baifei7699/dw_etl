#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#file   : monitor_task_execute_handler.py
#author : bofei
#date   : 2017-02-08

import os
import sys
import subprocess
import re
from datetime import datetime

from conf import conf
from uow import uow
from log import log
from db import db
from cfg import cfg
from monitor_task_execute_single import monitor_task_execute_single

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

t_task_sql = 'select ID,MONITOR_CASE_ID,UOW,HOST_NAME,SCRIPT_TYPE,SCRIPT, \
                    TASK_STATUS \
                    from MONITOR_CASE_RESULT \
                    WHERE IS_PASSED=1 AND TASK_STATUS=0 ORDER BY CREATE_TIME DESC'


t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('task_sql:' + t_task_sql)
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))

t_uow_from = datetime.strptime(uow_from,'%Y%m%d%H%M%S')
t_uow_to = datetime.strptime(uow_to,'%Y%m%d%H%M%S')

t_db.setCONN()
t_db.db_query(t_task_sql)

t_num = 0
t_task_executes = []
t_task_exe_infos = []
for row in t_db.cur:
    t_id,t_monitor_case_id,t_uow,t_host_name,t_script_type,t_script,t_task_status = row
    t_task_execute = monitor_task_execute_single(t_id,t_uow,t_host_name,t_script_type,t_script)
    t_task_executes.append(t_task_execute)
    t_task_execute.start()
    t_task_info = 'execute task:     pid:'+str(t_task_execute.getPID())+';     id:'+str(t_id)+';monitor_case_id:'+ \
                    str(t_monitor_case_id)+ ';uow:'+str(t_uow)+';host_name'+t_host_name+ \
                    ';script_type'+t_script_type+';t_script'+t_script+';'
    t_log.logger.info(t_task_info)
    t_num = t_num + 1

t_log.logger.info('total execute tasks:' + str(t_num))
t_log.logger.info('waiting for tasks finish.')

for t_task_execute in t_task_executes:
    t_task_execute.join()

t_log.logger.info('all tasks finished.')
#todo

    