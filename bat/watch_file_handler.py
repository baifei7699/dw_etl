#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys
import subprocess
import time

from conf import conf
from uow import uow
from log import log

handle_name = sys.argv[0].split(os.sep)[-1]
etl_id = sys.argv[1]
level_id = etl_id.split('.')[0]
table_id = etl_id.split('.')[1]
watch_etl_id = sys.argv[2]

t_conf = conf(etl_id,handle_name)
t_uow = uow(t_conf)
t_log = log(t_conf)

uow_from = t_uow.getUOW_FROM(etl_id)
uow_to = t_uow.getUOW_TO(etl_id)

watch_file = t_conf.FOLDER_DONE + os.sep + watch_etl_id.split('.')[0] + os.sep + watch_etl_id + uow_to

t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))
t_log.logger.info('watch_file:' + watch_file)

while 1 :
	if os.path.exists(watch_file):
		t_log.logger.info('file generated.')
		break
	else:
		t_log.logger.info('file not generated')
		time.sleep(180)
