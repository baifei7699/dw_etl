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

t_conf = conf(etl_id,handle_name)
t_uow = uow(t_conf)
t_log = log(t_conf)

uow_from = t_uow.getUOW_FROM(etl_id)
uow_to = t_uow.getUOW_TO(etl_id)

touch_path = t_conf.FOLDER_DONE + os.sep + level_id  + os.sep + etl_id
touch_file = touch_path + uow_to
if not(os.path.exists(touch_path)):
			os.makedirs(touch_path)

t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))
t_log.logger.info('touch_file:' + touch_file)

with open(touch_file,'wt') as f:
	f.write('')
t_log.logger.info('touch file success')
