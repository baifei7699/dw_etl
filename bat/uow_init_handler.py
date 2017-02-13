#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys

from conf import conf
from uow import uow
from log import log

handle_name = sys.argv[0].split(os.sep)[-1]
etl_id = sys.argv[1]
level_id = etl_id.split('.')[0]
table_id = etl_id.split('.')[1]
uow_freq = sys.argv[2]
uow_value = sys.argv[3]
uow_interval = 1
if len(sys.argv)>4:
	uow_interval = sys.argv[4]

t_conf = conf(etl_id,handle_name)
t_uow = uow(t_conf)
t_log = log(t_conf)

t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))

t_uow_value = t_uow.initUOW(etl_id,uow_freq,uow_value,uow_interval)

if int(t_uow_value) > 0 :
	t_log.logger.info('uow init success.')
else:
	t_log.logger.error('uow init false.')
	sys.exit(4)
