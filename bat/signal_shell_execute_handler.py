#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys
import subprocess
from conf import conf
from uow import uow
from log import log


handle_name = sys.argv[0].split(os.sep)[-1]
etl_id = sys.argv[1]
level_id = etl_id.split('.')[0]
table_id = etl_id.split('.')[1]
shell_name = sys.argv[2]
shell_argv = sys.argv[3:]

t_conf = conf(etl_id,handle_name)
t_uow = uow(t_conf)
t_log = log(t_conf)

uow_from = t_uow.getUOW_FROM(etl_id)
uow_to = t_uow.getUOW_TO(etl_id)

shell_file = t_conf.FOLDER_BAT + os.sep + shell_name

t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('shell_file:' + shell_file)
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))

t_env = os.environ.copy()
t_env['ETL_ID'] = str(etl_id)
t_env['LEVEL_ID'] = str(level_id)
t_env['TABLE_ID'] = str(table_id)
t_env['UOW_FROM'] = str(uow_from)
t_env['UOW_TO'] = str(uow_to)
cmd = shell_file + ' ' + ' '.join(shell_argv)

t_log.logger.info('command: ' + cmd)
t_log.logger.info(t_env)

sub = subprocess.Popen(cmd,cwd=t_conf.FOLDER_BAT,env=t_env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)

returncode = sub.poll()
while 1:
	line = sub.stdout.readline().decode('utf-8')
	if line == '':
		break
	print(line.strip())
	t_log.logger.info(line.strip())

returncode = sub.poll()
if not(returncode == 0) :
	t_log.logger.error('shell executed false.')
	sys.exit(4)
else:
	t_log.logger.info('shell executed success.')
	sys.exit(0)
