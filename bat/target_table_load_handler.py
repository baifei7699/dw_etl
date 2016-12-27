#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import os
import sys
import subprocess
import re
from datetime import datetime

from conf import conf
from uow import uow
from log import log
from db import db


def multiple_replace(t_text,tDict):
	mr_text = re.compile('|'.join(map(re.escape,tDict)))
	def one_xlat(match):
		return tDict[match.group(0)]
	return mr_text.sub(one_xlat,t_text)

handle_name = sys.argv[0].split(os.sep)[-1]
etl_id = sys.argv[1]
level_id = etl_id.split('.')[0]
table_id = etl_id.split('.')[1]
sql_name = sys.argv[2]

t_conf = conf(etl_id,handle_name)
t_uow = uow(t_conf)
t_log = log(t_conf)
t_db = db(t_conf)

uow_from = t_uow.getUOW_FROM(etl_id)
uow_to = t_uow.getUOW_TO(etl_id)

sql_file = t_conf.FOLDER_SQL + os.sep + sql_name
tmp_sql_file = t_conf.FOLDER_TMP + os.sep + sql_name + t_conf.f_time

t_env = {}
t_env['{ETL_ID}'] = str(etl_id)
t_env['{LEVEL_ID}'] = str(level_id)
t_env['{TABLE_ID}'] = str(table_id)
t_env['{UOW_FROM}'] = str(uow_from)
t_env['{UOW_TO}'] = str(uow_to)

t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('sql_file:' + sql_file)
t_log.logger.info('tmp_sql_file:' + tmp_sql_file)
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))

with open(sql_file, 'rt') as f:
	t_content = f.read()

tmp_sql = multiple_replace(t_content,t_env)

with open(tmp_sql_file,'wt') as f:
	f.write(tmp_sql)

cmd = 'psql -h ' + t_db.host + ' -p ' + t_db.port + ' -d '  + t_db.database + ' -v ON_ERROR_STOP=1 -U ' + t_db.user + ' -f ' + tmp_sql_file
t_log.logger.info('command: ' + cmd)
t_log.logger.info(t_env)

sub = subprocess.Popen(cmd,cwd=t_conf.FOLDER_SQL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)

while 1:
	line = sub.stdout.readline().decode('utf-8')
	if line == '':
		break
	print(line.strip())
	t_log.logger.info(line.strip())

returncode = sub.poll()
if not(returncode == 0) :
	t_log.logger.error('sql executed false.')
	sys.exit(4)
else:
	t_log.logger.info('sql executed success.')
	sys.exit(0)
