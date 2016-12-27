#!/usr/bin/env python3
#-*- coding:utf-8 -*-

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

def nonBlockReadline(output):
    fd = output.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    try:
        return output.readline().decode('utf-8')
    except:
        return ''

def multiple_replace(t_text,tDict):
    mr_text = re.compile('|'.join(map(re.escape,tDict)))
    def one_xlat(match):
        return tDict[match.group(0)]
    return mr_text.sub(one_xlat,t_text)

handle_name = sys.argv[0].split(os.sep)[-1]
etl_id = sys.argv[1]
level_id = etl_id.split('.')[0]
table_id = etl_id.split('.')[1]

t_conf = conf(etl_id,handle_name)
t_uow = uow(t_conf)
t_log = log(t_conf)
t_cfg = cfg(t_conf)
t_db = db(t_conf)

uow_from = t_uow.getUOW_FROM(etl_id)
uow_to = t_uow.getUOW_TO(etl_id)

ex_sql_file = t_conf.FOLDER_SQL + os.sep + etl_id + '.sel.sql'
tmp_ex_sql_file = t_conf.FOLDER_TMP + os.sep + etl_id + '.sel.sql' + t_conf.f_time
ex_data_folder = t_conf.FOLDER_IN + os.sep + etl_id + os.sep + uow_to
ex_data_file = ex_data_folder + os.sep + etl_id + '.dat'
ex_data_record = ex_data_folder + os.sep + etl_id + '.record'

t_env = {}
t_env['{ETL_ID}'] = str(etl_id)
t_env['{LEVEL_ID}'] = str(level_id)
t_env['{TABLE_ID}'] = str(table_id)
t_env['{UOW_FROM}'] = str(uow_from)
t_env['{UOW_TO}'] = str(uow_to)

t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('ex_sql_file:' + ex_sql_file)
t_log.logger.info('tmp_ex_sql_file:' + tmp_ex_sql_file)
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))
t_log.logger.info('ex_data_folder:' + ex_data_folder)
t_log.logger.info('ex_data_file:' + ex_data_file)

file_type = t_cfg.FILE_TYPE

ex_dbc_file = t_conf.FOLDER_DBC + os.sep + t_cfg.EX_DBC_FILE
ex_db_type = t_cfg.EX_DB_TYPE

ld_db_type = t_cfg.LD_DB_TYPE
working_table = t_cfg.WORKING_TABLE
retention = t_cfg.RETENTION

t_db.setDBC(ex_dbc_file)

with open(ex_sql_file, 'rt') as f:
    t_content = f.read()

tmp_sql = multiple_replace(t_content,t_env)
tmp_sql = tmp_sql.replace('\n',' ').replace('\r',' ')
data_record = '0'

if ex_db_type == 'PG':
    tmp_sql = '\copy ('+ tmp_sql +') to \'' + ex_data_file + '\' with (format ' + file_type + ')'

    with open(tmp_ex_sql_file,'wt') as f:
        f.write(tmp_sql)

    cmd = 'psql -h ' + t_db.host + ' -p ' + t_db.port + ' -d '  + t_db.database + ' -v ON_ERROR_STOP=1 -U ' + t_db.user + ' -f ' + tmp_ex_sql_file
    t_log.logger.info('command: ' + cmd)
    t_log.logger.info(t_env)

    if not(os.path.exists(ex_data_folder)):
        os.makedirs(ex_data_folder)
    else: 
        shutil.rmtree(ex_data_folder)
        os.makedirs(ex_data_folder)

    sub = subprocess.Popen(cmd,cwd=t_conf.FOLDER_SQL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)

    while 1:
        line = sub.stdout.readline().decode('utf-8')
        if line == '':
            break
        if line.find('COPY ') == 0 :
            data_record = str(line.split(' ')[1])
        t_log.logger.info(line.strip())

returncode = sub.poll()

with open(ex_data_record,'wt') as f:
    f.write(data_record)
t_log.logger.info('extract_record:' + data_record)

if not(returncode == 0) :
    t_log.logger.error('extract executed false.')
    sys.exit(4)
else:
    t_log.logger.info('extract executed success.')
    sys.exit(0)
