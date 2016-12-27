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

tmp_ld_sql_file = t_conf.FOLDER_TMP + os.sep + etl_id + '.ld.sql' + t_conf.f_time

t_env = {}
t_env['{ETL_ID}'] = str(etl_id)
t_env['{LEVEL_ID}'] = str(level_id)
t_env['{TABLE_ID}'] = str(table_id)
t_env['{UOW_FROM}'] = str(uow_from)
t_env['{UOW_TO}'] = str(uow_to)

t_log.logger.info('log file:' + t_conf.LOG_FILE)
t_log.logger.info(' '.join(sys.argv[0:]))
t_log.logger.info('tmp_ld_sql_file:' + tmp_ld_sql_file)
t_log.logger.info('UOW:' + str(uow_from) + ',' + str(uow_to))
#t_log.logger.info('ex_data_folder:' + ex_data_folder)
#t_log.logger.info('ex_data_file:' + ex_data_file)

file_type = t_cfg.FILE_TYPE

ex_dbc_file = t_conf.FOLDER_DBC + os.sep + t_cfg.EX_DBC_FILE
ex_db_type = t_cfg.EX_DB_TYPE

ld_db_type = t_cfg.LD_DB_TYPE
working_table = t_cfg.WORKING_TABLE
retention = int(t_cfg.RETENTION)
extract_etl_id = t_cfg.EXTRACT_ETL_ID
record_verification = int(t_cfg.RECORD_VERIFICATION)

ex_data_from_folder = t_conf.FOLDER_IN + os.sep + extract_etl_id + os.sep + uow_from
ex_data_to_folder = t_conf.FOLDER_IN + os.sep + extract_etl_id + os.sep + uow_to

print(ex_data_from_folder)
print(ex_data_to_folder)

ex_folders = {}
delete_folders = {}
t_ex_folders = os.listdir(t_conf.FOLDER_IN + os.sep + extract_etl_id)
t_ex_folders.sort(reverse=True)
print(t_ex_folders)
i = 0
for t_ex_f in t_ex_folders:
    t_ex_folder = t_conf.FOLDER_IN + os.sep + extract_etl_id + os.sep + str(t_ex_f)
    print(t_ex_folder)
    if os.path.isdir(t_ex_folder) and t_ex_folder > ex_data_from_folder and t_ex_folder <= ex_data_to_folder:
        ex_folders[t_ex_folder] = t_ex_folder
    if os.path.isdir(t_ex_folder) and t_ex_folder <= ex_data_to_folder:
        i = i + 1
    if i > retention:
        delete_folders[t_ex_folder] = t_ex_folder


#load
tmp_sql = 'truncate table ' + working_table + ';\n'
ex_record = 0


for ex_data_folder in ex_folders:
    ex_data_file = ex_data_folder + os.sep + extract_etl_id + '.dat'
    ex_data_record = ex_data_folder + os.sep + extract_etl_id + '.record'
    t_log.logger.info('extract_file:' + ex_data_file)
    if ex_db_type == 'PG':
        tmp_sql = tmp_sql + '\copy ' + working_table + ' from \'' + ex_data_file + '\' with (format ' + file_type + ');\n' 
    with open(ex_data_record,'rt') as f:
        t_content = f.read()
    t_log.logger.info('extract record:' + t_content)
    ex_record = ex_record + int(t_content)


data_record = 0
with open(tmp_ld_sql_file,'wt') as f:
        f.write(tmp_sql)

cmd = 'psql -h ' + t_db.host + ' -p ' + t_db.port + ' -d '  + t_db.database + ' -v ON_ERROR_STOP=1 -U ' + t_db.user + ' -f ' + tmp_ld_sql_file
t_log.logger.info('command: ' + cmd)
t_log.logger.info(t_env)

sub = subprocess.Popen(cmd,cwd=t_conf.FOLDER_SQL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)

while 1:
    line = sub.stdout.readline().decode('utf-8')
    if line == '':
        break
    if line.find('COPY ') == 0 :
        data_record = data_record + int(line.split(' ')[1])
    t_log.logger.info(line.strip())

returncode = sub.poll()

if not(returncode == 0) :
    t_log.logger.error('load executed false.')
    sys.exit(4)

t_log.logger.info('extracted records:' + str(ex_record) + ' ; loaded records:' + str(data_record))

if record_verification:
    if not(ex_record == data_record):
        t_log.logger.error('data verification false: extracted records not match loaded records')
        sys.exit(4)
    else:
        t_log.logger.info('data verification success.')

#retention
for del_folder in delete_folders:
    shutil.rmtree(del_folder)
    t_log.logger.info('deleted folder:' + del_folder)

t_log.logger.info('load executed success.')
