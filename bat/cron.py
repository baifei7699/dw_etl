#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#source : https://github.com/idning/pcl/blob/master/pcl/crontab.py
#file   : cron.py
#author : bofei
#date   : 2017-02-10

import time
#import logging
#import thread
from datetime import datetime

class cron:

    #def __init__():

    #support: 
    # * 
    # 59
    # 10,20,30
    def _match(self, value, expr):
        #print('match', value, expr)
        if expr == '*':
            return True
        values = expr.split(',')
        for v in values:
            if int(v) == value:
                return True
        return False

    def matchtime(self, desc,t):
        mins, hour, day, month, dow = desc.split()
        return self._match(t.minute       , mins)  and\
               self._match(t.hour         , hour)  and\
               self._match(t.day          , day)   and\
               self._match(t.month        , month) and\
               self._match(t.isoweekday() , dow)
