# -*- coding: UTF-8 -*-
import json
import urllib2
import math
import MySQLdb
import time

import datetime


def count():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    for x in range(1, 62):
        select_cmd = 'select count(*) from `stock_evaluate` where `industryid` = '+str(x)
        cursor.execute(select_cmd)
        count = list(cursor.fetchall())[0][0]
        # print count
        update_cmd = 'update `industry_tag` set `count` = '+str(count)+' where `industryid` = '+str(x)
        cursor.execute(update_cmd)

    for x in range(1, 62):
        select_cmd = 'select `count` from `industry_tag` where `industryid` = '+str(x)
        cursor.execute(select_cmd)
        count = list(cursor.fetchall())[0][0]
        update_cmd = 'update `stock_evaluate` set `count` = '+str(count)+' where `industryid` = '+str(x)
        cursor.execute(update_cmd)

count()

