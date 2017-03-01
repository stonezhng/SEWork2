# -*- coding: UTF-8 -*-
import json
import urllib2

import datetime
import time

import re
import tushare as ts
import MySQLdb

def committee(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    print id
    shReg = "(600[0-9]{3}|601[0-9]{3}|900[0-9]{3}|603[0-9]{3})"
    szReg = "(000[0-9]{3}|002[0-9]{3}|300[0-9]{3}|200[0-9]{3}|001[0-9]{3})"
    stockid = ''
    if re.match(shReg, id):
        stockid = 'sh' + id
        # print id
    elif re.match(szReg, id):
        stockid = 'sz' + id
    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    start = end + datetime.timedelta(days=-8)
    ticks = []
    while start.strftime("%Y-%m-%d") != enddate:
        com1 = 0
        com2 = 0
        df = ts.get_tick_data(id, date=start.strftime("%Y-%m-%d"))
        # print df
        if str(df.values.tolist()[1][1]) != 'nan':
            # print (df.values.tolist())
            temp = df.values.tolist()[-5:]
            for each in temp:
            #     print each[5]
                if each[5] == '买盘':
                    com1 += 1
                elif each[5] == '卖盘':
                    com2 += 1
            result = [start.strftime("%Y-%m-%d"), float(com1 - com2)/float(com1 + com2), stockid]
            update_cmd = 'update `short_ticks` set `date` = "%s", `committee` = %s where `stockid` = "%s"' % tuple(ticks)
            # ticks.append(result)
            cursor.execute(update_cmd)
        start = start + datetime.timedelta(days=1)
    # print ticks
    # insert_cmd = 'insert into `short_ticks` (`stockid`, `date`, `committee`)' \
    #              'VALUES (%s, %s, %s)'
    # cursor.executemany(insert_cmd, ticks)
    # db.commit()
    db.close()


def quantity(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    start = end + datetime.timedelta(days=-15)
    startdate = start.strftime("%Y-%m-%d")

    select_cmd = 'select `stockid`, `date`, `volume` from `stock_2016` where `date` >= "' + startdate + '" and `date` <"' + enddate + '" and `stockid` = "' + id + '"'
    cursor.execute(select_cmd)
    volume = []
    date = []
    data = list(cursor.fetchall())
    for each in data:
        volume.append(each[2])
        date.append(each[1])
    # print volume
    volumerate = []
    # print date

    for x in range(4, len(volume)):
        base = float(volume[x - 4] + volume[x - 3] + volume[x - 2] + volume[x - 1] + volume[x])
        if base == 0:
            temp = 0
        else:
            temp = float(5 * volume[x]) / base
        volumerate.append(temp)
    volumerate = volumerate[-5:]
    # print date[-5:]
    for x in range(0, len(volumerate)):
        update_cmd = 'update `short_ticks` set `quantity_ratio` = ' + str(volumerate[x]) + ' where `stockid` ="'+id+'" ' \
                    'and `date` = "'+date[x+5]+'"'
        # print update_cmd
        cursor.execute(update_cmd)


starttime = time.clock()
# quantity('sh600000')
# db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
#                      port=8161, charset="utf8")
# cursor = db.cursor()
# cursor.execute('drop table `short_ticks`')
#
# cursor.execute("""
#     create table `short_ticks` (`stockid` varchar(40) NOT NULL, `date` varchar(40) NOT NULL, `committee` float,
#     `quantity_ratio` float, PRIMARY KEY (`stockid`, `date`))ENGINE=MyISAM DEFAULT CHARSET=utf8
#     """)
file = open('StockList.txt')
while 1:
    line = file.readline()
    if not line:
        break
    committee(line[2:8])
    print line[:8]
    quantity(line[:8])

endtime = time.clock()

print (endtime-starttime)


