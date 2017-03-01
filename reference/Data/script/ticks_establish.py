# -*- coding: utf-8 -*-
import datetime

import MySQLdb
import tushare as ts
from gmsdk import md


def create_ticks():
    ticks = md.get_ticks("SHSE.600000",
                         "2016-05-07 9:30:00",
                         "2016-05-07 15:00:00")
    print ticks[0]


def create_tushare_ticks(id):
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "StocksAnalysis")
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis", port=8161)
    cursor = db.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS `" + id + '`')
    except:
        1
#  time       price change  volume  amount  type
    create_cmd = "CREATE TABLE `" + id + """`(
        `index` int NOT NULL AUTO_INCREMENT,
        `time` varchar(60) NOT NULL,
        `price` float,
        `change` text,
        `volume` bigint,
        `amount` bigint,
        `type` text,
        PRIMARY KEY(`index`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    data = []
    cursor.execute(create_cmd)
    starttime = datetime.datetime(2010, 01, 01)
    endtime = datetime.datetime(2016, 05, 07)

    while (starttime - endtime).days != 0:
        df = ts.get_tick_data(id[2:], str(starttime)[0:10])
        # if str(df.values[0][0]) == 'alert("当天没有数据");':
        #     temp = (str(starttime), None, None, None, None, None)
        #     data.append(temp)
        #     print id + str(starttime) + ' Null'
        # else:
        # print id + ' ' + str(starttime)[0:10]
        for row in df.values:
            if str(row[0])=='nan' or str(row[1])=='nan' or str(row[2])=='nan' or str(row[3])=='nan' or str(row[4])=='nan' or str(row[5])=='nan':
                temp = (str(starttime), None, None, None, None, None)
                data.append(temp)
                # print id + str(starttime) + ' Null'
                break
            else:
                temp = ((str(starttime)[0:10] + " " + row[0]), row[1], row[2], row[3], row[4], row[5])
                # print id + (str(starttime)[0:10] + " " + row[0])
                data.append(temp)
        starttime = starttime + datetime.timedelta(days=1)
    insert_cmd = 'INSERT INTO `' + id + '` (`time`, `price`, `change`, `volume`, `amount`, `type`) VALUES (%s, %s, %s, %s, %s, %s)'

    count = 1000
    data = tuple(data)
    while count < len(data):
        # print data[count - 1000]
        cursor.executemany(insert_cmd, data[count - 1000: count])
        db.commit()
        count += 1000
    cursor.executemany(insert_cmd, data[count:])
    db.commit()
    db.close()

# file = open('StockList.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     create_tushare_ticks(line[:8])
#
starttime = datetime.datetime.now()
create_ticks()
endtime = datetime.datetime.now()
print 'running time: '
print str((endtime-starttime).seconds) + ' seconds'
# print str(df.values[1][4]) == 'nan'
