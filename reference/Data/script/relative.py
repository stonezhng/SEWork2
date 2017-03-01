import urllib2
import json
import sys

import MySQLdb
import datetime
import numpy as np
import time
from scipy import stats

def create_relative():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    cursor.execute('drop table `relative`')
    cursor.execute("""
    create table `relative` (`stockid` varchar(40) not null, `benchid` VARCHAR(40) NOT NULL,
    `open_mean` float, `open_var` float, `open_skewness` float, `open_kurtosis` float, `open_corrcoef` float,
    `volume_mean` float, `volume_var` float, `volume_skewness` float, `volume_kurtosis` float, `volume_corrcoef` float,
    `devia_mean` float, `devia_var` float, `devia_skewness` float, `devia_kurtosis` float, `devia_corrcoef` float,
     `corrcoef` float, PRIMARY KEY (`stockid`))ENGINE=MyISAM DEFAULT CHARSET=utf8
    """)
    db.close()


def calc(id):
    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    start = end + datetime.timedelta(days=-8)
    startdate = start.strftime("%Y-%m-%d")
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    select_cmd = 'select `open`, `volume`, `deviation_per`, `amount` from `stock_2016` where `stockid` = "'+id+'" ' \
                    ' and `date` >= "'+startdate+'" and `date` < "'+enddate+'"'
    cursor.execute(select_cmd)
    data = list(cursor.fetchall())
    stock_open = []
    stock_volume = []
    stock_devia = []
    amount = []
    for each in data:
        stock_open.append(each[0])
        stock_volume.append(each[1])
        stock_devia.append(each[2])
        amount.append(each[3])
    stock_open = stock_open[-5:]
    stock_volume = stock_volume[-5:]
    stock_devia = stock_devia[-5:]
    amount = amount[-5:]
    select_cmd = 'select `open`, `volume`, `deviation_per` from `bench` where `stockid` = "hs300" ' \
                    ' and `date` >= "' + startdate + '" and `date` < "' + enddate + '"'
    cursor.execute(select_cmd)
    data = list(cursor.fetchall())
    bench_open = []
    bench_volume = []
    bench_devia = []
    for each in data:
        bench_open.append(each[0])
        bench_volume.append(each[1])
        bench_devia.append(each[2])
    bench_open = bench_open[-5:]
    bench_volume = bench_volume[-5:]
    bench_devia = bench_devia[-5:]
    if 0 not in amount:
        open_info = stats.describe(stock_open)[2:]
        open_re = np.corrcoef(stock_open, bench_open, rowvar=0)[0][1]
    else:
        open_info = ['null', 'null', 'null', 'null']
        open_re = 'null'

    if 0 not in amount:
        volume_info = stats.describe(stock_volume)[2:]
        volume_re = np.corrcoef(stock_volume, bench_volume, rowvar=0)[0][1]
    else:
        volume_info = ['null', 'null', 'null', 'null']
        volume_re = 'null'

    if 0 not in amount:
        devia_info = stats.describe(stock_devia)[2:]
        devia_re = np.corrcoef(stock_devia, bench_devia, rowvar=0)[0][1]
    else:
        devia_info = ['null', 'null', 'null', 'null']
        devia_re = 'null'
    # volume_info = stats.describe(stock_volume)
    # devia_info = stats.describe(stock_devia)

    # volume_re = np.corrcoef(stock_volume, bench_volume, rowvar=0)[0][1]
    # devia_re = np.corrcoef(stock_devia, bench_devia, rowvar=0)[0][1]
    if open_re == 'null' or volume_re == 'null' or devia_re == 'null':
        re = 'null'
    else:
        re = float(open_re+volume_re+devia_re)/float(3)

    val = ("'"+id+"'", '"hs300"', open_info[0], open_info[1], open_info[2], open_info[3], open_re,
           volume_info[0], volume_info[1], volume_info[2], volume_info[3], volume_re,
           devia_info[0], devia_info[1], devia_info[2], devia_info[3], devia_re, re)

    insert_cmd = """
    insert into `relative`(`stockid`, `benchid`,
    `open_mean`, `open_var`, `open_skewness`, `open_kurtosis`, `open_corrcoef`,
    `volume_mean`, `volume_var`, `volume_skewness`, `volume_kurtosis`, `volume_corrcoef`,
    `devia_mean`, `devia_var`, `devia_skewness`, `devia_kurtosis`, `devia_corrcoef`,
     `corrcoef`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """ % val
    # print insert_cmd

    cursor.execute(insert_cmd)
    db.commit()
    db.close()


create_relative()
file = open('StockList.txt')
while 1:
    line = file.readline()
    if not line:
        break
    print line[:8]
    calc(line[:8])


# stock_data = stockOpen(get_input())
# bench_data = benchOpen(get_input())
#
# except_of_stock = np.mean(stock_data)
# except_of_bencmark = np.mean(bench_data)
#
# stock_info = stats.describe(stock_data)
# bench_info = stats.describe(bench_data)
#
# cov_data = np.array([stock_data, bench_data]).T
# cov = np.cov(cov_data, bias=0)[0][1]
#
# cov_data_origin = []
# for x in xrange(0, len(stock_data)):
#     cov_data_origin.append(stock_data[x]*bench_data[x])
# cov_origin = np.mean(cov_data_origin) - except_of_stock*except_of_bencmark
#
# print(bench_info[2])
# print(bench_info[3])
# print(bench_info[4])
# print bench_info[5]
# print(stock_info[2])
# print(stock_info[3])
# print stock_info[4]
# print stock_info[5]
# # print(cov)
# # print(cov_origin)
# print(np.corrcoef(stock_data, bench_data, rowvar=0)[0][1])
# # print(cov_origin/np.sqrt(np.var(stock_data)*np.var(bench_data)))
