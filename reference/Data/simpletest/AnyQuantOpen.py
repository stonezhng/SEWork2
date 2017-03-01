import urllib2
import json
import sys
import numpy as np
from scipy import stats
from sqlalchemy import create_engine
import MySQLdb
from pandas import DataFrame
import pandas as pd


def get_input():
    param = []
    for i in range(1, len(sys.argv)):
        param.append(sys.argv[i])
    return param


def stockOpen(param):
    url = "http://121.41.106.89:8010/api/stock/"+param[0]+"/?start="+param[2]+"&end="+param[3]
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']

    open = []

    for x in xrange(0, len(json_file)):
        open.append(json_file[x]['open'])

    return open


def stock(id):
    url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=2015-01-01&end=2016-05-01"
    # print url
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']

    engine = create_engine('mysql://client:123456@115.159.46.93:3306/StocksAnalysis?charset=utf8')
    # db = MySQLdb.connect(host="115.159.46.93", port=3306,
    #                      user='client', passwd="123456", db='StocksAnalysis', charset='utf8')
    # cursor = db.cursor()
    create_cmd = "CREATE TABLE `" + id + """` (
    `date` varchar(40) NOT NULL,
    `open` float,
    `high` float,
    `low` float,
    `close` float,
    `volume` int,
    `adj_price` float,
    `turnover` float,
    `pe_ttm` float,
    `pb` float,
    PRIMARY KEY(`date`)
    )ENGINE=MyISAM DEFAULT CHARSET=utf8;
    """

    engine.execute(create_cmd)

    for x in xrange(0, len(json_file)):
        insert_cmd = 'INSERT INTO `' + id + '`(date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb)' \
                                            'VALUES ("' + \
                     str(json_file[x]['date']) + '", ' + str(json_file[x]['open']) + ', ' + str(json_file[x]['high']) + ', ' + \
                     str(json_file[x]['low']) + ', ' + str(json_file[x]['close']) + ',' + str(json_file[x]['volume']) + ', ' + \
                     str(json_file[x]['adj_price']) + ', ' + \
                     str(json_file[x]['turnover']) + ', ' + str(json_file[x]['pe_ttm']) + ', ' + \
                     str(json_file[x]['pb']) + ')'
        engine.execute(insert_cmd)


def stock_pandas(id):
    url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=2010-01-01&end=2016-05-01"
    # print url
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    date = []
    open = []
    high = []
    low = []
    close = []
    volume = []
    adj_price = []
    turnover = []
    pe_ttm = []
    pb = []
    json_file = json.loads(res)['data']['trading_info']
    for x in xrange(0, len(json_file)):
        date.append(json_file[x]['date'])
        open.append(json_file[x]['open'])
        high.append(json_file[x]['high'])
        low.append(json_file[x]['low'])
        close.append(json_file[x]['close'])
        volume.append(json_file[x]['volume'])
        adj_price.append(json_file[x]['adj_price'])
        turnover.append(json_file[x]['turnover'])
        pe_ttm.append(json_file[x]['pe_ttm'])
        pb.append(json_file[x]['pb'])

    data = {'date': date,
            'open': open,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'adj_price': adj_price,
            'turnover': turnover,
            'pe_ttm': pe_ttm,
            'pb': pb}
    frame = DataFrame(data)
    engine = create_engine('mysql://client:123456@115.159.46.93:3306/StocksAnalysis?charset=utf8')
    engine.execute("DROP TABLE IF EXISTS `"+id+"`")
    frame.to_sql(id, engine)


def benchOpen(param):
    url = "http://121.41.106.89:8010/api/benchmark/" + param[1] + "/?start=" + param[2] + "&end=" + param[3]
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']

    open = []

    for x in xrange(0, len(json_file)):
        open.append(json_file[x]['open'])

    return open

# stock_pandas('sh600000')
file = open("StockList.txt")

while 1:
    line = file.readline()
    print line[:8]
    if not line:
        break
    stock_pandas(line[:8])
# stock_data = stockOpen(get_input())
# bench_data = benchOpen(get_input())
#
# # except_of_stock = np.mean(stock_data)
# # except_of_bencmark = np.mean(bench_data)
#
# stock_info = stats.describe(stock_data)
# bench_info = stats.describe(bench_data)
#
# cov_data = np.array([stock_data, bench_data])
# cov = np.cov(cov_data, bias=1)[0][1]

# url = "http://121.41.106.89:8010/api/stock/sh600000/?start=2015-04-01&end=2016-04-13"
# req = urllib2.Request(url)
# req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
# res_data = urllib2.urlopen(req)
# res = res_data.read()
# json_file = json.loads(res)['data']['trading_info']
# print json_file
# print bench_info[2]
# print bench_info[3]
# print bench_info[4]
# print bench_info[5]
# print stock_info[2]
# print stock_info[3]
# print stock_info[4]
# print stock_info[5]
# # print(cov)
# print(np.corrcoef(stock_data, bench_data, rowvar=0)[0][1])
# print(cov_origin/np.sqrt(np.var(stock_data)*np.var(bench_data)))
