# -*- coding: UTF-8 -*-
import urllib
import urllib2
import json
from string import strip

import time

import datetime
from lxml import etree

import tushare as ts
import sys
import numpy as np
from scipy import stats
from sqlalchemy import create_engine
import MySQLdb
from pandas import DataFrame
import pandas as pd
import talib
from sklearn import linear_model
import numpy as np
import requests

#
# url = "http://121.41.106.89:8010/api/benchmark/hs300/?start=2010-01-01&end=2010-12-31"
# req = urllib2.Request(url)
# req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
# res_data = urllib2.urlopen(req)
# res = res_data.read()
# json_file = json.loads(res)['data']['trading_info']
# print json_file

# data = ts.get_today_all('600000')
# print data

# url = "http://121.41.106.89:8010/api/stock/sh600000/?start=2015-01-01&end=2016-12-31"
# req = urllib2.Request(url)
# req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
# res_data = urllib2.urlopen(req)
# res = res_data.read()
# high = []
# low = []
# close = []
# date = []
# json_file = json.loads(res)['data']['trading_info']
# for row in json_file:
#     if row['volume'] != 0:
#         date.append(row['date'])
#         close.append(row['close'])
#         high.append(row['high'])
#         low.append(row['low'])
# print len(close)
# ma60 = talib.MA(np.array(close), timeperiod=60, matype=0)
# print list(ma60)

#
# db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
#                      port=8161)
# cursor = db.cursor()
# # select_cmd = 'select * from `sh600117_Analysis` where date >= "2010-01-01" and date <= "2010-12-31"'
# file = open('StockList.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     drop_cmd = 'delete from `season` where ``'
#     cursor.execute(drop_cmd)

# url = 'https://api.wmcloud.com:443/data/v1/' \
#       'api/equity/getEqu.json?field=listStatusCD,listDate,delistDate,primeOperating,secFullName' \
#       '&ticker=000001&secID=&equTypeCD=&listStatusCD='
# req = urllib2.Request(url)
# req.add_header("Authorization", 'Bearer 4c81816b40a4dbcc659f0017b81482281bf220618d616ac81c3287697fc0e755')
# res_data = urllib2.urlopen(req)
# res = res_data.read()
# print res
#
# file = open('StockList.txt')
#
# while 1:
#     line = file.readline()
#     if not line:
#         break


# from gmsdk import md
# ret = md.init("18351892367", "141250191")
# ticks = md.get_ticks("SHSE.600000",
#                      "2015-10-28 9:30:00",
#                      "2015-10-29 15:00:00")
# print ticks

# url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol=sh600000&date=2016-05-05'
# response = requests.get(url)
# print response.body
from bs4 import BeautifulSoup
# import urllib
# from lxml import etree
#
# url = 'http://gupiao.baidu.com/stock/sh600000.html'
# response = urllib.urlopen(url).read()
# tree = etree.HTML(response)
#
# open = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[1]/dd')[0].text)
# print open
#
# volume = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[2]/dd')[0].text)
# volume = float(volume[:-2])*10000
# print volume
#
# high = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[3]/dd')[0].text)
# print high
#
# up_stop = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[4]/dd')[0].text)
# print up_stop
#
# inner_count = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[5]/dd')[0].text)
# inner_count = float(inner_count[:-2])*10000
# print inner_count
#
# amount = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[6]/dd')[0].text)
# amount = float(amount[:-1])*10000
# print amount
#
# committee = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[7]/dd')[0].text)
# committee = float(committee[:-1])*0.01
# print committee
#
# value = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[8]/dd')[0].text)
# value = float(value[:-1])
# print value
#
# pe = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[9]/dd')[0].text)
# print pe
#
# profit_per = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[10]/dd')[0].text)
# print profit_per
#
# total_volume = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[1]/dl[11]/dd')[0].text)
# total_volume = float(total_volume[:-1])*100000000
# print total_volume
#
# close = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[1]/dd')[0].text)
# print close
#
# turnover = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[2]/dd')[0].text)
# turnover = float(turnover[:-1])*0.01
# print turnover
#
# low = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[3]/dd')[0].text)
# print low
#
# down_stop = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[4]/dd')[0].text)
# print down_stop
#
# outer_count = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[5]/dd')[0].text)
# outer_count = float(outer_count[:-2])*10000
# print outer_count
#
# amplitude = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[6]/dd')[0].text)
# amplitude = float(amplitude[:-1])*0.01
# print amplitude
#
# quantityratio = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[7]/dd')[0].text)
# print quantityratio
#
# total_amount = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[8]/dd')[0].text)
# total_amount = float(total_amount[:-1])
# print total_amount
#
# pb = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[9]/dd')[0].text)
# print pb
#
# value_per_stock = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[10]/dd')[0].text)
# print value_per_stock
#
# available_stock = strip(tree.xpath('//*[@id="app-wrap"]/div[2]/div/div[2]/div[2]/dl[11]/dd')[0].text)
# available_stock = float(available_stock[:-1])*100000000
# print available_stock
#
# #  外盘是卖，内盘是买
#
# url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol=sz300208&date=2016-05-17'
# url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol=sh600000&date=2016-05-17'
# response = urllib.urlopen(url).read()
# # print response
# tree = etree.HTML(response)
# value = tree.xpath('//*[@id="quote_area"]')
# print value
# print soup.findAll('//*[@id="quote_area"]/div[2]/table/tr[8]/td[2]/')
# df = ts.get_tick_data('600000', '2016-05-22')
# print df
# enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
# end = datetime.datetime.now()
# enddate = end.strftime('%Y-%m-%d')
# start = end + datetime.timedelta(days=-15)
# startdate = start.strftime("%Y-%m-%d")
# print startdate
# print end.DayOfWeek.ToString()
# url = 'http://market.finance.sina.com.cn/downxls.php?date=2011-07-08&symbol=sh000001'
# response = requests.get(url)
# print response

# x = [1, 2, 3, 4]
# y = [2, 4, 6, 8]
# X = np.array([x]).T
# regr = linear_model.LinearRegression()
# regr.fit(X, y)
# print regr.coef_[0]

# df = ts.get_h_data('399001', start='2016-05-28', end='2016-06-02', index=True)
# print type(df)
# print df.to_dict(orient='dict')
# # print df.tolist()
# raw = df.to_dict(orient='dict')
# # date = []
# # # print raw['open']
# for each in raw['open']:
#     print str(each)[0:10]

# for each in df[['open', 'high', 'low', 'close', 'volume']].values:
#     for value in each:
#         print value
# r = requests.get('https://xueqiu.com/statuses/stock_timeline.json?symbol_id=sh600000&count=10&source=%E7%A0%94%E6%8A%A5&page=1')
# print r.content
#
# df = ts.get_tick_data('002644', date='2016-06-16', retry_count=1)
# print df

df = ts.get_h_data('399001', start='2016-06-26', end='2016-06-30', index=True)
print df