# -*- coding: utf-8 -*-
import sys
import tushare as ts
from itertools import islice
from sqlalchemy import create_engine
import csv
reload(sys)
sys.setdefaultencoding("utf-8")


def get_industry():
    df = ts.get_industry_classified()
    # df.to_csv('./industry.csv')
    engine = create_engine('mysql://client:123456@115.159.46.93:3306/StocksAnalysis?charset=utf8')

    # 存入数据库
    df.to_sql('industry', engine)

    # 追加数据到现有表
    # df.to_sql('tick_data',engine,if_exists='append')


def get_history(id):
    df = ts.get_hist_data(id, start='2013-01-01', end='2015-01-01')
    # df.to_csv(id+'.csv')
    # history = ts.get_h_data('600848')
    engine = create_engine('mysql://client:123456@115.159.46.93:3306/StocksAnalysis?charset=utf8')

    # 存入数据库
    try:
        df.to_sql(id, engine, if_exists='append')
    except:
        cmd = 'CREATE INDEX ix_' + id + '_date ON `' + id + '` (date(20))'
        engine.execute(cmd)
        df.to_sql(id, engine, if_exists='append')


def get_all(id):
    df = ts.get_h_data(id)
    # df.to_csv(id+'.csv')
    # history = ts.get_h_data('600848')
    engine = create_engine('mysql://client:123456@115.159.46.93:3306/StocksAnalysis?charset=utf8')

    # 存入数据库
    try:
        df.to_sql(id, engine, if_exists='append')
    except:
        cmd = 'CREATE INDEX ix_' + id + '_date ON `' + id + '` (date(20))'
        engine.execute(cmd)
        df.to_sql(id, engine, if_exists='append')


def get_history_data():
    csvfile = file('history.csv')
    reader = csv.reader(csvfile)
    op = []
    high = []
    low = []
    close = []
    volume = []
    price_change = []
    turnover = []
    for line in islice(reader, 1, None):
        op.append(float(line[1]))
        high.append(float(line[2]))
        low.append(float(line[4]))
        close.append(float(line[3]))
        volume.append(float(line[5]))
        price_change.append(float(line[6]))
        turnover.append(float(line[14]))

    # print(op)
    # print(high)
    # print(low)
    # print(close)
    # print(volume)
    # print(price_change)
    # print(turnover)

# get_history('600000')
# get_all('600000')
#
get_industry()
# file = open("StockList.txt")
#
# while 1:
#     line = file.readline()
#     print line
#     get_history(line[2:8])
#     if not line:
#         break
