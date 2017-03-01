import json
import urllib
import urllib2
from string import strip

from lxml import etree

import MySQLdb
import datetime
import time
import numpy as np
import talib
import tushare as ts


def refresh_s(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    cursor.execute('select MAX(`date`) from `bench` where `stockid`= "' + id + '"')

    startdate = str(cursor.fetchall()[0])

    startdate = startdate[2:12]
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    start = start + datetime.timedelta(days=1)
    startdate = start.strftime("%Y-%m-%d")
    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    end = end + datetime.timedelta(days=1)
    enddate = end.strftime("%Y-%m-%d")

    data1 = []

    url = "http://121.41.106.89:8010/api/benchmark/" + id + "/?start=" + startdate + "&end=" + enddate
    # print id
    print url
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']

    for x in range(0, len(json_file)):
        temp = [id, str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
                float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
                float(json_file[x]['adj_price'])]
        data1.append(temp)
    # print data1

    insert_cmd = 'INSERT INTO `bench` ' \
                         '(`stockid`, `date`, open, high, low, close, volume, ' \
                         'adj_price) ' \
                         'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, data1)
    db.commit()
    db.close()


def refresh_sinabench(id):
    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    # end = end + datetime.timedelta(days=1)
    enddate = end.strftime("%Y-%m-%d")
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    cursor.execute('select MAX(`date`) from `bench` where `stockid`= "' + id + '"')
    print 'select MAX(`date`) from `bench` where `stockid`= "' + id + '"'
    startdate = str(cursor.fetchall()[0][0])
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    start = start + datetime.timedelta(days=1)
    startdate = start.strftime("%Y-%m-%d")

    print startdate
    print enddate

    if id == 'sh000001':
        df = ts.get_h_data('000001', start=startdate, end=enddate, index=True)
    elif id == 'sz399001':
        df = ts.get_h_data('399001', start=startdate, end=enddate, index=True)
    raw = df.to_dict(orient='dict')
    data = []
    for each in raw['open']:
        temp = [id, str(each)[0:10]]
        data.append(temp)

    for x in range(0, len(df[['open', 'high', 'low', 'close', 'volume', 'amount']].values)):
        data[x].extend(df[['open', 'high', 'low', 'close', 'volume', 'amount']].values[x])

    # print data
    insert_cmd = 'INSERT INTO `bench` ' \
                     '(`stockid`, `date`, open, high, low, close, volume, `amount`) ' \
                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()


def refresh_talib(id):
    # high = []
    # low = []
    # close = []
    # date = []
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    end = end + datetime.timedelta(days=1)
    enddate = end.strftime("%Y-%m-%d")

    cursor.execute('select MIN(`date`) from `bench` where `stockid`= "' + id + '" and `dif` is null')

    startdate = str(cursor.fetchall()[0][0])

    # startdate = '2010-01-04'
    # print startdate
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")

    startprime = start + datetime.timedelta(days=-800)
    startdateprime = startprime.strftime("%Y-%m-%d")
    #
    # url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=" + startdate + "&end=" + enddate
    # # print url
    # req = urllib2.Request(url)
    # req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    # res_data = urllib2.urlopen(req)
    # res = res_data.read()
    # json_file = json.loads(res)['data']['trading_info']
    # for x in range(0, len(json_file)):
    #     if json_file[x]['turnover'] != 0:
    #         high.append(json_file[x]['high'])
    #         low.append(json_file[x]['low'])
    #         close.append(json_file[x]['close'])
    #         date.append(json_file[x]['date'])
    high = []
    low = []
    close = []
    date = []
#  ' select `date`, `close`, `high`, `low`, `amount` from `stock_2013` where `stockid` = "' + id + '"  ' \
#                     ' and `date` < "' + enddate + '"union ' \
    select_cmd =' select `date`, `close`, `high`, `low` from `bench` where `stockid` = "' + id +'" ' \
                'and `date` > "'+startdateprime+'" and date < "'+enddate+'" order by `date`'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    # print raw
    for each in raw:
        date.append(each[0])
        close.append(each[1])
        high.append(each[2])
        low.append(each[3])
    # print close
    dif, dea, macd = talib.MACD(np.array(close), fastperiod=12, slowperiod=26, signalperiod=9)
    slowK, slowD = talib.STOCH(np.array(high), np.array(low), np.array(close), fastk_period=9, slowk_period=3,
                                       slowk_matype=0, slowd_period=3, slowd_matype=0)
    boll_upper, boll_middle, boll_low = talib.BBANDS(np.array(close), timeperiod=15, nbdevup=1, nbdevdn=1,
                                                             matype=0)
    ma5 = talib.MA(np.array(close), timeperiod=5, matype=0)
    ma10 = talib.MA(np.array(close), timeperiod=10, matype=0)
    ma20 = talib.MA(np.array(close), timeperiod=20, matype=0)
    ma30 = talib.MA(np.array(close), timeperiod=30, matype=0)
    ma60 = talib.MA(np.array(close), timeperiod=60, matype=0)
    atr = talib.ATR(np.array(high), np.array(low), np.array(close), timeperiod=14)
    rsi6 = talib.RSI(np.array(close), timeperiod=6)
    rsi12 = talib.RSI(np.array(close), timeperiod=12)
    rsi24 = talib.RSI(np.array(close), timeperiod=24)

    dif = list(dif)
    dea = list(dea)
    macd = list(macd)
    slowK = list(slowK)
    slowD = list(slowD)
    boll_upper = list(boll_upper)
    boll_middle = list(boll_middle)
    boll_low = list(boll_low)
    ma5 = list(ma5)
    ma10 = list(ma10)
    ma20 = list(ma20)
    ma30 = list(ma30)
    ma60 = list(ma60)
    atr = list(atr)
    rsi6 = list(rsi6)
    rsi12 = list(rsi12)
    rsi24 = list(rsi24)

    # print date

    data2 = []
    temp = []
    for x in range(0, len(date)):
        if date[x] >= startdate and date[x] < enddate:
            temp.append(date[x])
            if str(dif[x]) != 'nan':
                temp.append(dif[x])
            else:
                temp.append('null')
            if str(dea[x]) != 'nan':
                temp.append(dea[x])
            else:
                temp.append('null')
            if str(macd[x]) != 'nan':
                temp.append(macd[x])
            else:
                temp.append('null')
            if str(slowK[x]) != 'nan':
                temp.append(slowK[x])
            else:
                temp.append('null')
            if str(slowD[x]) != 'nan':
                temp.append(slowD[x])
            else:
                temp.append('null')
            if str(slowD[x]) != 'nan':
                temp.append(3 * slowD[x] - 2 * slowK[x])
            else:
                temp.append('null')
            if str(boll_upper[x]) != 'nan':
                temp.append(boll_upper[x])
            else:
                temp.append('null')
            if str(boll_middle[x]) != 'nan':
                temp.append(boll_middle[x])
            else:
                temp.append('null')
            if str(boll_low[x]) != 'nan':
                temp.append(boll_low[x])
            else:
                temp.append('null')
            if str(ma5[x]) != 'nan':
                temp.append(ma5[x])
            else:
                temp.append('null')
            if str(ma10[x]) != 'nan':
                temp.append(ma10[x])
            else:
                temp.append('null')
            if str(ma20[x]) != 'nan':
                temp.append(ma20[x])
            else:
                temp.append('null')
            if str(ma30[x]) != 'nan':
                temp.append(ma30[x])
            else:
                temp.append('null')
            if str(ma60[x]) != 'nan':
                temp.append(ma60[x])
            else:
                temp.append('null')
            if str(atr[x]) != 'nan':
                temp.append(atr[x])
            else:
                temp.append('null')
            if str(rsi6[x]) != 'nan':
                temp.append(rsi6[x])
            else:
                temp.append('null')
            if str(rsi12[x]) != 'nan':
                temp.append(rsi12[x])
            else:
                temp.append('null')
            if str(rsi24[x]) != 'nan':
                temp.append(rsi24[x])
            else:
                temp.append('null')
            if x == 0:
                temp.append('null')
                temp.append('null')
            else:
                temp.append(close[x] - close[x - 1])
                temp.append(float(close[x] - close[x - 1]) / float(close[x - 1]))
                if float(close[x] - close[x - 1]) / float(close[x - 1]) > 0.1:
                    print close[x]
                    print close[x-1]
                    print date[x]
            data2.append(temp)
            # print temp
            temp = []

##################################################################################
    # print data2
    for x in range(0, len(data2)):
        temp = data2[x][1:]
        temp.append(id)
        temp.append(data2[x][0])
        update_cmd = """
                        update `bench` set `dif` = %s, `dea` = %s, `macd` = %s, `slowK` = %s, `slowD` = %s,
                        `slowJ` = %s, `boll_upper` = %s, `boll_middle` = %s, `boll_low` = %s, `ma5` = %s, `ma10` = %s, `ma20` = %s,
                        `ma30` = %s, `ma60` = %s, `atr` = %s, `rsi6` = %s, `rsi12` = %s, `rsi24` = %s, `deviation_val` = %s, `deviation_per` = %s
                        where `stockid` = "%s" and `date` = "%s"
                        """ % tuple(temp)
        # print update_cmd
        cursor.execute(update_cmd)
# try:
#     refresh_sinabench('sz399001')
#     refresh_talib('sz399001')
#     refresh_sinabench('sh000001')
#     refresh_talib('sh000001')
# except Exception, e:
#     print e
refresh_sinabench('sz399001')
refresh_talib('sz399001')
refresh_sinabench('sh000001')
refresh_talib('sh000001')
refresh_s('hs300')
refresh_talib('hs300')