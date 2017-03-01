import json
import urllib
import urllib2
from lxml import etree

import MySQLdb
import datetime
import time
import numpy as np
import talib


def refresh_s(id, startdate, enddate):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    # cursor.execute('select MAX(`date`) from `stock_2016` where `stockid`= "' + id + '"')
    #
    # startdate = str(cursor.fetchall()[0])
    #
    # startdate = startdate[2:12]
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    # start = start + datetime.timedelta(days=1)
    startprime = start
    # startdate = start.strftime("%Y-%m-%d")
    # enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    amount = []
    while str(startprime.strftime("%Y-%m-%d")) != str(enddate):
        url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol='+id+'&date='+\
          startprime.strftime("%Y-%m-%d")
        print url
        response = urllib.urlopen(url).read()
        tree = etree.HTML(response)
        value = tree.xpath('//*[@id="quote_area"]/div[2]/table/tr[8]/td[2]')[0].text
        temp = []
        temp.append(startprime.strftime("%Y-%m-%d"))
        value = int(float(value)*1000)
        temp.append(value)
        amount.append(temp)
        startprime = startprime + datetime.timedelta(days=1)


    data1 = []

    url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=" + startdate + "&end=" + enddate
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
                float(json_file[x]['adj_price']), float(json_file[x]['turnover']),
                float(json_file[x]['pe_ttm']), float(json_file[x]['pb'])]
        data1.append(temp)

    j = 0
    for x in range(0, len(data1)):
            # print str(amount[j][1])
        while str(amount[j][0]) != str(data1[x][1]):
            j += 1
        data1[x].append(amount[j][1])

################################################################################

    high = []
    low = []
    close = []
    date = []

    select_cmd = ' select `date`, `close`, `high`, `low`, `amount` from `stock_2013` where `stockid` = "' + id + '"  union ' \
                ' select `date`, `close`, `high`, `low`, `amount` from `stock_2014` where `stockid` = "' + id + '"  union ' \
                ' select `date`, `close`, `high`, `low`, `amount` from `stock_2015` where `stockid` = "' + id + '"  union ' \
                ' select `date`, `close`, `high`, `low`, `amount` from `stock_2016` where `stockid` = "' + id + '" ' \
                ' and `date` < "'+enddate+'"'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    for each in raw:
        if each[4] != 0:
            date.append(each[0])
            close.append(each[1])
            high.append(each[2])
            low.append(each[3])
    # url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=" + starttime + "&end=" + enddate
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
    # print len(close)
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
    # print dif
    data2 = []
    temp = []
    for x in range(0, len(date)):
        if date[x] >= startdate and date[x] <= enddate:
            # print date[x]
            temp.append(date[x])
            if str(dif[x]) != 'nan':
                temp.append(dif[x])
            else:
                temp.append(None)
            if str(dea[x]) != 'nan':
                temp.append(dea[x])
            else:
                temp.append(None)
            if str(macd[x]) != 'nan':
                temp.append(macd[x])
            else:
                temp.append(None)
            if str(slowK[x]) != 'nan':
                temp.append(slowK[x])
            else:
                temp.append(None)
            if str(slowD[x]) != 'nan':
                temp.append(slowD[x])
            else:
                temp.append(None)
            if str(slowD[x]) != 'nan':
                temp.append(3 * slowD[x] - 2 * slowK[x])
            else:
                temp.append(None)
            if str(boll_upper[x]) != 'nan':
                temp.append(boll_upper[x])
            else:
                temp.append(None)
            if str(boll_middle[x]) != 'nan':
                temp.append(boll_middle[x])
            else:
                temp.append(None)
            if str(boll_low[x]) != 'nan':
                temp.append(boll_low[x])
            else:
                temp.append(None)
            if str(ma5[x]) != 'nan':
                temp.append(ma5[x])
            else:
                temp.append(None)
            if str(ma10[x]) != 'nan':
                temp.append(ma10[x])
            else:
                temp.append(None)
            if str(ma20[x]) != 'nan':
                temp.append(ma20[x])
            else:
                temp.append(None)
            if str(ma30[x]) != 'nan':
                temp.append(ma30[x])
            else:
                temp.append(None)
            if str(ma60[x]) != 'nan':
                temp.append(ma60[x])
            else:
                temp.append(None)
            if str(atr[x]) != 'nan':
                temp.append(atr[x])
            else:
                temp.append(None)
            if str(rsi6[x]) != 'nan':
                temp.append(rsi6[x])
            else:
                temp.append(None)
            if str(rsi12[x]) != 'nan':
                temp.append(rsi12[x])
            else:
                temp.append(None)
            if str(rsi24[x]) != 'nan':
                temp.append(rsi24[x])
            else:
                temp.append(None)
            if x == 0:
                temp.append(None)
                temp.append(None)
            else:
                temp.append(close[x] - close[x - 1])
                temp.append(float(close[x] - close[x - 1]) / float(close[x - 1]))
            data2.append(tuple(temp))
            # print temp
            temp = []

##################################################################################
    i = 0
    for x in range(0, len(data1)):
        if i >= len(data2):
            data1[x].extend(
                ['null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null',
                 'null', 'null', 'null', 'null', 'null', 'null', 'null'])
        elif str(data1[x][1]) == str(data2[i][0]):
            data1[x].extend(data2[i][1:])
            i += 1
        else:
            data1[x].extend(['null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null'])
        da = data1[x][2:]
        da.extend(data1[x][0:2])
        print da
        update_cmd = """
                update `stock_2016` set  `open` = %s, `high` = %s, `low` = %s, `close` = %s, `volume` = %s, `adj_price` = %s,
                `turnover` = %s, `pe_ttm` = %s, `pb` = %s, `amount` = %s, `dif` = %s, `dea` = %s, `macd` = %s, `slowK` = %s, `slowD` = %s,
                `slowJ` = %s, `boll_upper` = %s, `boll_middle` = %s, `boll_low` = %s, `ma5` = %s, `ma10` = %s, `ma20` = %s,
                `ma30` = %s, `ma60` = %s, `atr` = %s, `rsi6` = %s, `rsi12` = %s, `rsi24` = %s, `deviation_val` = %s, `deviation_per` = %s
                where `stockid` = "%s" and `date` = "%s"
                """ % tuple(da)
        cursor.execute(update_cmd)
    # print data1

    # insert_cmd = 'INSERT INTO `stock_2016` ' \
    #                                             '(`stockid`, `date`, open, high, low, close, volume, ' \
    #                                             'adj_price, turnover, pe_ttm, pb,  `amount`, ' \
    #                                             '`dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
    #                                             '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,' \
    #                                             '`deviation_val`, `deviation_per`) ' \
    #                                             'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    # cursor.executemany(insert_cmd, data1)
    # db.commit()
    db.close()

# file = open('list.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     refresh_s(line[:8], '2016-05-17', '2016-05-27')

def add_data_rsi(id):
    # db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
    #                      port=8161)
    db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
                         port=3306, charset="utf8")
    cursor = db.cursor()

    date = []
    close = []

    select_cmd = ' select `date`, `close`, `amount` from `stock_2010` where `stockid` = "' + id + '"  union ' \
        ' select `date`, `close`, `amount` from `stock_2011` where `stockid` = "' + id + '"  union ' \
                ' select `date`, `close`, `amount` from `stock_2012` where `stockid` = "' + id + '"  union ' \
        ' select `date`, `close`, `amount` from `stock_2013` where `stockid` = "' + id + '"  union ' \
                ' select `date`, `close`, `amount` from `stock_2014` where `stockid` = "' + id + '"  union ' \
                ' select `date`, `close`, `amount` from `stock_2015` where `stockid` = "' + id + '"  union ' \
                ' select `date`, `close`, `amount` from `stock_2016` where `stockid` = "' + id + '" '
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    for each in raw:
        if each[2] != 0:
            date.append(each[0])
            close.append(each[1])
    rsi6 = talib.RSI(np.array(close), timeperiod=6)
    rsi12 = talib.RSI(np.array(close), timeperiod=12)
    rsi24 = talib.RSI(np.array(close), timeperiod=24)
    rsi6 = list(rsi6)
    rsi12 = list(rsi12)
    rsi24 = list(rsi24)

    add_data2010 = []
    add_data2011 = []
    add_data2012 = []
    add_data2013 = []
    add_data2014 = []
    add_data2015 = []
    add_data2016 = []

    for x in range(0, len(date)):
        temp = []
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
        temp.append(id)
        temp.append(date[x])
        if date[x][0:4] == '2010':
            add_data2010.append(temp)
        elif date[x][0:4] == '2011':
            add_data2011.append(temp)
        elif date[x][0:4] == '2012':
            add_data2012.append(temp)
        elif date[x][0:4] == '2013':
            add_data2013.append(temp)
        elif date[x][0:4] == '2014':
            add_data2014.append(temp)
        elif date[x][0:4] == '2015':
            add_data2015.append(temp)
        elif date[x][0:4] == '2016':
            add_data2016.append(temp)
    for each in add_data2010:
        update_cmd = 'update `stock_2010` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s ' \
                     'where `stockid` = "%s" and `date` = "%s"' % tuple(each)
        cursor.execute(update_cmd)
    for each in add_data2011:
        update_cmd = 'update `stock_2011` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s ' \
                         'where `stockid` = "%s" and `date` = "%s"' % tuple(each)
        cursor.execute(update_cmd)
    for each in add_data2012:
        update_cmd = 'update `stock_2012` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s ' \
                         'where `stockid` = "%s" and `date` = "%s"' % tuple(each)
        cursor.execute(update_cmd)
    for each in add_data2013:
        update_cmd = 'update `stock_2013` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s ' \
                         'where `stockid` = "%s" and `date` = "%s"' % tuple(each)
        cursor.execute(update_cmd)
    for each in add_data2014:
        update_cmd = 'update `stock_2014` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s ' \
                         'where `stockid` = "%s" and `date` = "%s"' % tuple(each)
        cursor.execute(update_cmd)
    for each in add_data2015:
        update_cmd = 'update `stock_2015` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s ' \
                         'where `stockid` = "%s" and `date` = "%s"' % tuple(each)
        cursor.execute(update_cmd)
    for each in add_data2016:
        update_cmd = 'update `stock_2016` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s ' \
                         'where `stockid` = "%s" and `date` = "%s"' % tuple(each)
        cursor.execute(update_cmd)
# refresh_bench()
# refresh_s('sz300208', '2016-05-17', '2016-05-27')


def revise_benchrsi(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
    #                      port=3306, charset="utf8")
    cursor = db.cursor()

    date = []
    close = []
    cursor.execute('select `close`, `date` from `bench` where `stockid` = "'+id+'"')
    raw = list(cursor.fetchall())
    for each in raw:
        date.append(each[1])
        close.append(each[0])
    rsi6 = talib.RSI(np.array(close), timeperiod=6)
    rsi12 = talib.RSI(np.array(close), timeperiod=12)
    rsi24 = talib.RSI(np.array(close), timeperiod=24)
    rsi6 = list(rsi6)
    rsi12 = list(rsi12)
    rsi24 = list(rsi24)
    for x in range(0, len(date)):
        temp = []
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
        temp.append(id)
        temp.append(date[x])
        cursor.execute('update `bench` set `rsi6` = %s, `rsi12` = %s, `rsi24` = %s '
                       ' where `stockid` = "%s" and `date` = "%s" ' % tuple(temp))



revise_benchrsi('sz399001')
# file = open('list.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     print line[:8]
#     add_data_rsi(line[:8])