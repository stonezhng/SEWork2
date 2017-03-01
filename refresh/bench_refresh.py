import urllib

import talib
from lxml import etree
import numpy as np

import MySQLdb
import datetime


def benchdata_refresh(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    select_cmd = 'select MAX(`date`) from `bench` where `stockid` = "' + id + '"'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    # print raw
    if raw[0][0] is None:
        return
    startdate = raw[0][0]
    start = datetime.datetime.strptime(startdate, '%Y-%m-%d')
    end = datetime.datetime.now()
    enddate = end.strftime('%Y-%m-%d')
    url_list = url_generator(id, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    data = []
    for each in url_list:
        print each
        response = urllib.urlopen(each).read()
        tree = etree.HTML(response)
        table = tree.xpath('/html/body/div[2]/div[3]/table/tr')
        for each in table:
            date = each.xpath('td[1]/text()')[0]
            date = date[0:4] + '-' + date[4:6] + '-' + date[6:]
            if startdate < date <= enddate:
                temp = [id, date]
                open = each.xpath('td[2]/text()')[0]
                open = open.replace(",", "")
                temp.append(float(open))
                high = each.xpath('td[3]/text()')[0]
                high = high.replace(",", "")
                temp.append(float(high))
                low = each.xpath('td[4]/text()')[0]
                low = low.replace(",", "")
                temp.append(float(low))
                close = each.xpath('td[5]/text()')[0]
                close = close.replace(",", "")
                temp.append(float(close))
                temp.append(each.xpath('td[6]/text()')[0])
                temp.append(each.xpath('td[7]/text()')[0])
                volume = each.xpath('td[8]/text()')[0]
                volume = volume.replace(",", "")
                temp.append(round(float(volume), 0))
                amount = each.xpath('td[9]/text()')[0]
                amount = amount.replace(",", "")
                temp.append(round(float(amount), 0))
                data.append(temp)
    cursor.executemany(" INSERT INTO `bench` (`stockid`, `date`, `open`, `high`, `low`, `close`, "
                                   "`deviation_val`, `deviation_per`,`volume`, `amount`) "
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ", tuple(data))
    db.commit()
    db.close()


def talib_refresh(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    select_cmd = 'select MIN(`date`) from `bench` where `stockid` = "%s" and `dif` = 0' % id
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    if raw[0][0] is None:
        print 'empty'
        return
    startdate = raw[0][0]
    select_cmd = 'select MAX(`date`) from `bench` where `stockid` = "%s"' % id
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    if raw[0][0] is None:
        print 'empty'
        return
    enddate = raw[0][0]
    select_cmd = 'select `date`, `close`, `high`, `low` from `bench` where `stockid` = "%s" and `date` <= "%s" ' \
                 ' and `date` >= "2015-01-01" group by `date`' % (id, enddate)
    # print select_cmd
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    # print raw
    date = []
    close = []
    high = []
    low = []
    for each in raw:
        date.append(each[0])
        close.append(each[1])
        high.append(each[2])
        low.append(each[3])
    # print close
    if not date:
        print 'empty'
        return
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

    temp = []
    for x in range(0, len(date)):
        if enddate >= date[x] >= startdate:
            if str(dif[x]) != 'nan':
                temp.append(dif[x])
            else:
                temp.append(0)
            if str(dea[x]) != 'nan':
                temp.append(dea[x])
            else:
                temp.append(0)
            if str(macd[x]) != 'nan':
                temp.append(macd[x])
            else:
                temp.append(0)
            if str(slowK[x]) != 'nan':
                temp.append(slowK[x])
            else:
                temp.append(0)
            if str(slowD[x]) != 'nan':
                temp.append(slowD[x])
            else:
                temp.append(0)
            if str(slowD[x]) != 'nan':
                temp.append(3 * slowD[x] - 2 * slowK[x])
            else:
                temp.append(0)
            if str(boll_upper[x]) != 'nan':
                temp.append(boll_upper[x])
            else:
                temp.append(0)
            if str(boll_middle[x]) != 'nan':
                temp.append(boll_middle[x])
            else:
                temp.append(0)
            if str(boll_low[x]) != 'nan':
                temp.append(boll_low[x])
            else:
                temp.append(0)
            if str(ma5[x]) != 'nan':
                temp.append(ma5[x])
            else:
                temp.append(0)
            if str(ma10[x]) != 'nan':
                temp.append(ma10[x])
            else:
                temp.append(0)
            if str(ma20[x]) != 'nan':
                temp.append(ma20[x])
            else:
                temp.append(0)
            if str(ma30[x]) != 'nan':
                temp.append(ma30[x])
            else:
                temp.append(0)
            if str(ma60[x]) != 'nan':
                temp.append(ma60[x])
            else:
                temp.append(0)
            if str(atr[x]) != 'nan':
                temp.append(atr[x])
            else:
                temp.append(0)
            if str(rsi6[x]) != 'nan':
                temp.append(rsi6[x])
            else:
                temp.append(0)
            if str(rsi12[x]) != 'nan':
                temp.append(rsi12[x])
            else:
                temp.append(0)
            if str(rsi24[x]) != 'nan':
                temp.append(rsi24[x])
            else:
                temp.append(0)
            temp.append(id)
            temp.append(date[x])
            # print temp
            update_cmd = 'update `bench` set `dif` = %s, `dea` = %s, `macd` = %s, `slowK` = %s, ' \
                        '`slowD` = %s, `slowJ` = %s, `boll_upper` = %s, `boll_middle` = %s, `boll_low` = %s, ' \
                        '`ma5` = %s, `ma10` = %s, `ma20` = %s, `ma30` = %s, `ma60` = %s, `atr` = %s, `rsi6` = %s, ' \
                        '`rsi12` = %s, `rsi24` = %s where `stockid` = "%s" and `date` = "%s"' % tuple(temp)
            cursor.execute(update_cmd)
            temp = []


def url_generator(id, startdate, enddate):
    url_list = []
    abstract = "http://quotes.money.163.com/trade/lsjysj_zhishu_%s.html?year=%s&season=%s"
    s_year = startdate[0:4]
    e_year = enddate[0:4]
    if int(startdate[5:7]) <= 3:
        s_season = '1'
    elif 3 < int(startdate[5:7]) <= 6:
        s_season = '2'
    elif 6 < int(startdate[5:7]) <= 9:
        s_season = '3'
    elif 9 < int(startdate[5:7]):
        s_season = '4'

    if int(enddate[5:7]) <= 3:
        e_season = '1'
    elif 3 < int(enddate[5:7]) <= 6:
        e_season = '2'
    elif 6 < int(enddate[5:7]) <= 9:
        e_season = '3'
    elif 9 < int(enddate[5:7]):
        e_season = '4'

    i = 0

    while int(s_year) + i <= int(e_year):
        if int(s_year) + i != int(e_year):
            if i != 0:
                for x in range(1, 5):
                    url_list.append(abstract % (id, str(int(s_year) + i), str(x)))
            else:
                for x in range(int(s_season), 5):
                    url_list.append(abstract % (id, str(int(s_year) + i), str(x)))
        else:
            if i != 0:
                for x in range(1, int(e_season) + 1):
                    url_list.append(abstract % (id, str(int(s_year) + i), str(x)))
            else:
                for x in range(int(s_season), int(e_season) + 1):
                    url_list.append(abstract % (id, str(int(s_year) + i), str(x)))
        i += 1

    return url_list


def refresh_bench():
    benchdata_refresh('000001')
    talib_refresh('000001')
    benchdata_refresh('399001')
    talib_refresh('399001')
    benchdata_refresh('399300')
    talib_refresh('399300')