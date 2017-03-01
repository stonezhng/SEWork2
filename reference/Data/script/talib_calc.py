import urllib2
import json

import talib
import numpy as np
import MySQLdb


def create_talib(id):
    # url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=2010-01-01&end=2016-12-31"
    # req = urllib2.Request(url)
    # req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    # res_data = urllib2.urlopen(req)
    # res = res_data.read()
    high = []
    low = []
    close = []
    date = []
    # deviation_val = []
    # deviation_per = []
    # json_file = json.loads(res)['data']['trading_info']

    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    select_cmd = 'select `date`, `close`, `high`, `low`, `amount` from `pre_2010` where `stockid` = "'+id+'" union ' \
                 'select `date`, `close`, `high`, `low`, `amount` from `pre_2011` where `stockid` = "'+id+'" union ' \
                 'select `date`, `close`, `high`, `low`, `amount` from `pre_2012` where `stockid` = "'+id+'"  union ' \
                 'select `date`, `close`, `high`, `low`, `amount` from `pre_2013` where `stockid` = "'+id+'"  union ' \
                 'select `date`, `close`, `high`, `low`, `amount` from `pre_2014` where `stockid` = "'+id+'"  union ' \
                 'select `date`, `close`, `high`, `low`, `amount` from `pre_2015` where `stockid` = "'+id+'"  union ' \
                 'select `date`, `close`, `high`, `low`, `amount` from `pre_2016` where `stockid` = "'+id+'"  '
    cursor.execute(select_cmd)

    json_file = list(cursor.fetchall())
    # print json_file[0]
    # date.append(json_file[0][0])
    # close.append(json_file[0][1])
    # high.append(json_file[0][2])
    # low.append(json_file[0][3])


    # for row in json_file[1:]:
    #     if row['volume'] != 0:
    #         date.append(row['date'])
    #         close.append(row['close'])
    #         high.append(row['high'])
    #         low.append(row['low'])
    #         deviation_val.append()

    for x in range(0, len(json_file)):
        if json_file[x][4] != 0:
            date.append(json_file[x][0])
            close.append(json_file[x][1])
            high.append(json_file[x][2])
            low.append(json_file[x][3])

    dif, dea, macd = talib.MACD(np.array(close), fastperiod=12, slowperiod=26, signalperiod=9)
    slowK, slowD = talib.STOCH(np.array(high), np.array(low), np.array(close), fastk_period=9, slowk_period=3,
                               slowk_matype=0, slowd_period=3, slowd_matype=0)
    boll_upper, boll_middle, boll_low = talib.BBANDS(np.array(close), timeperiod=15, nbdevup=1, nbdevdn=1, matype=0)
    ma5 = talib.MA(np.array(close), timeperiod=5, matype=0)
    ma10 = talib.MA(np.array(close), timeperiod=10, matype=0)
    ma20 = talib.MA(np.array(close), timeperiod=20, matype=0)
    ma30 = talib.MA(np.array(close), timeperiod=30, matype=0)
    ma60 = talib.MA(np.array(close), timeperiod=60, matype=0)
    atr = talib.ATR(np.array(high), np.array(low), np.array(close), timeperiod=14)
    rsi = talib.RSI(np.array(close), timeperiod=14)

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
    rsi = list(rsi)


    create_cmd = "CREATE TABLE `" + id + """_Analysis`(
        `id` varchar(40) NOT NULL,
        `date` varchar(40) NOT NULL,
        `dif` float,
        `dea` float,
        `macd` float,
        `slowK` float,
        `slowD` float,
        `slowJ` float,
        `boll_upper` float,
        `boll_middle` float,
        `boll_low` float,
        `ma5` float,
        `ma10` float,
        `ma20` float,
        `ma30` float,
        `ma60` float,
        `atr` float,
        `rsi` float,
        `deviation_val` float,
        `deviation_per` float,
        PRIMARY KEY(`date`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    cursor.execute(create_cmd)

    data = []
    temp = []
    for x in range(0, len(date)):
        temp.append(id)
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
            temp.append(3*slowD[x]-2*slowK[x])
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
        if str(rsi[x]) != 'nan':
            temp.append(rsi[x])
        else:
            temp.append(None)
        if x == 0:
            temp.append(None)
            temp.append(None)
        else:
            temp.append(close[x] - close[x-1])
            temp.append(float(close[x] - close[x-1])/float(close[x-1]))
        data.append(tuple(temp))
        temp = []

    insert_cmd = 'INSERT INTO `' + id + '_Analysis` ' \
                                        '(`id`, `date`, `dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                        '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi`, `deviation_val`, `deviation_per`) ' \
                                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()

file = open('StockList.txt')
while 1:
    id = file.readline()
    if not id:
        break
    print id[:8]
    create_talib(id[:8])
#sz002482
#sz002073
#sz000610
# create_talib('sz002482')


def benchmark_calc():
    url = "http://121.41.106.89:8010/api/benchmark/hs300/?start=2010-01-01&end=2016-12-31"
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    high = []
    low = []
    close = []
    date = []
    deviation_val = []
    deviation_per = []
    json_file = json.loads(res)['data']['trading_info']

    date.append(json_file[0]['date'])
    close.append(json_file[0]['close'])
    high.append(json_file[0]['high'])
    low.append(json_file[0]['low'])

    # for row in json_file[1:]:
    #     if row['volume'] != 0:
    #         date.append(row['date'])
    #         close.append(row['close'])
    #         high.append(row['high'])
    #         low.append(row['low'])
    #         deviation_val.append()

    for x in range(1, len(json_file)):
        date.append(json_file[x]['date'])
        close.append(json_file[x]['close'])
        high.append(json_file[x]['high'])
        low.append(json_file[x]['low'])

    dif, dea, macd = talib.MACD(np.array(close), fastperiod=12, slowperiod=26, signalperiod=9)
    slowK, slowD = talib.STOCH(np.array(high), np.array(low), np.array(close), fastk_period=9, slowk_period=3,
                               slowk_matype=0, slowd_period=3, slowd_matype=0)
    boll_upper, boll_middle, boll_low = talib.BBANDS(np.array(close), timeperiod=15, nbdevup=1, nbdevdn=1, matype=0)
    ma5 = talib.MA(np.array(close), timeperiod=5, matype=0)
    ma10 = talib.MA(np.array(close), timeperiod=10, matype=0)
    ma20 = talib.MA(np.array(close), timeperiod=20, matype=0)
    ma30 = talib.MA(np.array(close), timeperiod=30, matype=0)
    ma60 = talib.MA(np.array(close), timeperiod=60, matype=0)
    atr = talib.ATR(np.array(high), np.array(low), np.array(close), timeperiod=14)
    rsi = talib.RSI(np.array(close), timeperiod=14)

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
    rsi = list(rsi)

    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    create_cmd = "CREATE TABLE `" + 'hs300' + """_Analysis`(
        `id` varchar(40) NOT NULL,
        `date` varchar(40) NOT NULL,
        `dif` float,
        `dea` float,
        `macd` float,
        `slowK` float,
        `slowD` float,
        `slowJ` float,
        `boll_upper` float,
        `boll_middle` float,
        `boll_low` float,
        `ma5` float,
        `ma10` float,
        `ma20` float,
        `ma30` float,
        `ma60` float,
        `atr` float,
        `rsi` float,
        `deviation_val` float,
        `deviation_per` float,
        PRIMARY KEY(`date`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    cursor.execute(create_cmd)

    data = []
    temp = []
    for x in range(0, len(date)):
        temp.append('hs300')
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
        if str(rsi[x]) != 'nan':
            temp.append(rsi[x])
        else:
            temp.append(None)
        if x == 0:
            temp.append(None)
            temp.append(None)
        else:
            temp.append(close[x] - close[x - 1])
            temp.append(float(close[x] - close[x - 1]) / float(close[x - 1]))
        data.append(tuple(temp))
        temp = []

    insert_cmd = 'INSERT INTO `' + 'hs300' + '_Analysis` ' \
                                             '(`id`, `date`, `dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                             '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi`, `deviation_val`, `deviation_per`) ' \
                                             'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()
