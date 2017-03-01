import json
import urllib2

import MySQLdb
import talib
import numpy as np


def bench_establish():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test", port=8161)
    cursor = db.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS `bench`")
    except:
        1

    create_cmd = """CREATE TABLE `bench`(
            `stockid` varchar(40) NOT NULL,
            `date` varchar(40) NOT NULL,
            `open` float,
            `high` float,
            `low` float,
            `close` float,
            `volume` bigint,
            `adj_price` float,
            `amount` bigint,
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
            PRIMARY KEY(`date`, `stockid`),
            INDEX(`stockid`)
            )ENGINE=MyISAM DEFAULT CHARSET=utf8;
            """
    data = []
    cursor.execute(create_cmd)

    url = "http://121.41.106.89:8010/api/benchmark/hs300/?start=2010-01-01&end=2016-12-31"
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']
    for x in range(0, len(json_file)):
        temp = ['hs300', str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
                float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
                float(json_file[x]['adj_price'])]
        data.append(temp)

    insert_cmd = 'INSERT INTO `bench` (stockid, date, open, high, low, close, volume, ' \
                                            'adj_price) ' \
                                            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

    count = 1000
    data = tuple(data)
    while count < len(data):
        cursor.executemany(insert_cmd, data[count - 1000: count])
        db.commit()
        count += 1000
    cursor.executemany(insert_cmd, data[count-1000:])
    db.commit()
    db.close()


def bench_calc(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test", port=8161)
    cursor = db.cursor()

    select_cmd = 'select `date`, `close`, `high`, `low`, `amount` from `bench` where `stockid` = "'+id+'"'
    cursor.execute(select_cmd)
    rawdata = list(cursor.fetchall())
    high = []
    low = []
    close = []
    date = []
    for x in range(0, len(rawdata)):
        date.append(rawdata[x][0])
        close.append(rawdata[x][1])
        high.append(rawdata[x][2])
        low.append(rawdata[x][3])
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

    data = []
    temp = []
    for x in range(0, len(date)):
        temp.append(date[x])
        if str(dif[x]) != 'nan':
            temp.append(dif[x])
        else:
            temp.append('NULL')
        if str(dea[x]) != 'nan':
            temp.append(dea[x])
        else:
            temp.append('NULL')
        if str(macd[x]) != 'nan':
            temp.append(macd[x])
        else:
            temp.append('NULL')
        if str(slowK[x]) != 'nan':
            temp.append(slowK[x])
        else:
            temp.append('NULL')
        if str(slowD[x]) != 'nan':
            temp.append(slowD[x])
        else:
            temp.append('NULL')
        if str(slowD[x]) != 'nan':
            temp.append(3 * slowD[x] - 2 * slowK[x])
        else:
            temp.append('NULL')
        if str(boll_upper[x]) != 'nan':
            temp.append(boll_upper[x])
        else:
            temp.append('NULL')
        if str(boll_middle[x]) != 'nan':
            temp.append(boll_middle[x])
        else:
            temp.append('NULL')
        if str(boll_low[x]) != 'nan':
            temp.append(boll_low[x])
        else:
            temp.append('NULL')
        if str(ma5[x]) != 'nan':
            temp.append(ma5[x])
        else:
            temp.append('NULL')
        if str(ma10[x]) != 'nan':
            temp.append(ma10[x])
        else:
            temp.append('NULL')
        if str(ma20[x]) != 'nan':
            temp.append(ma20[x])
        else:
            temp.append('NULL')
        if str(ma30[x]) != 'nan':
            temp.append(ma30[x])
        else:
            temp.append('NULL')
        if str(ma60[x]) != 'nan':
            temp.append(ma60[x])
        else:
            temp.append('NULL')
        if str(atr[x]) != 'nan':
            temp.append(atr[x])
        else:
            temp.append('NULL')
        if str(rsi[x]) != 'nan':
            temp.append(rsi[x])
        else:
            temp.append('NULL')
        if x == 0:
            temp.append('NULL')
            temp.append('NULL')
        else:
            temp.append(close[x] - close[x - 1])
            temp.append(float(close[x] - close[x - 1]) / float(close[x - 1]))
        temp.append(id)
        data.append(tuple(temp))
        temp = []
    for each in data:
        update_cmd = 'update `bench` set `dif` = %s, `dea` = %s, `macd` = %s, `slowK` = %s, `slowD` = %s, `slowJ` = %s, ' \
                     '`boll_upper` = %s, `boll_middle` = %s, `boll_low` = %s, `ma5` = %s, `ma10` = %s, `ma20` = %s, ' \
                     '`ma30` = %s, `ma60` = %s, `atr` = %s, `rsi` = %s, `deviation_val` = %s, `deviation_per` = %s' \
                     ' where `stockid` = "%s" and `date` = "%s"' % (each[1], each[2], each[3], each[4], each[5], each[6],
                                                                   each[7], each[8], each[9], each[10], each[11],
                                                                   each[12], each[13], each[14], each[15], each[16],
                                                                   each[17], each[18], each[19], each[0])
        print update_cmd
        cursor.execute(update_cmd)


# bench_establish()
bench_calc('hs300')