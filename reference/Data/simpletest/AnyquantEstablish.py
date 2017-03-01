import json
import urllib2
from multiprocessing.dummy import Pool as ThreadPool
import tushare as ts
import MySQLdb
import datetime
import talib
import numpy as np
#
# def init_anyquant_database():
#     idlist = []
#     file = open('StockList.txt')
#     while 1:
#         line = file.readline()
#         if not line:
#             break
#         idlist.append(line[:8])
#         # print line[:8]
#     print idlist
#     failid = []
#     for x in range(0, len(idlist)):
#         isOK = create_anyquant_table(idlist[x])
#         if isOK == 0:
#             failid.append(id)
#     # pool = ThreadPool(2)
#     # failid = pool.map(create_anyquant_table, idlist)
#     # pool.close()
#     # pool.join()
#     file = open('establish_report.txt', 'w')
#     for x in range(0, len(failid)):
#         file.write(failid[x])


def get_calc(id):
    file = open('validdate.txt', 'w')
    url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=2010-01-01&end=2016-12-31"
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    high = []
    low = []
    close = []
    json_file = json.loads(res)['data']['trading_info']
    for row in json_file:
        if row['volume'] != 0:
            close.append(row['close'])
            high.append(row['high'])
            low.append(row['low'])
            file.write(row['date']+'\n')
    print len(close)
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
    # for item in atr:
    #     print item
    file.close()
    return [atr, ma5, ma10, ma20, ma30, ma60, dif, dea, macd, slowK, slowD, rsi, boll_upper, boll_middle, boll_low]


def create_tushare_ticks(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis", port=8161)
    cursor = db.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS `" + id + '`')
    except:
        1
#  time       price change  volume  amount  type
    create_cmd = "CREATE TABLE `" + id + """`(
        `time` varchar(60) NOT NULL,
        `price` float,
        `change` text,
        `volume` bigint,
        `amount` bigint,
        `type` text,
        PRIMARY KEY(`time`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    data = []
    cursor.execute(create_cmd)
    starttime = datetime.datetime(2010, 01, 01)
    endtime = datetime.datetime(2016, 05, 07)
    while (starttime - endtime).days != 0:
        df = ts.get_tick_data(id[2:], str(starttime)[0:10])
        for row in df.values:
            temp = (line+" "+row[0], row[1], row[2], row[3], row[4], row[5])
            print temp
            data.append(temp)
        starttime = starttime + datetime.timedelta(days=1)
    insert_cmd = 'INSERT INTO `' + id + '` (`time`, `price`, `change`, `volume`, `amount`, `type`) VALUES (%s, %s, %s, %s, %s, %s)'

    count = 1000
    data = tuple(data)
    while count < len(data):
        cursor.executemany(insert_cmd, data[count - 1000: count])
        db.commit()
        count += 1000;
    cursor.executemany(insert_cmd, data[count:])
    db.commit()
    db.close()
    #     url = "http://market.finance.sina.com.cn/downxls.php?date=" + line[:10] + "&symbol="+id
    #     print url
    #     req = urllib2.Request(url)
    #     res_data = urllib2.urlopen(req)
    #     res = res_data.read()
    #     print res
    # url = "http://market.finance.sina.com.cn/pricehis.php?symbol=sh600900&startdate=2011-08-17&enddate=2011-08-19"
    # print url
    # req = urllib2.Request(url)
    # res_data = urllib2.urlopen(req)
    # res = res_data.read()
    # print res


def create_anyquant_year(year, idlist):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis")
    cursor = db.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS `" + year + '`')
    except:
        1

    create_cmd = "CREATE TABLE `" + year + """`(
        `stockid` varchar(40) NOT NULL,
        `date` varchar(40) NOT NULL,
        `open` float,
        `high` float,
        `low` float,
        `close` float,
        `volume` bigint,
        `adj_price` float,
        `turnover` float,
        `pe_ttm` float,
        `pb` float,
        `ma5` float,
        `ma10` float,
        `ma20` float,
        `ma30` float,
        `ma60` float,
        `atr`, float,
        `dea`, float,
        `dif`, float,
        `macd`, float,
        `change`, float,
        PRIMARY KEY(`date`, `stockid`)
        INDEX(`stockid`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    data = []
    cursor.execute(create_cmd)
    for id in idlist:
        print id
        url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=" + year + "-01-01&end=" + year + "-12-31"
        req = urllib2.Request(url)
        req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
        res_data = urllib2.urlopen(req)
        res = res_data.read()
        json_file = json.loads(res)['data']['trading_info']
        insert_cmd = 'INSERT INTO `' + year + '` (stockid, date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb) ' \
                                              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        for x in range(0, len(json_file)):
            temp = [id, str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
                    float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
                    float(json_file[x]['adj_price']), float(json_file[x]['turnover']),
                    float(json_file[x]['pe_ttm']), float(json_file[x]['pb'])]
            data.append(temp)
    count = 1000
    data = tuple(data)
    while count < len(data):
        cursor.executemany(insert_cmd, data[count-1000: count])
        db.commit()
        count += 1000
    cursor.executemany(insert_cmd, data[count:])
    db.commit()
    db.close()


# def create_anyquant_table(id):
#     print id
#     db = MySQLdb.connect("115.159.46.93", "client", "123456", "StocksAnalysis")
#     cursor = db.cursor()
#     try:
#         cursor.execute("DROP TABLE IF EXISTS `"+id+'`')
#     except:
#         1
#
#     create_cmd = "CREATE TABLE `" + id + """`(
#         `date` varchar(40) NOT NULL,
#         `open` float,
#         `high` float,
#         `low` float,
#         `close` float,
#         `volume` bigint,
#         `adj_price` float,
#         `turnover` float,
#         `pe_ttm` float,
#         `pb` float,
#         PRIMARY KEY(`date`)
#         )ENGINE=MyISAM DEFAULT CHARSET=utf8;
#         """
#     cursor.execute(create_cmd)
#
#     url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=2010-01-01&end=2016-05-01"
#     req = urllib2.Request(url)
#     req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
#     try:
#         res_data = urllib2.urlopen(req)
#     except:
#         return id
#     res = res_data.read()
#     json_file = json.loads(res)['data']['trading_info']
#     data = []
#     for x in xrange(0, len(json_file)):
#         temp = (str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
#                     float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
#                     float(json_file[x]['adj_price']), float(json_file[x]['turnover']),
#                     float(json_file[x]['pe_ttm']), float(json_file[x]['pb']))
#         data.append(temp)
#     insert_cmd = 'INSERT INTO `' + id + '` (date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb) ' \
#                                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
#     try:
#         cursor.executemany(insert_cmd, data)
#     except Exception, e:
#         return 0
#     finally:
#         db.commit()
#         db.close()
#         return 1

# starttime = datetime.datetime.now()
# init_anyquant_database()
# endtime = datetime.datetime.now()
# print 'running time: '
# print (endtime-starttime).seconds
# print 'seconds'

# file = open('StockList.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     create_tushare_ticks(line[:8])


#
# create_anyquant_year('2010', idlist)
# create_anyquant_year('2011', idlist)
# create_anyquant_year('2012', idlist)
# create_anyquant_year('2013', idlist)
# create_anyquant_year('2014', idlist)
# create_anyquant_year('2015', idlist)
# create_anyquant_year('2016', idlist)