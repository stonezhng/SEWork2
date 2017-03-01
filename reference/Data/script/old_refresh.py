def refresh_stock(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis",
                         port=8161)
    cursor = db.cursor()

    cursor.execute('select MAX(`date`) from `stock_2016` where `stockid`= "' + id + '"')

    startdate = str(cursor.fetchall()[0])

    startdate = startdate[2:12]
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    start = start + datetime.timedelta(days=1)
    startdate = start.strftime("%Y-%m-%d")

    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))

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

################################################################################

    high = []
    low = []
    close = []
    date = []
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    start = start + datetime.timedelta(days=-400)
    startdate = start.strftime("%Y-%m-%d")

    url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=" + startdate + "&end=" + enddate
    # print url
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']
    for x in range(0, len(json_file)):
        if json_file[x]['turnover'] != 0:
            high.append(json_file[x]['high'])
            low.append(json_file[x]['low'])
            close.append(json_file[x]['close'])
            date.append(json_file[x]['date'])
    print close
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

    data2 = []
    temp = []
    for x in range(0, len(date)):
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
        data2.append(tuple(temp))
        temp = []

##################################################################################
    start_point = len(data2) - len(data1)
    for x in range(0, len(data1)):
        data1[x].extend(data2[start_point+x])
    # print data1

    insert_cmd = 'INSERT INTO `stock_2016` ' \
                                                '(`stockid`, `date`, open, high, low, close, volume, ' \
                                                'adj_price, turnover, pe_ttm, pb,  ' \
                                                '`dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                                '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,' \
                                                '`deviation_val`, `deviation_per`) ' \
                                                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, data1)
    db.commit()
    db.close()


def refresh_bench():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis",
                         port=8161)
    cursor = db.cursor()

    cursor.execute('select MAX(`date`) from `stock_2016` where `stockid`= "hs300"')

    startdate = str(cursor.fetchall()[0])

    startdate = startdate[2:12]
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    start = start + datetime.timedelta(days=1)
    startdate = start.strftime("%Y-%m-%d")

    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    data1 = []

    url = "http://121.41.106.89:8010/api/benchmark/hs300/?start=" + startdate + "&end=" + enddate
    # print id
    print url
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']
    for x in range(0, len(json_file)):
        temp = ['hs300', str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
                float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
                float(json_file[x]['adj_price']), None, None, None]
        data1.append(temp)

################################################################################

    high = []
    low = []
    close = []
    date = []
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    start = start + datetime.timedelta(days=-400)
    startdate = start.strftime("%Y-%m-%d")

    url = "http://121.41.106.89:8010/api/benchmark/hs300/?start=" + startdate + "&end=" + enddate
    # print url
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']
    for x in range(0, len(json_file)):
        high.append(json_file[x]['high'])
        low.append(json_file[x]['low'])
        close.append(json_file[x]['close'])
        date.append(json_file[x]['date'])
    print close
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

    data2 = []
    temp = []
    for x in range(0, len(date)):
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
        data2.append(tuple(temp))
        temp = []

##################################################################################
    start_point = len(data2) - len(data1)
    for x in range(0, len(data1)):
        data1[x].extend(data2[start_point+x])
    # print data1

    insert_cmd = 'INSERT INTO `stock_2016` ' \
                                                '(`stockid`, `date`, open, high, low, close, volume, ' \
                                                'adj_price, turnover, pe_ttm, pb,  ' \
                                                '`dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                                '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,' \
                                                '`deviation_val`, `deviation_per`) ' \
                                                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, data1)
    db.commit()
    db.close()