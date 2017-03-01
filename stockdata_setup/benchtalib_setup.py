import MySQLdb
import talib
import numpy as np


def talib_calc(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    select_cmd = 'select `date`, `close`, `high`, `low` from `bench` where `stockid` = "%s" ORDER BY `date`' % id
    cursor.execute(select_cmd)
    json_file = list(cursor.fetchall())
    # print json_file
    if not json_file:
        print 'empty'
        return
    high = []
    low = []
    close = []
    date = []

    for x in range(0, len(json_file)):
        # if json_file[x][4] != 0:
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
        if str(dif[x]) != 'nan':
            temp.append(dif[x])
        else:
            temp.append(0)
        if str(dea[x]) != 'nan':
            temp.append(dea[x])
        else:
            temp.append(0)
        if str(macd[x]) != 'nan':
            temp.append(2*macd[x])
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
            temp.append(3*slowD[x]-2*slowK[x])
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
        update_cmd = 'update `bench` set `dif` = %s, `dea` = %s, `macd` = %s, `slowK` = %s, ' \
                    '`slowD` = %s, `slowJ` = %s, `boll_upper` = %s, `boll_middle` = %s, `boll_low` = %s, ' \
                    '`ma5` = %s, `ma10` = %s, `ma20` = %s, `ma30` = %s, `ma60` = %s, `atr` = %s, `rsi6` = %s, ' \
                    '`rsi12` = %s, `rsi24` = %s where `stockid` = "%s" and `date` = "%s"' % tuple(temp)
        cursor.execute(update_cmd)
        temp = []

    db.close()

print '000001'
talib_calc('sh000001')
print '399001'
talib_calc('sz399001')
print '399300'
talib_calc('399300')