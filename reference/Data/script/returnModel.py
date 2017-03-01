from sklearn import svm

import pandas as pd
import csv
from itertools import islice
from matplotlib import pyplot as plt
import numpy as np
import scipy.integrate as integrate
from sklearn.cross_validation import train_test_split
from sklearn.linear_model import LinearRegression
import MySQLdb
import datetime
import time


def predict_price():
    csvfile = file('history.csv')
    reader = csv.reader(csvfile)
    op = []
    high = []
    low = []
    close = []
    price_change = []
    turnover = []
    for line in islice(reader, 1, None):
        op.append(float(line[1]))
        high.append(float(line[2]))
        low.append(float(line[4]))
        close.append(float(line[3]))
        price_change.append(float(line[6]))
        turnover.append(float(line[14]))

    op = op[1:]
    high = high[:-1]
    low = low[:-1]
    close = close[:-1]
    price_change = price_change[:-1]
    turnover = turnover[:-1]

    data = pd.DataFrame([high, low, close, price_change, turnover, op],
                        index=['high', 'low', 'close', 'price_change', 'turnover', 'op'])
    data = data.T
    x = data[['high', 'low', 'close', 'price_change', 'turnover']]
    y = data['op']

    # X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
    X_train = x[:-3]
    X_test = x[-3:]
    Y_train = y[:-3]
    Y_test = y[-3:]

    linreg = LinearRegression()
    linreg.fit(X_train, Y_train)

    print(linreg.coef_)
    print("############################################")
    print(np.mean((linreg.predict(X_test) - Y_test) ** 2))
    print("############################################")
    print(linreg.score(X_test, Y_test))
    print("############################################")
    print(Y_test)
    print("############################################")
    print(linreg.predict(X_test))
    print("############################################")
    # plt.scatter(X_test, Y_test, color='black')
    plt.scatter(Y_test, linreg.predict(X_test), color='blue')
    plt.show()
    #
    # plt.xticks(())
    # plt.yticks(())


def predict(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    start = end + datetime.timedelta(days=-400)
    startdate = start.strftime("%Y-%m-%d")

    select_cmd = "select * from `stock_2016`where `date` > '"+startdate+"' and `date` < '"+enddate+"' and `stockid` = '" + id + "'"
    cursor.execute(select_cmd)
    raw_result = cursor.fetchall()
    # print raw_result
    trainX = []
    trainYClose = []
    trainYOpen = []
    trainYHigh = []
    trainYLow = []
    trainYAdj = []
    temp = list(raw_result[0][2:6])
    temp.append(raw_result[0][7])
    trainX.append(temp)
    for x in range(1, len(raw_result)-1):
        temp = list(raw_result[x][2:6])
        temp.append(raw_result[x][7])
        trainX.append(temp)
        trainYClose.append(raw_result[x][5])
        trainYOpen.append(raw_result[x][2])
        trainYHigh.append(raw_result[x][3])
        trainYLow.append(raw_result[x][4])
        trainYAdj.append(raw_result[x][7])

    trainYClose.append(raw_result[len(raw_result)-1][5])
    trainYOpen.append(raw_result[len(raw_result)-1][2])
    trainYHigh.append(raw_result[len(raw_result)-1][3])
    trainYLow.append(raw_result[len(raw_result)-1][4])
    trainYAdj.append(raw_result[len(raw_result)-1][7])
    # print trainX
    # print trainY
    temp = list(raw_result[len(raw_result)-1][2:6])
    temp.append(raw_result[len(raw_result)-1][7])
    testX = temp
    clf = svm.SVR()

    clf.fit(trainX, trainYClose)
    npY = np.array(trainYClose)
    var = np.var(npY)/len(trainYClose)
    d = np.sqrt(var)
    result = clf.predict(testX)
    close_middle_fst = result[0]
    close_upper_fst = result[0] + d
    close_low_fst = result[0] - d

    clf.fit(trainX, trainYOpen)
    open_pre = clf.predict(testX)[0]
    clf.fit(trainX, trainYHigh)
    high_pre = clf.predict(testX)[0]
    clf.fit(trainX, trainYLow)
    low_pre = clf.predict(testX)[0]
    clf.fit(trainX, trainYAdj)
    adj_pre = clf.predict(testX)[0]

    testX = [open_pre, high_pre, low_pre, close_middle_fst, adj_pre]
    trainX.append(testX)
    trainYClose.append(close_middle_fst)
    clf.fit(trainX, trainYClose)
    result = clf.predict(testX)
    close_middle_snd = result[0]
    close_upper_snd = result[0] + d
    close_low_snd = result[0] - d

    trainYOpen.append(open_pre)
    trainYHigh.append(high_pre)
    trainYLow.append(low_pre)
    trainYAdj.append(adj_pre)
    clf.fit(trainX, trainYOpen)
    open_pre = clf.predict(testX)[0]
    clf.fit(trainX, trainYHigh)
    high_pre = clf.predict(testX)[0]
    clf.fit(trainX, trainYLow)
    low_pre = clf.predict(testX)[0]
    clf.fit(trainX, trainYAdj)
    adj_pre = clf.predict(testX)[0]

    testX = [open_pre, high_pre, low_pre, close_middle_fst, adj_pre]
    trainX.append(testX)
    trainYClose.append(close_middle_snd)
    clf.fit(trainX, trainYClose)
    result = clf.predict(testX)
    close_middle_thd = result[0]
    close_upper_thd = result[0] + d
    close_low_thd = result[0] - d

    return [close_upper_fst, close_middle_fst, close_low_fst, close_upper_snd, close_middle_snd, close_low_snd,
            close_upper_thd, close_middle_thd, close_low_thd]


def refresh_predict():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    file = open('StockList.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        print line[:8]
        temp = (predict(line[:8]))
        temp.append(line[:8])

    # insert_cmd = """
    # insert into `predict` (`stockid`, `close_upper_fst`, `close_middle_fst`, `close_low_fst`, `close_upper_snd`,
    #  `close_middle_snd`, `close_low_snd`, `close_upper_thd`, `close_middle_thd`, `close_low_thd`)
    #  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    # """
        update_cmd = """
        update `predict` set `close_upper_fst` = %s, `close_middle_fst` = %s, `close_low_fst` = %s, `close_upper_snd` = %s,
        `close_middle_snd` = %s, `close_low_snd` = %s, `close_upper_thd` = %s, `close_middle_thd` = %s, `close_low_thd` = %s
        where `stockid` = "%s"
        """ % tuple(temp)
        cursor.execute(update_cmd)
    # db.commit()
    db.close()

# predict_price()
# print predict('sz000620')
refresh_predict()