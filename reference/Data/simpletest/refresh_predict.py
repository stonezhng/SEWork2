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


def predict(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    # enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    # end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    # start = end + datetime.timedelta(days=-400)
    # startdate = start.strftime("%Y-%m-%d")

    select_cmd = "select * from `stock_2016` where  `stockid` = '" + id + "' union " \
                " select * from `stock_2015` where  `stockid` = '" + id + "'"

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
    print trainX
    print trainYClose
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

refresh_predict()
