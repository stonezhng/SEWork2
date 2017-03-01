from sklearn import svm

import MySQLdb
import datetime
import numpy as np


def get_predict(trainX, trainY, testX):
    clf = svm.SVR()
    clf.fit(trainX, trainY)
    # print testX
    return clf.predict([testX])[0]


def refresh(id, base):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
    #                      port=3306, charset="utf8")
    cursor = db.cursor()
    # cursor.execute('update `forecast` set `unstable` = 1 where `date`>= "2016-06-09"')
    cursor.execute('select MAX(`date`) from `svm_forecast` where `stockid` = "'+id+'" and `unstable` = 0')
    startdate = list(cursor.fetchall())[0][0]
    # if startdate is None:
    #     print id + ' empty table'
    #     return
    # start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    # end = datetime.datetime.now()
    # # end = end + datetime.timedelta(-1)
    # enddate = end.strftime("%Y-%m-%d")
    start = datetime.datetime.now()
    startdate = start.strftime('%Y-%m-%d')
    end = start + datetime.timedelta(1)
    enddate = end.strftime('%Y-%m-%d')

    # start = datetime.datetime.strptime(startdate, '%Y-%m-%d')

    cursor.execute('delete from `svm_forecast` where `stockid` = "'+id+'" and `unstable` = 1')
    cursor.execute('select `close` from `stock_2016` where `stockid` = "' + id + '" and `date` <= "' + startdate + '"')

    fetch = list(cursor.fetchall())
    for every in fetch:
        base.append(every[0])

    # start = start + datetime.timedelta(1)

    # print len(base)

    data = []
    predict = []
    while start.strftime("%Y-%m-%d") != enddate:
        # print base
        # temp = []
        date = start.strftime("%Y-%m-%d")
        temp = [id, date]
        start = start + datetime.timedelta(1)
        # print date
        cursor.execute(
            'select `close` from `stock_2016` where `stockid` = "' + id + '" and `date` = "' + date + '" and `amount`>0')
        tempdata = list(cursor.fetchall())
        if len(tempdata) != 0:

            base.append(tempdata[0][0])
        else:
            continue

        trainX = []
        trainY = []
        # print base
        for x in range(0, len(base) - 10):
            tempX = base[x:x + 10]
            tempY = base[x + 10]
                # sum = 0
                # # for every in tempX:
                # #     sum += every
                # for k in range(0, len(tempX)):
                #     # tempX[k] = float(tempX[k]) / float(sum)
                #     if tempX[k] >= 0:
                #         tempX[k] = 1
                #     else:
                #         tempX[k] = 0
                # tempY = float(tempY) / float(sum)
            trainX.append(tempX)
            trainY.append(tempY)
        testX = base[-10:]
            # sum = 0
            # for every in testX:
            #     sum += every
            # for i in range(0, len(testX)):
            #     testX[i] = float(testX[i]) / float(sum)

        price = get_predict(trainX, trainY, testX)
        # print price
        var = np.var(np.array(base)) / len(base)
        d = np.sqrt(var)
        temp.append(get_predict(trainX, trainY, testX))
        predict.append(price)
        temp.append(price + d)
        temp.append(price - d)
        temp.append(0)
        data.append(temp)
        # base.append(each)

    # print predict
    if len(predict) == 0:
        cursor.execute('select `price_middle` from `svm_forecast` where `stockid` = "'+id+'" order by `date` desc limit 0, 1')
        temp = list(cursor.fetchall())
        if temp:
            predict.append(temp[0][0])
    for x in range(0, 15):
        date = start.strftime("%Y-%m-%d")
        temp = [id, date]
        start = start + datetime.timedelta(1)
        if not predict:
            continue
        base.append(predict[-1])
        trainX = []
        trainY = []
        # print base
        for x in range(0, len(base) - 10):
            tempX = base[x:x + 10]
            tempY = base[x + 10]
            # sum = 0
            # # for every in tempX:
            # #     sum += every
            # for k in range(0, len(tempX)):
            #     # tempX[k] = float(tempX[k]) / float(sum)
            #     if tempX[k] >= 0:
            #         tempX[k] = 1
            #     else:
            #         tempX[k] = 0
            # tempY = float(tempY) / float(sum)
            trainX.append(tempX)
            trainY.append(tempY)
        testX = base[-10:]
        # sum = 0
        # for every in testX:
        #     sum += every
        # for i in range(0, len(testX)):
        #     testX[i] = float(testX[i]) / float(sum)
        price = get_predict(trainX, trainY, testX)
        var = np.var(np.array(base)) / len(base)
        d = np.sqrt(var)
        temp.append(get_predict(trainX, trainY, testX))
        predict.append(price)
        temp.append(price + d)
        temp.append(price - d)
        temp.append(1)
        data.append(temp)
    # print data
    insert_cmd = 'insert into `svm_forecast` (`stockid`, `date`, `price_middle`, `price_high`, `price_low`, `unstable`) ' \
                 ' VALUES (%s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, tuple(data))
    db.commit()
    db.close()


def refresh_svm():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    # file = open('/root/python_script/full_list.txt')
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        # if line[:8] == 'sz002736':
        #     continue
        print 'svm: ' + line[:8]
        cursor.execute('select `close` from `stock_2015` where `stockid` = "' + line[:8] + '" and `amount` > 0')
        raw = list(cursor.fetchall())

        base = []
        for each in raw:
            base.append(each[0])
        refresh(line[:8], base)

# refresh_svm()

# cursor.execute('select `close` from `stock_2015` where `stockid` = "' + 'sz002644' + '" and `amount` > 0')
# raw = list(cursor.fetchall())
#
# base = []
# for each in raw:
#     base.append(each[0])
# refresh('sz002644', base)
