from sklearn import svm

import MySQLdb
import datetime
import numpy as np


def get_svm_predict(trainX, trainY, testX):
    clf = svm.SVR()
    clf.fit(trainX, trainY)
    # print testX
    return clf.predict([testX])[0]


def refresh_svm(id, base):
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
    startdate = '2016-07-29'
    enddate = '2016-07-30'
    start = datetime.datetime.strptime(startdate, '%Y-%m-%d')

    cursor.execute('delete from `svm_forecast` where `stockid` = "'+id+'" and `unstable` = 1')
    cursor.execute('select `close` from `bench` where `stockid` = "' + id + '" and `date` <= "' + startdate + '"')

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
            'select `close` from `bench` where `stockid` = "' + id + '" and `date` = "' + date + '" and `amount`>0')
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

            trainX.append(tempX)
            trainY.append(tempY)
        testX = base[-10:]

        price = get_svm_predict(trainX, trainY, testX)
        # print price
        var = np.var(np.array(base)) / len(base)
        d = np.sqrt(var)
        temp.append(get_svm_predict(trainX, trainY, testX))
        predict.append(price)
        temp.append(price + d)
        temp.append(price - d)
        temp.append(0)
        data.append(temp)
        # base.append(each)

        # print predict
    if len(predict) == 0:
        cursor.execute(
            'select `price_middle` from `forecast` where `stockid` = "' + id + '" order by `date` desc limit 0, 1')
        predict.append(list(cursor.fetchall())[0][0])
    for x in range(0, 15):
        date = start.strftime("%Y-%m-%d")
        temp = [id, date]
        start = start + datetime.timedelta(1)
        base.append(predict[-1])
        trainX = []
        trainY = []
        # print base
        for x in range(0, len(base) - 10):
            tempX = base[x:x + 10]
            tempY = base[x + 10]
            sum = 0
            for every in tempX:
                sum += every
            for k in range(0, len(tempX)):
                tempX[k] = float(tempX[k]) / float(sum)
                if tempX[k] >= 0:
                    tempX[k] = 1
                else:
                    tempX[k] = 0
            tempY = float(tempY) / float(sum)
            trainX.append(tempX)
            trainY.append(tempY)
        testX = base[-10:]
        sum = 0
        for every in testX:
            sum += every
        for i in range(0, len(testX)):
            testX[i] = float(testX[i]) / float(sum)
        price = get_svm_predict(trainX, trainY, testX) * sum
        var = np.var(np.array(base)) / len(base)
        d = np.sqrt(var)
        temp.append(price)
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


def nonlin(x, deriv=False):
    if deriv == True:
        return x*(1-x)
    return 1/(1+np.exp(-x))


def get_bp_predict(trainX, trainY, testX):
    # clf = svm.SVR()
    # clf.fit(trainX, trainY)
    # # print testX
    # return clf.predict([testX])[0]
    trainX = np.array(trainX)
    trainY = np.array(trainY).T
    syn0 = 2 * np.random.random((10, 1)) - 1

    for iter in xrange(100):
        # forward propagation
        l0 = trainX
        l1 = nonlin(np.dot(l0, syn0))

        # how much did we miss?
        l1_error = trainY - l1

        # multiply how much we missed by the
        # slope of the sigmoid at the values in l1
        l1_delta = l1_error * nonlin(l1, True)

        # update weights
        syn0 += np.dot(l0.T, l1_delta)
        # print "Output After Training:"
        # print l1
        # print syn0
        #

    # syn1 = 2 * np.random.random((13, 1)) - 1
    # for j in xrange(10000):
    #
    #     l1 = 1 / (1 + np.exp(-(np.dot(trainX, syn0))))
        # l2 = 1 / (1 + np.exp(-(np.dot(l1, syn1))))
        # l2_delta = (trainY - l2) * (l2 * (1 - l2))
        # l1_delta = l2_delta.dot(syn1.T) * (l1 * (1 - l1))
        # syn1 += l1.T.dot(l2_delta)
        # syn0 += trainX.T.dot(l1_delta)
    l1 = 1 / (1 + np.exp(-(np.dot(testX, syn0))))
    # print l1
    return l1[0]


def refresh_bp(id, base):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
    #                      port=3306, charset="utf8")
    cursor = db.cursor()
    # cursor.execute('update `forecast` set `unstable` = 1 where `date`>= "2016-06-09"')
    cursor.execute('select MAX(`date`) from `bp_forecast` where `stockid` = "'+id+'" and `unstable` = 0')
    # startdate = list(cursor.fetchall())[0][0]
    # if startdate is None:
    #     print id + ' empty table'
    #     return
    # start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    # end = datetime.datetime.now()
    # # end = end + datetime.timedelta(-1)
    # enddate = end.strftime("%Y-%m-%d")
    # startdate = '2016-07-29'
    # enddate = '2016-07-30'
    # start = datetime.datetime.strptime(startdate, '%Y-%m-%d')
    start = datetime.datetime.now()
    startdate = start.strftime('%Y-%m-%d')
    end = start + datetime.timedelta(1)
    enddate = end.strftime('%Y-%m-%d')

    cursor.execute('delete from `bp_forecast` where `stockid` = "'+id+'" and `unstable` = 1')
    cursor.execute('select `close` from `bench` where `stockid` = "' + id + '" and `date` <= "' + startdate + '"')

    fetch = list(cursor.fetchall())
    for every in fetch:
        base.append(every[0])

    # start = start + datetime.timedelta(1)

    # print (base)

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
            'select `close` from `bench` where `stockid` = "' + id + '" and `date` = "' + date + '" and `amount`>0')
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
            sum = 0
            for every in tempX:
                sum += every
            # print sum
            if sum == 0:
                continue
            else:
                for k in range(0, len(tempX)):
                    tempX[k] = float(tempX[k]) / float(sum)
                #     if tempX[k] >= 0:
                #         tempX[k] = 1
                #     else:
                #         tempX[k] = 0
                tempY = float(tempY) / float(sum)
                trainX.append(tempX)
                trainY.append(tempY)
        testX = base[-10:]
        sum = 0
        for every in testX:
            sum += every
        for i in range(0, len(testX)):
            testX[i] = float(testX[i]) / float(sum)

        price = get_bp_predict(trainX, [trainY], testX)*sum
        # print price
        var = np.var(np.array(base)) / len(base)
        d = np.sqrt(var)
        temp.append(price)
        predict.append(price)
        temp.append(price + d)
        temp.append(price - d)
        temp.append(0)
        data.append(temp)
        # base.append(each)

    # print predict
    if len(predict) == 0:
        cursor.execute('select `price_middle` from `bp_forecast` where `stockid` = "'+id+'" order by `date` desc limit 0, 1')
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
        for x in range(0, len(base) - 10):
            tempX = base[x:x + 10]
            tempY = base[x + 10]
            sum = 0
            for every in tempX:
                sum += every
            if sum == 0:
                continue
            else:
                for k in range(0, len(tempX)):
                    tempX[k] = float(tempX[k]) / float(sum)
            #     if tempX[k] >= 0:
            #         tempX[k] = 1
            #     else:
            #         tempX[k] = 0
                tempY = float(tempY) / float(sum)
                trainX.append(tempX)
                trainY.append(tempY)
        testX = base[-10:]
        sum = 0
        for every in testX:
            sum += every
        for i in range(0, len(testX)):
            testX[i] = float(testX[i]) / float(sum)
        price = get_bp_predict(trainX, [trainY], testX)*sum
        var = np.var(np.array(base)) / len(base)
        d = np.sqrt(var)
        temp.append(price)
        predict.append(price)
        temp.append(price + d)
        temp.append(price - d)
        temp.append(1)
        # print temp
        data.append(temp)
    # print data
    insert_cmd = 'insert into `bp_forecast` (`stockid`, `date`, `price_middle`, `price_high`, `price_low`, `unstable`) ' \
                 ' VALUES (%s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, tuple(data))
    db.commit()
    db.close()


def refresh_benchforecast():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    for each in ['000001', '399001', '399300']:
        # if line[:8] == 'sz002736':
        #     continue
        print 'bench_forecast: ' + each
        cursor.execute('select `close` from `bench` where `stockid` = "' + each + '" '
                ' and `date` >= "2015-01-01" and `date`<="2015-12-31"')
        raw = list(cursor.fetchall())

        base = []
        for ele in raw:
            base.append(ele[0])
        refresh_svm(each, base)
        refresh_bp(each, base)

# refresh_benchforecast()