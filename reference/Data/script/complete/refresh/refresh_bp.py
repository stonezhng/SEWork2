from sklearn import svm

import MySQLdb
import datetime
import numpy as np


def nonlin(x, deriv=False):
    if deriv == True:
        return x*(1-x)
    return 1/(1+np.exp(-x))


def get_predict(trainX, trainY, testX):
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


def refresh(id, base):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
    #                      port=3306, charset="utf8")
    cursor = db.cursor()
    # cursor.execute('update `forecast` set `unstable` = 1 where `date`>= "2016-06-09"')
    cursor.execute('select MAX(`date`) from `bp_forecast` where `stockid` = "'+id+'" and `unstable` = 0')
    startdate = list(cursor.fetchall())[0][0]
    if startdate is None:
        print id + ' empty table'
        return
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    end = datetime.datetime.now()
    # end = end + datetime.timedelta(-1)
    enddate = end.strftime("%Y-%m-%d")

    cursor.execute('delete from `bp_forecast` where `stockid` = "'+id+'" and `unstable` = 1')
    cursor.execute('select `close` from `stock_2016` where `stockid` = "' + id + '" and `date` <= "' + startdate + '"')

    fetch = list(cursor.fetchall())
    for every in fetch:
        base.append(every[0])

    start = start + datetime.timedelta(1)

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

        price = get_predict(trainX, [trainY], testX)*sum
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
        predict.append(list(cursor.fetchall())[0][0])
    for x in range(0, 15):
        date = start.strftime("%Y-%m-%d")
        temp = [id, date]
        start = start + datetime.timedelta(1)
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
        price = get_predict(trainX, [trainY], testX)*sum
        var = np.var(np.array(base)) / len(base)
        d = np.sqrt(var)
        temp.append(price)
        predict.append(price)
        temp.append(price + d)
        temp.append(price - d)
        temp.append(1)
        data.append(temp)
    # print data
    insert_cmd = 'insert into `bp_forecast` (`stockid`, `date`, `price_middle`, `price_high`, `price_low`, `unstable`) ' \
                 ' VALUES (%s, %s, %s, %s, %s, %s)'
    cursor.executemany(insert_cmd, tuple(data))
    db.commit()
    db.close()


db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                     port=8161, charset="utf8")
cursor = db.cursor()

file = open('StockList.txt')
while 1:
    line = file.readline()
    if not line:
        break
    if line[:8] == 'sz002736':
        continue
    print line[:8]
    cursor.execute('select `close` from `stock_2015` where `stockid` = "' + line[:8] + '" and `amount` > 0')
    raw = list(cursor.fetchall())

    base = []
    for each in raw:
        base.append(each[0])
    refresh(line[:8], base)

# cursor.execute('select `close` from `stock_2015` where `stockid` = "' + 'sz002664' + '" and `amount` > 0')
# raw = list(cursor.fetchall())
#
# base = []
# for each in raw:
#     base.append(each[0])
# refresh('sz002664', base)


# import MySQLdb
# import datetime
# import numpy as np
#
#
# def nonlin(x, deriv=False):
#     if deriv == True:
#         return x*(1-x)
#     return 1/(1+np.exp(-x))
#
#
# def get_data(id):
#     db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
#                          port=8161)
#     cursor = db.cursor()
#
#     end = datetime.datetime.now()
#     start = end + datetime.timedelta(days=-200)
#     enddate = end.strftime('%Y-%m-%d')
#     startdate = start.strftime('%Y-%m-%d')
#     # startdate = '2015-05-01'
#     # enddate = '2016-05-20'
#
#     select_cmd = 'select `close` from `stock_2016` where `stockid` = "'+id+'" and `date` < "'+enddate+'" order by `date`'
#     cursor.execute(select_cmd)
#     raw = list(cursor.fetchall())
#     close = []
#     for each in raw:
#         if each[0] != 0:
#             close.append(each[0])
#     # print close
#     return close
#
#
# def refresh(id):
#     close = get_data(id)
#     # print close
#     X = []
#     Y = []
#     for n in range(0, len(close) - 6):
#         tempX = close[n:n + 5]
#         sum = 0
#         for each in tempX:
#             sum += each
#         for i in range(0, len(tempX)):
#             tempX[i] /= float(sum)
#         tempY = close[n + 5] / float(sum)
#         X.append(tempX)
#         Y.append(tempY)
#
#     X = np.array(X)
#     Y = np.array([Y]).T
#     # print X
#     # print Y
#
#     if len(X) == 0:
#         data = ['"' + id + '"', 0, 0, 0, 0, 0, 0]
#         close.append(0)
#         syn0 = np.array([[0], [0], [0], [0], [0]]).T
#     else:
#         np.random.seed(0)
#         syn0 = 2 * np.random.random((5, 1)) - 1
#
#         for iter in xrange(1000):
#             # forward propagation
#             l0 = X
#             l1 = nonlin(np.dot(l0, syn0))
#
#             # how much did we miss?
#             l1_error = Y - l1
#
#             # multiply how much we missed by the
#             # slope of the sigmoid at the values in l1
#             l1_delta = l1_error * nonlin(l1, True)
#
#             # update weights
#             syn0 += np.dot(l0.T, l1_delta)
#             # print "Output After Training:"
#             # print l1
#             # print syn0
#             #
#         testX = close[-5:]
#         sum = 0
#         for each in testX:
#             sum += each
#         for x in range(0, len(testX)):
#             testX[x] = testX[x] / float(sum)
#
#         l1 = nonlin(np.dot(testX, syn0))
#         # print l1[0] * sum
#         data = ['"' + id + '"']
#         for each in syn0:
#             data.append(each[0])
#         data.append(l1[0] * sum)
#
#         close.append(l1[0] * sum)
#     # print data
#     insert_cmd = 'insert into `bp_predict` (`stockid`, `w11`, `w12`, `w13`, `w14`, `w15`, `price`) ' \
#                  'VALUES (%s, %s, %s, %s, %s, %s, %s)' % tuple(data)
#     db = MySQLdb.connect(host="572b2568442c7.sh.cdb.myqcloud.com", user="cdb_outerroot", passwd="software2015", db="test",
#                          port=8161)
#     cursor = db.cursor()
#     try:
#         cursor.execute('delete from `bp_predict` where `stockid` = "'+id+'"')
#     except:
#         1
#     cursor.execute(insert_cmd)
#     db.commit()
#
#     close = close[-6:]
#     predict = []
#     for x in range(0, 14):
#         # print close
#         if len(close) == 6:
#             result = pre_future(close, syn0)
#         else:
#             result = [[0], [0]]
#         close = result[0]
#         syn0 = result[1]
#         predict.append(close[-1])
#     predict.append(id)
#     # print predict
#     cursor.execute('update `bp_predict` set `price1`= %s, `price2`= %s, `price3`= %s, `price4`= %s, `price5`= %s, '
#                    ' `price6`= %s, `price7`= %s, `price8`= %s, `price9`= %s, `price10`= %s, `price11`= %s, '
#                    ' `price12`= %s, `price13`= %s, `price14`= %s where `stockid` = "%s"' % tuple(predict))
#     # pre_future(id, close[-6:], syn0)
#     db.close()
#
#
# def pre_future(close, syn0):
#     X = []
#     Y = []
#     # print close
#
#     tempX = close[0:5]
#     tempY = close[5]
#     sum = 0
#     for each in tempX:
#         sum += each
#     if sum != 0:
#         for i in range(0, len(tempX)):
#             tempX[i] /= float(sum)
#         tempY = close[5] / float(sum)
#     X.append(tempX)
#     Y.append(tempY)
#
#     X = np.array(X)
#     Y = np.array([Y]).T
#     #
#     # print X
#     # print Y
#
#     if len(X) == 0:
#         return [close, syn0]
#     else:
#         np.random.seed(0)
#         syn0 = 2 * np.random.random((5, 1)) - 1
#
#         for iter in xrange(1000):
#             # forward propagation
#             l0 = X
#             l1 = nonlin(np.dot(l0, syn0))
#
#             # how much did we miss?
#             l1_error = Y - l1
#
#             # multiply how much we missed by the
#             # slope of the sigmoid at the values in l1
#             l1_delta = l1_error * nonlin(l1, True)
#
#             # update weights
#             syn0 += np.dot(l0.T, l1_delta)
#         testX = close[-5:]
#         sum = 0
#         for each in testX:
#             sum += each
#         for x in range(0, len(testX)):
#             testX[x] = testX[x] / float(sum)
#
#         l1 = nonlin(np.dot(testX, syn0))
#         close.append(l1[0]*sum)
#         # print close
#         return [close[-6:], syn0]
#
#
# def another_refresh(id):
#     db = MySQLdb.connect(host="572b2568442c7.sh.cdb.myqcloud.com", user="cdb_outerroot", passwd="software2015",
#                          db="test",
#                          port=8161)
#     cursor = db.cursor()
#     cursor.execute('select `w11`, `w12`, `w13`, `w14`, `w15` from `bp_predict` where `stockid` = "'+id+'"')
#     syn0 = []
#     raw = list(cursor.fetchall())
#     for each in raw:
#         syn0.append(each[0])
#     syn0 = np.array([syn0]).T
#
#
#
# file = open('StockList.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     print line[:8]
#     refresh(line[:8])
# # establish('sh601268')
# # establish('sh600000')