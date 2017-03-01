import MySQLdb
import datetime
import numpy as np
from sklearn import svm


def get_predict(trainX, trainY, testX):
    clf = svm.SVR()
    clf.fit(trainX, trainY)
    # print testX
    return clf.predict([testX])[0]
    # trainX = np.array(trainX)
    # trainY = np.array(trainY).T
    # syn0 = 2 * np.random.random((10, 1)) - 1
    #
    # for iter in xrange(5000):
    #     # forward propagation
    #     l0 = trainX
    #     l1 = nonlin(np.dot(l0, syn0))
    #
    #     # how much did we miss?
    #     l1_error = trainY - l1
    #
    #     # multiply how much we missed by the
    #     # slope of the sigmoid at the values in l1
    #     l1_delta = l1_error * nonlin(l1, True)
    #
    #     # update weights
    #     syn0 += np.dot(l0.T, l1_delta)
    #     # print "Output After Training:"
    #     # print l1
    #     # print syn0
    #     #
    #
    # # syn1 = 2 * np.random.random((13, 1)) - 1
    # # for j in xrange(10000):
    # #
    # #     l1 = 1 / (1 + np.exp(-(np.dot(trainX, syn0))))
    #     # l2 = 1 / (1 + np.exp(-(np.dot(l1, syn1))))
    #     # l2_delta = (trainY - l2) * (l2 * (1 - l2))
    #     # l1_delta = l2_delta.dot(syn1.T) * (l1 * (1 - l1))
    #     # syn1 += l1.T.dot(l2_delta)
    #     # syn0 += trainX.T.dot(l1_delta)
    # l1 = 1 / (1 + np.exp(-(np.dot(testX, syn0))))
    # return l1[0][0]


def draw(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    # cursor.execute('select `close`, `date` from `stock_2014` where `amount` >0  and `stockid` = "'+id+'"')
    cursor.execute('select `close`, `date` from `bench` where `stockid` = "'+id+'" and '
                   '`date`>="2014-01-01" and `date`<="2014-12-31"')
    raw = list(cursor.fetchall())
    base = []
    for each in raw:
        base.append(each[0])
    #     if each[0] >= 0:
    #         base.append(1)
    #     else:
    #         base.append(0)
    actual = []
    date = []
    # cursor.execute('select `close`, `date` from `stock_2015` where `amount` >0  and `stockid` = "'+id+'"')
    cursor.execute('select `close`, `date` from `bench` where `stockid` = "'+id+'" and '
                   '`date`>="2015-01-01"')
    raw = list(cursor.fetchall())

    for each in raw:
        actual.append(each[0])
        date.append(each[1])

    # cursor.execute('select `close`, `date` from `stock_2016` where `amount` >0  and `stockid` = "' + id + '"')
    # raw = list(cursor.fetchall())

    for each in raw:
        actual.append(each[0])
        date.append(each[1])
        # if each[0] >= 0:
        #     actual.append(1)
        # else:
        #     actual.append(0)
    predict = []
    predict_upper = []
    predict_low = []
    X = []
    j = 1
    for each in actual:
        X.append(j)
        j += 1
        trainX = []
        trainY = []
        for x in range(0, len(base)-10):
            tempX = base[x:x+10]
            tempY = base[x+10]
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
        predict.append(get_predict(trainX, trainY, testX))
        predict_upper.append(price+d)
        predict_low.append(price-d)
        base.append(each)

    time = date[-1]
    time = datetime.datetime.strptime(time, "%Y-%m-%d")
    time = time + datetime.timedelta(1)
    for x in range(0, 14):
        date.append(time.strftime("%Y-%m-%d"))
        time = time + datetime.timedelta(1)
        base.append(predict[-1])
        X.append(j)
        j += 1
        trainX = []
        trainY = []
        for x in range(0, len(base) - 10):
            tempX = base[x:x + 10]
            tempY = base[x + 10]
            # sum = 0
            # for every in tempX:
            #     sum += every
            # for k in range(0, len(tempX)):
            #     tempX[k] = float(tempX[k]) / float(sum)
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
        predict.append(get_predict(trainX, trainY, testX))
        predict_upper.append(price + d)
        predict_low.append(price - d)

    # print actual
    # print predict
    # print X

    # hit = 0
    # total = 0
    #
    # for x in range(0, len(actual)):
    #     total += 1
    #     if actual[x] == 1 and predict[x] > 0.5:
    #         hit += 1
    #     elif actual[x] == 0 and predict[x] < 0.5:
    #         hit += 1
    # print float(hit) / float(total)

    # plt.plot(X[:-14], actual, 'g')
    # plt.plot(X, predict, 'r')
    # plt.plot(X, predict_upper, 'b')
    # plt.plot(X, predict_low, 'y')
    # # plt.ylim(-4, 4)
    # # plt.xlim(200, 300)
    # plt.show()
    # data = []
    for x in range(0, len(date)):
        # data.append([id, date[x], predict[x], predict_upper[x], predict_low[x]])
        cursor.execute('update `forecast` set `price_middle` = %s, `price_high` = %s, `price_low` = %s '
        ' where `stockid` = "%s" and `date` = "%s"' % (predict[x], predict_upper[x], predict_low[x], id, date[x]))
    # insert_cmd = 'insert into `forecast` (`stockid`, `date`, `price_middle`, `price_high`, `price_low`) ' \
    #              ' VALUES (%s, %s, %s, %s, %s)'

    # cursor.executemany(insert_cmd, tuple(data))
    # db.commit()
    db.close()

draw('hs300')
# file = open('list.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     print line[:8]
#     try:
#         draw(line[:8])
#     except:
#         print 'no data ' + line[:8]
