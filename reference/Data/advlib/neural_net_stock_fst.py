import MySQLdb
import datetime
import time
import matplotlib
import matplotlib.pyplot as plt
import numpy as np


def nonlin(x, deriv=False):
    if deriv == True:
        return x*(1-x)
    return 1/(1+np.exp(-x))


def get_drawing_data(id, date):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

# end = datetime.datetime.now()
# start = end + datetime.timedelta(days=-400)
# enddate = end.strftime('%Y-%m-%d')
# startdate = start.strftime('%Y-%m-%d')
# startdate = '2015-05-01'
# enddate = '2016-05-20'

    select_cmd = 'select `close` from `stock_2014` where `stockid` = "' + id + '" and `date` > "2014-06-01" order by `date` '
                # ' select `close` from `stock_2015` where `stockid` = "' + id + '" order by `date` where `date` < "'+date+'"'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        close.append(each[0])
    select_cmd = ' select `close` from `stock_2015` where `stockid` = "' + id + '" and `date` < "'+date+'"'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    for each in raw:
        close.append(each[0])
    return close


def get_data(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    # end = datetime.datetime.now()
    # start = end + datetime.timedelta(days=-400)
    # enddate = end.strftime('%Y-%m-%d')
    # startdate = start.strftime('%Y-%m-%d')
    # startdate = '2015-05-01'
    # enddate = '2016-05-20'

    select_cmd = ' (select `close` from `stock_2015` where `stockid` = "'+id+'" order by `date`) union ' \
                ' (select `close` from `stock_2016` where `stockid` = "'+id+'" order by `date`)'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        close.append(each[0])
    return close


def establish(id):
    close = get_data(id)
    # print close
    X = []
    Y = []
    for n in range(0, len(close) - 6):
        tempX = close[n:n + 5]
        sum = 0
        for each in tempX:
            sum += each
        for i in range(0, len(tempX)):
            tempX[i] /= float(sum)
        tempY = close[n + 5] / float(sum)
        X.append(tempX)
        Y.append(tempY)

    X = np.array(X)
    Y = np.array([Y]).T
    #
    # print X
    # print Y

    if len(X) == 0:
        data = ['"'+id+'"', 0, 0, 0, 0, 0, 0]
    else:
        np.random.seed(0)
        syn0 = 2 * np.random.random((5, 1)) - 1

        for iter in xrange(1000):
        # forward propagation
            l0 = X
            l1 = nonlin(np.dot(l0, syn0))

        # how much did we miss?
            l1_error = Y - l1

        # multiply how much we missed by the
        # slope of the sigmoid at the values in l1
            l1_delta = l1_error * nonlin(l1, True)

        # update weights
            syn0 += np.dot(l0.T, l1_delta)
    # print "Output After Training:"
    # print l1
    # print syn0
    #
        testX = close[-5:]
        sum = 0
        for each in testX:
            sum += each
        for x in range(0, len(testX)):
            testX[x] = testX[x] / float(sum)

        l1 = nonlin(np.dot(testX, syn0))
    # print l1[0] * sum
        data = ['"'+id+'"']
        for each in syn0:
            data.append(each[0])
        data.append(l1[0] * sum)
    # print data
    insert_cmd = 'insert into `bp_predict` (`stockid`, `w11`, `w12`, `w13`, `w14`, `w15`, `price`) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s, %s)' % tuple(data)
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    cursor.execute(insert_cmd)
    db.commit()
    db.close()


def draw(id, close):
    # print close
    X = []
    Y = []
    for n in range(0, len(close) - 7):
        tempX = close[n:n + 5]
        sum = 0
        for each in tempX:
            sum += each
        for i in range(0, len(tempX)):
            tempX[i] /= float(sum)
        tempY = close[n + 5] / float(sum)
        X.append(tempX)
        Y.append(tempY)

    X = np.array(X)
    Y = np.array([Y]).T
    #
    # print X
    # print Y

    if len(X) == 0:
        data = ['"' + id + '"', 0, 0, 0, 0, 0, 0]
    else:
        np.random.seed(0)
        syn0 = 2 * np.random.random((5, 1)) - 1

        for iter in xrange(1000):
            # forward propagation
            l0 = X
            l1 = nonlin(np.dot(l0, syn0))

            # how much did we miss?
            l1_error = Y - l1

            # multiply how much we missed by the
            # slope of the sigmoid at the values in l1
            l1_delta = l1_error * nonlin(l1, True)

            # update weights
            syn0 += np.dot(l0.T, l1_delta)
            # print "Output After Training:"
            # print l1
            # print syn0
            #
        testX = close[-6: -1]
        sum = 0
        for each in testX:
            sum += each
        for x in range(0, len(testX)):
            testX[x] = testX[x] / float(sum)

        l1 = nonlin(np.dot(testX, syn0))
        # print l1[0] * sum
        return [l1[0]*sum, close[-1]]
    # print data

startdate = '2015-01-11'
start = datetime.datetime.strptime(startdate, "%Y-%m-%d")
predict = []
actual = []
X = []
for x in range(0, 300):
    date = start.strftime("%Y-%m-%d")
    close = get_drawing_data('sh600000', date)
    temp = draw('sh600000', close)
    predict.append(temp[0])
    actual.append(temp[1])
    X.append(x)
    start = start + datetime.timedelta(1)
print predict
print actual
plt.plot(X, predict, 'g')
plt.plot(X, actual, 'b')
plt.show()
# file = open('StockList.txt')
# while 1:
#     line = file.readline()
#     if not line:
#         break
#     print line[:8]
#     establish(line[:8])
# establish('sh601268')