import MySQLdb
import datetime
import numpy as np
import matplotlib.pyplot as plt

def nonlin(x, deriv=False):
    if deriv == True:
        return x*(1-x)
    return 1/(1+np.exp(-x))


def get_data(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    # end = datetime.datetime.now()
    # start = end + datetime.timedelta(days=-200)
    # enddate = end.strftime('%Y-%m-%d')
    # startdate = start.strftime('%Y-%m-%d')
    # startdate = '2015-05-01'
    # enddate = '2016-05-20'

    select_cmd = 'select `close` from `stock_2014` where `stockid` = "'+id+'"'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        if each[0] != 0:
            close.append(each[0])
    # print close
    return close


def init(id):
    predict = []
    actual = []
    close = get_data(id)
    X = []
    Y = []

    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    # cursor.execute('select MIN(`date`) from `stock_2015` where `stockid` = "'+id+'" and `amount` > 0')
    # startdate = list(cursor.fetchall())[0][0]
    # cursor.execute('select `close` from `stock_2015` where `stockid` = "'+id+'" and `date` = "'+startdate+'"')
    # price = float(list(cursor.fetchall())[0][0])
    # actual.append(price)

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
    print X
    print Y
    np.random.seed(0)
    syn0 = 2 * np.random.random((5, 1)) - 1

    for iter in xrange(100):
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

    l1 = nonlin(np.dot(testX, syn0))[0]
    # predict.append(l1)

    cursor.execute('( select `close`, `date` from `stock_2015` where `stockid` = "'+id+'" and `amount` > 0 ) union ( select `close`, `date` from `stock_2016` where `stockid` = "'+id+'" and `amount` > 0)')
    raw = list(cursor.fetchall())
    date = []
    for each in raw:
        actual.append(each[0])
        date.append(each[1])

    X = []

    for x in range(0, len(raw)):
        result = recursion(close[-6:], syn0)
        syn0 = result[1]
        close.append(actual[x])
        predict.append(result[0])
        X.append(x)
    print len(date)
    print len(predict)
    data = []
    for x in range(0, len(date)):
        data.append([id, date[x], predict[x]])
    cursor.executemany('insert into `future` (`stockid`, `date`, `price`) VALUES (%s, %s, %s)', tuple(data))
    db.commit()
    db.close()
    # plt.plot(X, predict, 'r')
    # plt.plot(X, actual, 'g')
    #
    # plt.show()
    # print predict


def recursion(close, syn0):
    X = []
    Y = []
    sum = 0
    for x in range(0, 5):
        sum += close[x]
    for x in range(0, 5):
        X.append(close[x]/sum)
    Y.append(close[5]/sum)
    X = np.array([X])
    Y = np.array([Y]).T
    for iter in xrange(100):
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
    testX = close[-5:]

    sum = 0
    for each in testX:
        sum += each
    for x in range(0, len(testX)):
        testX[x] = testX[x] / float(sum)

    l1 = nonlin(np.dot(testX, syn0))[0]*sum
    return [l1, syn0]

file = open('StockList.txt')
# file = open('list.txt')
while 1:
    line = file.readline()
    if not line:
        break
    try:
        init(line[:8])
    except Exception, e:
        print e
