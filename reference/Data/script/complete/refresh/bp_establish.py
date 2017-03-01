import MySQLdb
import datetime
import numpy as np


def nonlin(x, deriv=False):
    if deriv == True:
        return x*(1-x)
    return 1/(1+np.exp(-x))


def get_data(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    end = datetime.datetime.now()
    start = end + datetime.timedelta(days=-200)
    enddate = end.strftime('%Y-%m-%d')
    startdate = start.strftime('%Y-%m-%d')
    # startdate = '2015-05-01'
    # enddate = '2016-05-20'
    select_cmd = 'select `close` from `bench` where `stockid` = "' + id + '" and `date` < "' + enddate + '" ' \
                'and `date` >= "2014-01-01" order by `date`'
    # select_cmd = 'select `close` from `stock_2016` where `stockid` = "'+id+'" and `date` < "'+enddate+'" order by `date`'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        if each[0] != 0:
            close.append(each[0])
    # print close
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
        data = ['"' + id + '"', 0, 0, 0, 0, 0, 0]
        close.append(0)
        syn0 = np.array([[0], [0], [0], [0], [0]]).T
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
        data = ['"' + id + '"']
        for each in syn0:
            data.append(each[0])
        data.append(l1[0] * sum)

        close.append(l1[0] * sum)
    # print data
    insert_cmd = 'insert into `bp_predict` (`stockid`, `w11`, `w12`, `w13`, `w14`, `w15`, `price`) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s, %s)' % tuple(data)
    db = MySQLdb.connect(host="572b2568442c7.sh.cdb.myqcloud.com", user="cdb_outerroot", passwd="software2015", db="test",
                         port=8161)
    cursor = db.cursor()
    try:
        cursor.execute('delete from `bp_predict` where `stockid` = "'+id+'"')
    except:
        1
    cursor.execute(insert_cmd)
    db.commit()

    close = close[-6:]
    predict = []
    for x in range(0, 14):
        # print close
        if len(close) == 6:
            result = pre_future(close, syn0)
        else:
            result = [[0], [0]]
        close = result[0]
        syn0 = result[1]
        predict.append(close[-1])
    predict.append(id)
    # print predict
    cursor.execute('update `bp_predict` set `price1`= %s, `price2`= %s, `price3`= %s, `price4`= %s, `price5`= %s, '
                   ' `price6`= %s, `price7`= %s, `price8`= %s, `price9`= %s, `price10`= %s, `price11`= %s, '
                   ' `price12`= %s, `price13`= %s, `price14`= %s where `stockid` = "%s"' % tuple(predict))
    # pre_future(id, close[-6:], syn0)
    db.close()


def pre_future(close, syn0):
    X = []
    Y = []
    # print close

    tempX = close[0:5]
    tempY = close[5]
    sum = 0
    for each in tempX:
        sum += each
    if sum != 0:
        for i in range(0, len(tempX)):
            tempX[i] /= float(sum)
        tempY = close[5] / float(sum)
    X.append(tempX)
    Y.append(tempY)

    X = np.array(X)
    Y = np.array([Y]).T
    #
    # print X
    # print Y

    if len(X) == 0:
        return [close, syn0]
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
        testX = close[-5:]
        sum = 0
        for each in testX:
            sum += each
        for x in range(0, len(testX)):
            testX[x] = testX[x] / float(sum)

        l1 = nonlin(np.dot(testX, syn0))
        close.append(l1[0]*sum)
        # print close
        return [close[-6:], syn0]


file = open('StockList.txt')
while 1:
    line = file.readline()
    if not line:
        break
    print line[:8]
    establish(line[:8])
# establish('sh601268')
# establish('sh600000')