import MySQLdb
from sklearn import linear_model
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

def calc_beta(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    stock_data = []
    bench_data = []

    if id[0:2] == 'sh':
        benchid = 'sh000001'
    elif id[0:2] == 'sz':
        benchid = 'sz399001'

    for x in range(2015, 2016):
        for y in range(1, 13):
            if y < 10:
                month = '0'+str(y)
            else:
                month = str(y)
            select_cmd = 'select MIN(`date`) from `stock_'+str(x)+'` where `stockid` = "'+id+'" and `date` >= ' \
                        '"'+str(x)+'-'+month+'-01" and `date` <= "'+str(x)+'-'+month+'-31" and `amount` > 0'
            # print select_cmd
            cursor.execute(select_cmd)
            date = list(cursor.fetchall())[0][0]
            # print date
            if date is None:
                break
            select_cmd = 'select `close` from `stock_'+str(x)+'` where `stockid` = "'+id+'" and `date` = "'+date+'"'
            cursor.execute(select_cmd)
            temp = list(cursor.fetchall())
            for each in temp:
                # if each[0] < 0.1:
                stock_data.append(each[0])
            if len(temp) == 0:
                break

            select_cmd = 'select MIN(`date`) from `bench` where `stockid` = "'+benchid+'" and `date` >= ' \
                        '"' + str(x) + '-' + month + '-01" and `date` <= "' + str(x) + '-' + month + '-31"'
            cursor.execute(select_cmd)
            date = list(cursor.fetchall())[0][0]
            # print date
            select_cmd = 'select `close` from `bench` where `stockid` = "' + benchid + '" and `date` = "' + date + '"'
            cursor.execute(select_cmd)
            temp = list(cursor.fetchall())
            for each in temp:
                # if each[0] < 0.1:
                bench_data.append(each[0])
    # print stock_data
    # print bench_data

    stock_per = []
    bench_per = []
    for x in range(1, len(stock_data)):
        temp = float(stock_data[x] - stock_data[x-1])/float(stock_data[x-1]) - 0.02
        stock_per.append(temp)
    for x in range(1, len(bench_data)):
        temp = float(bench_data[x] - bench_data[x-1])/float(bench_data[x-1]) - 0.02
        bench_per.append(temp)
    # print len(stock_per)
    # print len(bench_per)
    X = np.array([stock_per]).T
    Y = np.array(bench_per)
    # print X
    # print Y
    if len(stock_per) <= 1:
        coef = 0
    else:
        regr = linear_model.LinearRegression()
        regr.fit(X, Y)
        print regr.coef_[0]
        coef = regr.coef_[0]
    update_cmd = 'update `relative` set `beta` = %s where `stockid` = "%s"' % (coef, id)
    cursor.execute(update_cmd)

    # x = np.linspace(-1, 1, 100)
    # plt.scatter(np.array(stock_per), Y)
    # plt.plot(x, regr.coef_[0] * x + regr.intercept_)
    # plt.ylim(-1.5, 1.5)
    # plt.xlim(-1.5, 1.5)
    # plt.show()

file = open('StockList.txt')
while 1:
    line = file.readline()
    if not line:
        break
    print line[:8]
    calc_beta(line[:8])
# calc_beta('sz002644')