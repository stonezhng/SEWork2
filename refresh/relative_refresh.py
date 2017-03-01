from sklearn import linear_model

import MySQLdb
import datetime
import numpy as np
import time
from scipy import stats


def refresh(id):

    if id[0:2] == 'sh':
        benchid = '000001'
    elif id[0:2] == 'sz':
        benchid = '399001'

    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    start = end + datetime.timedelta(days=-8)
    startdate = start.strftime("%Y-%m-%d")
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    select_cmd = 'select `open`, `volume`, `deviation_per`, `amount` from `stock_2016` where `stockid` = "'+id+'" ' \
                    ' and `date` >= "'+startdate+'" and `date` < "'+enddate+'"'
    cursor.execute(select_cmd)
    data = list(cursor.fetchall())
    stock_open = []
    stock_volume = []
    stock_devia = []
    amount = []

    for each in data:
        stock_open.append(each[0])
        stock_volume.append(each[1])
        stock_devia.append(each[2])
        amount.append(each[3])

    stock_open = stock_open[-5:]
    stock_volume = stock_volume[-5:]
    stock_devia = stock_devia[-5:]
    amount = amount[-5:]

    select_cmd = 'select `open`, `volume`, `deviation_per` from `bench` where `stockid` = "%s" ' \
                    ' and `date` >= "%s" and `date` < "%s"' % (benchid, startdate, enddate)
    # print select_cmd
    cursor.execute(select_cmd)
    data = list(cursor.fetchall())
    bench_open = []
    bench_volume = []
    bench_devia = []

    for each in data:
        bench_open.append(each[0])
        bench_volume.append(each[1])
        bench_devia.append(each[2])

    bench_open = bench_open[-5:]
    bench_volume = bench_volume[-5:]
    bench_devia = bench_devia[-5:]

    sopen = []
    svolume = []
    sdevia = []
    bopen = []
    bvolume = []
    bdevia = []

    for x in range(0, len(amount)):
        if amount[x] != 0:
            sopen.append(stock_open[x])
            bopen.append(bench_open[x])
            svolume.append(stock_volume[x])
            bvolume.append(bench_volume[x])
            sdevia.append(stock_devia[x])
            bdevia.append(bench_devia[x])
    #
    # print sopen
    # print svolume
    # print sdevia
    # print bopen
    # print bvolume
    # print bdevia
    if len(sopen) > 1:
        open_info = stats.describe(sopen)[2:]
        open_re = np.corrcoef(sopen, bopen, rowvar=0)[0][1]
    else:
        open_info = ['null', 'null', 'null', 'null']
        open_re = 'null'

    if len(svolume) > 1:
        volume_info = stats.describe(svolume)[2:]
        volume_re = np.corrcoef(svolume, bvolume, rowvar=0)[0][1]
    else:
        volume_info = ['null', 'null', 'null', 'null']
        volume_re = 'null'

    if len(sdevia) > 1:
        devia_info = stats.describe(sdevia)[2:]
        devia_re = np.corrcoef(sdevia, bdevia, rowvar=0)[0][1]
    else:
        devia_info = ['null', 'null', 'null', 'null']
        devia_re = 'null'
    # if 0 not in amount:
    #     open_info = stats.describe(stock_open)[2:]
    #     open_re = np.corrcoef(stock_open, bench_open, rowvar=0)[0][1]
    # else:
    #     open_info = ['null', 'null', 'null', 'null']
    #     open_re = 'null'
    #
    # if 0 not in amount:
    #     volume_info = stats.describe(stock_volume)[2:]
    #     volume_re = np.corrcoef(stock_volume, bench_volume, rowvar=0)[0][1]
    # else:
    #     volume_info = ['null', 'null', 'null', 'null']
    #     volume_re = 'null'
    #
    # if 0 not in amount:
    #     devia_info = stats.describe(stock_devia)[2:]
    #     devia_re = np.corrcoef(stock_devia, bench_devia, rowvar=0)[0][1]
    # else:
    #     devia_info = ['null', 'null', 'null', 'null']
    #     devia_re = 'null'
    if str(open_re) == 'nan':
        open_re = 'null'
    if str(volume_re) == 'nan':
        volume_re = 'null'
    if str(devia_re) == 'nan':
        devia_re = 'null'

    if open_re == 'null' or volume_re == 'null' or devia_re == 'null':
        re = 'null'
    else:
        re = float(open_re+volume_re+devia_re)/float(3)

    val = (open_info[0], open_info[1], open_info[2], open_info[3], open_re,
           volume_info[0], volume_info[1], volume_info[2], volume_info[3], volume_re,
           devia_info[0], devia_info[1], devia_info[2], devia_info[3], devia_re, re, id, benchid)

    # insert_cmd = """
    # insert into `relative`(`stockid`, `benchid`,
    # `open_mean`, `open_var`, `open_skewness`, `open_kurtosis`, `open_corrcoef`,
    # `volume_mean`, `volume_var`, `volume_skewness`, `volume_kurtosis`, `volume_corrcoef`,
    # `devia_mean`, `devia_var`, `devia_skewness`, `devia_kurtosis`, `devia_corrcoef`,
    #  `corrcoef`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    # """ % val
    update_cmd = """
    update `relative` set `open_mean` = %s, `open_var` = %s, `open_skewness` = %s, `open_kurtosis` = %s, `open_corrcoef` = %s,
    `volume_mean` = %s, `volume_var` = %s, `volume_skewness` = %s, `volume_kurtosis` = %s, `volume_corrcoef` = %s,
    `devia_mean` = %s, `devia_var` = %s, `devia_skewness` = %s, `devia_kurtosis` = %s, `devia_corrcoef` = %s,
    `corrcoef` = %s where `stockid` = "%s" and `benchid` = "%s"
    """ % val
    # print update_cmd

    cursor.execute(update_cmd)
    # db.commit()
    db.close()


def refresh_beta(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    stock_data = []
    bench_data = []

    if id[0:2] == 'sh':
        benchid = '000001'
    elif id[0:2] == 'sz':
        benchid = '399001'

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


def refresh_relative():
    # file = open('/root/python_script/full_list.txt')
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        print 'relative: ' + line[:8]
        refresh(line[:8])

# refresh_relative()