import MySQLdb
import datetime
import numpy as np
from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.tools.shortcuts import buildNetwork
from pybrain.structure.modules import TanhLayer
import matplotlib.pyplot as plt


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

    select_cmd = 'select `close` from `stock_2016` where `stockid` = "'+id+'" and `date` < "'+enddate+'" and `amount`>0 order by `date`'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        if each[0] != 0:
            close.append(each[0])
    # print close
    return close


def get_draw_data(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    end = datetime.datetime.now()
    start = end + datetime.timedelta(days=-200)
    enddate = end.strftime('%Y-%m-%d')
    startdate = start.strftime('%Y-%m-%d')
    # startdate = '2015-05-01'
    # enddate = '2016-05-20'

    select_cmd = 'select `close` from `stock_2014` where `stockid` = "' + id + '" and `amount`>0 order by `date`'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        if each[0] != 0:
            close.append(each[0])
    # select_cmd = 'select `close` from `stock_2015` where `stockid` = "' + id + '" and `amount`>0 and `date` < "'+date+'"order by `date`'
    select_cmd = 'select `close` from `stock_2015` where `stockid` = "' + id + '" and `amount`>0 order by `date`'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        if each[0] != 0:
            close.append(each[0])
# print close
    return close


def bp_establish(id):
    # ds.addSample((0,0), (0,))
    ds = SupervisedDataSet(5, 1)
    data = get_data(id)
    for x in range(0, len(data)-6):
        X = data[x:x+5]
        Y = data[x+5]
        ds.addSample(tuple(X), (Y, ))
    net = buildNetwork(5, 10, 1, bias=True, hiddenclass=TanhLayer)
    trainer = BackpropTrainer(net, ds)
    # trainer.trainUntilConvergence()
    trainer.trainEpochs(500)
    print net.activate(tuple(data[-5:]))


def draw(id):
    ds = SupervisedDataSet(5, 1)
    data = get_draw_data(id)
    actual = data[-300:]
    predict = []
    base = []
    for i in range(-301, -1):
        base.append(i+302)
        temp = data[:len(data)-i]
        for x in range(0, len(temp) - 6):
            X = temp[x:x + 5]
            Y = temp[x + 5]
            ds.addSample(tuple(X), (Y,))
        net = buildNetwork(5, 10, 1, bias=True, hiddenclass=TanhLayer)
        trainer = BackpropTrainer(net, ds)
        # trainer.trainUntilConvergence()
        trainer.trainEpochs(500)
        predict.append(net.activate(tuple(temp[-5:]))[0])
    plt.plot(base, actual, 'g')
    plt.plot(base, predict, 'r')
    plt.show()


draw('sh600000')
# bp_establish('sh600000')