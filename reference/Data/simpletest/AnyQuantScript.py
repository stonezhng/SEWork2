# -*- coding: utf-8 -*-
import urllib2
import json
import time

import datetime
from sqlalchemy import create_engine
from pandas import DataFrame
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import MySQLdb


def init():
    idlist = []
    file = open('StockList.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        idlist.append(line[:8])
        # print line[:8]
    # print idlist
    # for x in range(0, len(idlist)):
    #     establish(idlist[x])
    pool = ThreadPool(8)
    pool.map(update, idlist)
    pool.close()
    pool.join()


def establish(id):
    print id
    engine = create_engine('mysql://client:123456@115.159.46.93:3306/stockTest?charset=utf8')
    try:
        engine.execute("DROP TABLE IF EXISTS " + id )
    except Exception, e:
        print 'refresh'
    url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=2010-01-01&end=2016-05-01"
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']

    data = []
    for x in xrange(0, len(json_file)):
        # temp = {'id': '', 'date': '', 'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0, 'adj_price': 0,
        #         'turnover': 0, 'pe_ttm': 0, 'pb': 0}
        temp = {}
        temp['id'] = id
        temp['date'] = json_file[x]['date']
        temp['open'] = json_file[x]['open']
        temp['high'] = json_file[x]['high']
        temp['low'] = json_file[x]['low']
        temp['close'] = json_file[x]['close']
        temp['volume'] = json_file[x]['volume']
        temp['adj_price'] = json_file[x]['adj_price']
        temp['turnover'] = json_file[x]['turnover']
        temp['pe_ttm'] = json_file[x]['pe_ttm']
        temp['pb'] = json_file[x]['pb']
        data.append(temp)


    create_cmd = "CREATE TABLE " + id + """ (
            `date` varchar(40) NOT NULL,
            `open` float,
            `high` float,
            `low` float,
            `close` float,
            `volume` int,
            `adj_price` float,
            `turnover` float,
            `pe_ttm` float,
            `pb` float,
            PRIMARY KEY(`date`)
            )ENGINE=MyISAM DEFAULT CHARSET=utf8;
            """
    engine.execute(create_cmd)
    pool = ThreadPool(5)
    pool.map(insert, data)
    pool.close()
    pool.join()
    # for x in xrange(0, len(json_file)):
    #     # insert_cmd = 'INSERT INTO ' + id + \
    #     #             '(date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb)' \
    #     #             'VALUES (%s, %f, %f, %f, %f, %d, %f, %f, %f, %f)' % \
    #     #             (str(json_file[x]['date']), json_file[x]['open'],
    #     #              json_file[x]['high'], json_file[x]['low'], json_file[x]['close'],
    #     #              json_file[x]['volume'], json_file[x]['adj_price'],
    #     #              json_file[x]['turnover'],
    #     #              json_file[x]['pe_ttm'], json_file[x]['pb'])
    #     insert_cmd = 'INSERT INTO `' + id + '`(date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb)' \
    #                                         'VALUES ("' + \
    #                  str(json_file[x]['date']) + '", ' + str(json_file[x]['open']) + ', ' + str(
    #         json_file[x]['high']) + ', ' + \
    #                  str(json_file[x]['low']) + ', ' + str(json_file[x]['close']) + ',' + str(
    #         json_file[x]['volume']) + ', ' + \
    #                  str(json_file[x]['adj_price']) + ', ' + \
    #                  str(json_file[x]['turnover']) + ', ' + str(json_file[x]['pe_ttm']) + ', ' + \
    #                  str(json_file[x]['pb']) + ')'
    #     engine.execute(insert_cmd)


def insert(temp):
    engine = create_engine('mysql://client:123456@115.159.46.93:3306/stockTest?charset=utf8')
    insert_cmd = 'INSERT INTO `' + temp['id'] + '`(date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb)' \
                                        'VALUES ("' + \
                 str(temp['date']) + '", ' + str(temp['open']) + ', ' + str(
        temp['high']) + ', ' + \
                 str(temp['low']) + ', ' + str(temp['close']) + ',' + str(
        temp['volume']) + ', ' + \
                 str(temp['adj_price']) + ', ' + \
                 str(temp['turnover']) + ', ' + str(temp['pe_ttm']) + ', ' + \
                 str(temp['pb']) + ')'
    engine.execute(insert_cmd)


def update(id):
    try:
        url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=" + open('LatestLog.txt').readline() + \
              "&end=" + time.strftime('%Y-%m-%d', time.localtime(time.time()))
        print url
        req = urllib2.Request(url)
        req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
        res_data = urllib2.urlopen(req)
        res = res_data.read()
        json_file = json.loads(res)['data']['trading_info']

        engine = create_engine('mysql://client:123456@115.159.46.93:3306/StocksAnalysis?charset=utf8')
        index = int(engine.execute('SELECT COUNT(*) FROM `' + id + '`').fetchall()[0][0])
        for x in xrange(0, len(json_file)):
            # insert_cmd = 'INSERT INTO `' + id + \
            #              '` (index, date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb)' \
            #              'VALUES (' + str(index) + ", '" + \
            #              str(json_file[x]['date']) + "', " + str(json_file[x]['open']) + ', ' + str(json_file[x]['high']) + ', ' + \
            #              str(json_file[x]['low']) + ', ' + str(json_file[x]['close']) + ',' + str(json_file[x]['volume']) + ', ' + \
            #              str(json_file[x]['adj_price']) + ', ' + \
            #              str(json_file[x]['turnover']) + ', ' + str(json_file[x]['pe_ttm']) + ', ' + \
            #              str(json_file[x]['pb']) + ')'
            insert_cmd = 'INSERT INTO `' + id + \
                                '` (index, date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb)' \
                            'VALUES (%d, %s, %f, %f, %f, %f, %d, %f, %f, %f, %f)' % \
                            (index, str(json_file[x]['date']), json_file[x]['open'],
                             json_file[x]['high'], json_file[x]['low'], json_file[x]['close'],
                             json_file[x]['volume'], json_file[x]['adj_price'],
                             json_file[x]['turnover'],
                             json_file[x]['pe_ttm'], json_file[x]['pb'])
            engine.execute(insert_cmd)
            index += 1
    except Exception, e:
        print e


def init_anyquant_database():
    idlist = []
    file = open('StockList.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        idlist.append(line[:8])
        # print line[:8]
    # print idlist
    # for x in range(0, len(idlist)):
    #     establish(idlist[x])
    pool = ThreadPool(2)
    pool.map(create_anyquant_table, idlist)
    pool.close()
    pool.join()


def create_anyquant_table(id):
    print id
    db = MySQLdb.connect("115.159.46.93", "client", "123456", "stockTest")
    cursor = db.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS `"+id+'`')
    except:
        1
    create_cmd = "CREATE TABLE `" + id + """`(
        `date` varchar(40) NOT NULL,
        `open` float,
        `high` float,
        `low` float,
        `close` float,
        `volume` bigint,
        `adj_price` float,
        `turnover` float,
        `pe_ttm` float,
        `pb` float,
        PRIMARY KEY(`date`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    cursor.execute(create_cmd)

    url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=2010-01-01&end=2016-05-01"
    req = urllib2.Request(url)
    req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    json_file = json.loads(res)['data']['trading_info']
    data = []
    for x in xrange(0, len(json_file)):
        temp = (str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
                float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
                float(json_file[x]['adj_price']), float(json_file[x]['turnover']),
                float(json_file[x]['pe_ttm']), float(json_file[x]['pb']))
        data.append(temp)
    insert_cmd = 'INSERT INTO `' + id +'` (date, open, high, low, close, volume, adj_price, turnover, pe_ttm, pb) ' \
                                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    try:
        cursor.executemany(insert_cmd, data)
    except Exception, e:
        print e
    finally:
        db.commit()
        db.close()




# init()
# file = open('LatestLog.txt', 'w')
# file.write(time.strftime('%Y-%m-%d', time.localtime(time.time())))
init_anyquant_database()


