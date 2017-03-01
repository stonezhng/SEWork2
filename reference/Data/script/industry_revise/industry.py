# -*- coding: UTF-8 -*-
import json
import urllib2
import math
import MySQLdb
import time

import datetime


def evaluate(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    start = end + datetime.timedelta(days=-14)
    startdate = start.strftime("%Y-%m-%d")

    select_cmd = 'select `stockid`, `volume`, `pb`, `pe_ttm`, `deviation_per` from `stock_2016` where `date` >= "'+startdate+'" and `date` <"'+enddate+'" and `stockid` = "'+id+'"'
    cursor.execute(select_cmd)
    data = list(cursor.fetchall())
    pb = []
    deviation_per = []
    pe = []
    volume = []
    committee = []

    for each in data:
        pb.append(each[2])
        pe.append(each[3])
        deviation_per.append(each[4])
        volume.append(each[1])

    # print volume

    volumerate = []
    for x in range(4, len(volume)):
        base = float(volume[x-4] + volume[x-3] + volume[x-2] + volume[x-1] + volume[x])
        if base == 0:
            temp = 0
        else:
            temp = float(5*volume[x])/base
        volumerate.append(temp)

    select_cmd = 'select `committee` from `short_ticks` where  `date` >= "'+startdate+'" and `date` < "'+enddate+'" and `stockid` = "'+id+'"'
    cursor.execute(select_cmd)
    for each in list(cursor.fetchall()):
        committee.append(each[0])
    # print committee[-5:]
    # print deviation_per[-5:]
    # print pb[-5:]
    # print pe[-5:]
    # print volumerate[-5:]
    pb_index = get_pb_index(pb[-5:])
    pe_index = get_pe_index(pe[-5:])
    volume_index = get_volume_index(volumerate[-5:], deviation_per[-5:])
    committee_index = get_committee_index(committee[-5:])
    deviation_index = get_deviation_index(deviation_per[-5:])
    # print deviation_index

    select_cmd = 'select `stockid`, `industryid`, `total` from `stock_info` where `stockid` = "'+id+'"'
    cursor.execute(select_cmd)
    info = list(cursor.fetchall())[0]
    # print info
    score = (pb_index + deviation_index + pe_index + committee_index + volume_index)/float(5)
    if pb_index * pe_index * volume_index * committee_index * deviation_index == 0:
        return [info[1], info[0], info[2], 0, 0, 0, 0, 0, 0]
    else:
        return [info[1], info[0], info[2], score, pb_index, deviation_index, pe_index, committee_index, volume_index]

    # print data


def calc():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    for x in range(1, 62):
        select_cmd = 'select `stockid`, `score` from `stock_evaluate` where `industryid` = ' + str(x) + ' order by `score` DESC'
        cursor.execute(select_cmd)
        re = list(cursor.fetchall())
        i = 1
        for each in re:
            update_cmd = 'update `stock_evaluate` set `rank` = ' + str(i) + ' where `stockid` = "' + each[0] + '"'
            cursor.execute(update_cmd)
            i += 1


def create():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    # cursor.execute('drop table `stock_evaluate`')
    # create_cmd = """create table `stock_evaluate` (
    #     `industryid` bigint,
    #     `stockid` varchar(40) NOT NULL,
    #     `weight` bigint,
    #     `score` float,
    #     `pb_index` float,
    #     `deviation_index` float,
    #     `pe_index` float,
    #     `committee_index` float,
    #     `volume_index` float,
    #     `rank` bigint not null default -1,
    #     PRIMARY KEY(`stockid`)
    #     )ENGINE=MyISAM DEFAULT CHARSET=utf8;
    #    """
    # cursor.execute(create_cmd)

    # data = []

    file = open('StockList.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        print line[:8]
        result = evaluate(line[:8])
        #
        if result:
            update_cmd = 'update `stock_evaluate` set `score` = %s, `pb_index` = %s, `deviation_index` = %s, ' \
                         '`pe_index` = %s, `committee_index` = %s, `volume_index` = %s where `stockid` = "%s"' % (result[3],
                                                                                           result[4], result[5],
                                                                                           result[6], result[7],
                                                                                           result[8], line[:8])
            cursor.execute(update_cmd)
            # data.append(result)
    # insert_cmd = 'insert into `stock_evaluate` (`industryid`, `stockid`, `weight`, `score`, `pb_index`, ' \
    #              '`deviation_index`, `pe_index`, `committee_index`, `volume_index`) ' \
    #              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    # cursor.executemany(insert_cmd, data)
    # db.commit()
    # db.close()


def get_pe_index(pe):
    total = 0

    if len(pe) != 0:
        # print pe
        for temp in pe:
            if temp is None:
               total += 0
            else:
                # print temp
                if temp < 0:
                    total += -1
                if sum > 27:
                    total += (1 / (math.pow(1.01, temp - 28))) * 49
                else:
                    total = (28 - temp) * 1.75 + 49
                # print total
        # print (total / len(pe))
        return (total / len(pe))
    else:
        return 0


def get_pb_index(pb):
    total = 0

    if len(pb) != 0:
        for temp in pb:
            if temp is None:
                total += 0
            else:
                # print temp
                if temp > 0:
                    total += (1 - 1 / (math.pow(1.7, temp))) * 100
                # print total
        return (total / len(pb))
    else:
        return 0


def get_volume_index(volume, deviation_per):
    total = 0
    if len(volume) != 0:
        i = 0
        for temp in volume:
            if temp is None:
                total += 0
            else:
                if temp <= 0.5:
                    total += temp * 80
                if temp <= 2.75 and temp > 0.5:
                    total += -(1 - 1 / (math.pow(2.1, temp - 0.5))) * 20 + 90
                if temp <= 5 and temp > 2.75:
                    total += -(1 - 1 / (math.pow(2.1, 5 - temp))) * 20 + 90
                if temp > 5:
                    if deviation_per[i] > 0:
                        total += (1 - 1 / (math.pow(1.1, temp))) * 45 + 50
                    else:
                        total += -(1 - 1 / (math.pow(1.1, temp))) * 40 + 60
            i += 1

        return (total / len(volume))
    else:
        return 0


def get_deviation_index(deviation_per):
    total = 0
    # print deviation_per
    if len(deviation_per) != 0:
        for temp in deviation_per:
            if temp is None:
                total += 0
            else:
                # print temp
                if temp < 0:
                    total += 50 - (1 - 1 / (math.pow(1.4, (-temp) * 100))) * 50
                else:
                    total += (1 - 1 / (math.pow(1.4, temp * 100))) * 50 + 50
        # print total/len(deviation_per)
        # print len(deviation_per)
        return (total / len(deviation_per))
    else:
        return 0


def get_committee_index(committee):
    total = 0

    if len(committee) != 0:
        for temp in committee:
            if temp is None:
                total += 0
            else:
                if temp > 0:
                    total += (1 - 1 / (math.pow(1.35, temp * 10))) * 40 + 60
                else:
                    total += - (1 - 1 / (math.pow(1.3, temp * 10))) * 40 + 60
        return (total / len(committee))
    else:
        return 0


def estab_industry_rank():
    select_cmd = 'select distinct `industry` from `stock_info`'
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    create_cmd = """create table `industry_tag` (
            `industryid` bigint NOT NULL auto_increment,
            `industry` text,
            `score` float default -1,
            `pb_index` float  default -1,
            `pe_index` float  default -1,
            `deviation_index` float  default -1,
            `committee_index` float default -1,
            `volume_index` float default -1,
            `rank` bigint default -1,
            PRIMARY KEY(`industryid`)
            )ENGINE=MyISAM DEFAULT CHARSET=utf8;
           """
    cursor.execute(create_cmd)
    cursor.execute(select_cmd)
    data = tuple(cursor.fetchall())
    insert_cmd = 'insert into `industry_tag` (`industry`) VALUES (%s)'
    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()


def refresh_industry():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    for x in range(1, 62):
        select_cmd = 'select `weight`, `score`, `pb_index`, `pe_index`, `deviation_index`, `committee_index`, `volume_index`' \
                     'from `stock_evaluate` where `industryid` = ' + str(x)
        cursor.execute(select_cmd)
        data = list(cursor.fetchall())
        score = 0
        pb_index = 0
        pe_index = 0
        deviation_index = 0
        committee_index = 0
        volume_index = 0
        total = 0
        for each in data:
            total += each[0]
            score += each[1]*each[0]
            pb_index += each[2]*each[0]
            pe_index += each[3]*each[0]
            deviation_index += each[4]*each[0]
            committee_index += each[5]*each[0]
            volume_index += each[6]*each[0]
        score = float(score) / float(total)
        pb_index = float(pb_index) / float(total)
        pe_index = float(pe_index) / float(total)
        deviation_index = float(deviation_index) / float(total)
        committee_index = float(committee_index) / float(total)
        volume_index = float(volume_index) / float(total)

        updata_cmd = 'update `industry_tag` set `score` = '+str(score)+', `pb_index` = '+str(pb_index)+', `pe_index` = ' + str(pe_index) + \
                    ', `deviation_index` = '+str(deviation_index)+', `committee_index` = '+str(committee_index)+',' \
                    '`volume_index` = '+str(volume_index) + ' where `industryid` = '+str(x)
        cursor.execute(updata_cmd)

    i = 1
    select_cmd = 'select `score`, `industryid` from `industry_tag`  order by `score` desc'
    cursor.execute(select_cmd)
    data = list(cursor.fetchall())
    for each in data:
        updata_cmd = 'update `industry_tag` set `rank` = '+str(i) +' where `industryid` = '+ str(each[1])
        cursor.execute(updata_cmd)
        i += 1
# create_evaluate('sh600000')
create()
calc()
# evaluate('sh600603')
# estab_industry_rank()
refresh_industry()