# -*- coding: UTF-8 -*-
import urllib2
import json
import tushare as ts
import MySQLdb
from pandas import DataFrame
import pandas as pd
import talib
import re


def get_descript(id):
    url = 'https://api.wmcloud.com:443/data/v1/' \
          'api/equity/getEqu.json?field=listStatusCD,listDate,delistDate,primeOperating,secFullName' \
          '&ticker='+id[2:]+'&secID=&equTypeCD=&listStatusCD='
    req = urllib2.Request(url)
    req.add_header("Authorization", 'Bearer 4c81816b40a4dbcc659f0017b81482281bf220618d616ac81c3287697fc0e755')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    raw_data = json.loads(res)
    if 'data' in raw_data:
        raw_data = json.loads(res)['data']
        temp = []
    # id = ''
    # if re.match(shReg, str(['ticker'])):
    #     id = 'sh' + str(raw_data[0]['ticker'])
    # elif re.match(szReg, str(raw_data[0]['ticker'])):
    #      id = 'sz' + str(raw_data[0]['ticker'])
    # temp.append(id)
        temp.append(raw_data[0]['secFullName'])
        temp.append(raw_data[0]['listDate'])
        if 'delistDate' in raw_data[0]:
            temp.append(raw_data[0]['delistDate'])
        else:
            temp.append(None)
        status = None
        if raw_data[0]['listStatusCD'] == 'L':
            status = '上市'
        elif raw_data[0]['listStatusCD'] == 'S':
            status = '暂停'
        elif raw_data[0]['listStatusCD'] == 'DE':
            status = '已退市'
        elif raw_data[0]['listStatusCD'] == 'UN':
            status = '未上市'
        temp.append(status)
        temp.append(raw_data[0]['primeOperating'])
        return temp
    else:
        return [None, None, None, None, None]


def descript_estab():
    file = open('StockList.txt')
    idlist = []
    while 1:
        line = file.readline()
        if not line:
            break
        idlist.append(line[:8])

    # print len(idlist)
    df = ts.get_industry_classified()

    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161,  charset="utf8")
    cursor = db.cursor()
    cursor.execute('drop table `company`')

    # create_cmd = """create table `company` (
    #     `stockid` varchar(40) NOT NULL ,
    #     `name` text,
    #     `industry` text,
    #     `fullName` text,
    #     `listDate` text,
    #     `delistDate` text,
    #     `status` text,
    #     `description` text,
    #     PRIMARY KEY(`stockid`)
    #     )ENGINE=MyISAM DEFAULT CHARSET=utf8;
    #     """
    create_cmd = """create table `company` (
        `stockid` varchar(40) NOT NULL ,
        `name` text,
        `industry` text,
        PRIMARY KEY(`stockid`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    cursor.execute(create_cmd)
    data = []
    # print df.values[0]

    # df.to_csv('company.csv')

    shReg = "(600[0-9]{3}|601[0-9]{3}|900[0-9]{3}|603[0-9]{3})"
    szReg = "(000[0-9]{3}|002[0-9]{3}|300[0-9]{3}|200[0-9]{3}|001[0-9]{3})"

    currentid = []

    for each in df.values:
        temp = []
        id = ''
        if re.match(shReg, str(each[0])):
            id = 'sh' + str(each[0])
            # print id
        elif re.match(szReg, str(each[0])):
            id = 'sz' + str(each[0])
            # print id
        # else:
            # print str(each[0]) + 'no match'
        # print len(idlist)
        # if id in idlist and id != 'sz000587':
        if id != 'sz000587':
            # print id
            currentid.append(id)
            temp.append(id)
            temp.append(each[1])
            temp.append(each[2])
            # temp.extend(get_descript(id))
            data.append(temp)
        # elif id == 'sz000587':
        #     temp = ['sz000587', '金洲慈航', '有色金属']
        #     # temp.extend(get_descript(id))
        #     data.append(temp)
    temp = ['sz000587', '金洲慈航', '有色金属']
            # temp.extend(get_descript(id))
    data.append(temp)
    currentid.append('sz000587')
    # for id in idlist:
    #     if id not in currentid:
    #         print id
        # elif id == 'sh600710':
        #     temp.append(id)
        #     temp.append(each[1])
        #     temp.append(each[2])
        #     temp.extend(['常林股份有限公司', '1996-06-24', None, '暂停', '工程、林业、矿山、环保、采运、环卫、起重、农业机械设备及零部件的研制、生产、销售、租赁、维修及出口,专用汽车及零部件的研制、生产、销售、售后服务、租赁及出口。本企业生产、科研所需的原辅材料、机械设备、仪器仪表及零件的进口(国家组织统一联合经销的16种出口商品和国家实行核定公司经营的14种出口商品除外)。承包境外机械行业工程和境内国际招标工程;上述境外工程所需的设备、材料出口;对外派遣实施上述境外工程所需的劳务人员。'])
        #     data.append(temp)
    # print data

    # insert_cmd = 'INSERT INTO `company` (`stockid`, `name`, `industry`, `fullName`, ' \
    #              '`listDate`, `delistDate`, `status`, `description`) ' \
    #              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
    insert_cmd = 'INSERT INTO `company` (`stockid`, `name`, `industry`) ' \
                 'VALUES (%s, %s, %s)'
    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()

descript_estab()
# print get_descript('sz000587')
# url = 'https://api.wmcloud.com:443/data/v1/' \
#       'api/equity/getEqu.json?field=listStatusCD,listDate,delistDate,primeOperating,secFullName' \
#       '&ticker=600710&secID=&equTypeCD=&listStatusCD='
# req = urllib2.Request(url)
# req.add_header("Authorization", 'Bearer 4c81816b40a4dbcc659f0017b81482281bf220618d616ac81c3287697fc0e755')
# res_data = urllib2.urlopen(req)
# res = res_data.read()
# print json.loads(res)