# -*- coding: UTF-8 -*-
import json
import urllib
import urllib2

import datetime
import time

import re
from lxml import etree

import tushare as ts
import MySQLdb


def get_hist_ts(id, date):
    com1 = 0
    com2 = 0
    df = ts.get_tick_data(id[2:], date=date, retry_count=5)
    # print df
    if len(df.values.tolist()) == 0 or len(df.values.tolist()) == 1:
        return 0
    elif len(df.values.tolist()[1]) == 0:
        return 0
    elif str(df.values.tolist()[1][1]) != 'nan':
        # print (df.values.tolist())
        temp = df.values.tolist()
        for each in temp:
            #     print each[5]
            if each[5] == '买盘':
                com1 += 1
            elif each[5] == '卖盘':
                com2 += 1
        if com1 + com2 != 0:
            value = float(com1 - com2) / float(com1 + com2)
            return value
        else:
            return 0
            # value = 0
    else:
        return 0


def get_hist_committee(id, date):
    buyin = 0
    saleout = 0

    page = 1
    while 1:
        url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?' \
              'symbol='+id+'&date='+date+'&page='+str(page)
        response = urllib.urlopen(url).read()
        tree = etree.HTML(response)
        # attr = tree.xpath('//*[@id="datatbl"]/tbody/tr[8]/th[2]/h5')
        # //*[@id="datatbl"]/tbody/tr[63]/th[2]/h1
        # print url
        attr1 = tree.xpath('//*[@id="datatbl"]/tbody/tr[1]/th[2]/h5')
        attr2 = tree.xpath('//*[@id="datatbl"]/tbody/tr[1]/th[2]/h6')
        attr3 = tree.xpath('//*[@id="datatbl"]/tbody/tr[1]/th[2]/h1')

        if len(attr1) == 0 and len(attr2) == 0 and len(attr3) == 0:
            break

        num = 1
        while 1:
            attr1 = tree.xpath('//*[@id="datatbl"]/tbody/tr[' + str(num) + ']/th[2]/h5')
            attr2 = tree.xpath('//*[@id="datatbl"]/tbody/tr[' + str(num) + ']/th[2]/h6')
            attr3 = tree.xpath('//*[@id="datatbl"]/tbody/tr[' + str(num) + ']/th[2]/h1')
            if len(attr1) == 0 and len(attr2) == 0 and len(attr3) == 0:
                break
            else:
                if len(attr1) != 0:
                    buyin += 1
                elif len(attr2) != 0:
                    saleout += 1
            num += 1
            # print 'page: ' + str(page) + ' num: ' + str(num)

        page += 1
    if saleout + buyin == 0:
        return 0
    else:
        return float(saleout - buyin) / float(saleout + buyin)


def refresh_committee(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
    #                      port=3306, charset="utf8")
    cursor = db.cursor()
    # print id
    # shReg = "(600[0-9]{3}|601[0-9]{3}|900[0-9]{3}|603[0-9]{3})"
    # szReg = "(000[0-9]{3}|002[0-9]{3}|300[0-9]{3}|200[0-9]{3}|001[0-9]{3})"
    # stockid = ''
    # if re.match(shReg, id):
    #     stockid = 'sh' + id
    #     # print id
    # elif re.match(szReg, id):
    #     stockid = 'sz' + id

    # cursor.execute('delete from `short_ticks` where `stockid` = "'+id+'"')
    end = datetime.datetime.now()
    # end = datetime.datetime.strptime('2016-06-17', '%Y-%m-%d')
    end = end + datetime.timedelta(days=-1)
    result = []
    valid = 0
    invalid = 0
    while valid != 5:
        # print end.strftime("%Y-%m-%d")
        committee = get_hist_ts(id, end.strftime("%Y-%m-%d"))
        if committee == 0:
            invalid += 1
        else:
            result.append([id, end.strftime("%Y-%m-%d"), committee])
            valid += 1
            # df = ts.get_tick_data(id[2:], date=end.strftime("%Y-%m-%d"), retry_count=5)
            # # print df
            # if len(df.values.tolist()) == 0 or len(df.values.tolist()) == 1:
            #     invalid += 1
            #     continue
            # elif len(df.values.tolist()[1]) == 0:
            #     invalid += 1
            #     continue
            # elif str(df.values.tolist()[1][1]) != 'nan':
            #     # print (df.values.tolist())
            #     temp = df.values.tolist()
            #     for each in temp:
            #         #     print each[5]
            #         if each[5] == '买盘':
            #             com1 += 1
            #         elif each[5] == '卖盘':
            #             com2 += 1
            #     if com1 + com2 != 0:
            #         value = float(com1 - com2) / float(com1 + com2)
            #         valid += 1
            #         result.append([id, end.strftime("%Y-%m-%d"), value])
            #     else:
            #         invalid += 1
            #         # value = 0
            # else:
            #     invalid += 1

        if invalid == 5:
            result.append([id, end.strftime("%Y-%m-%d"), 0])
            invalid = 0
            valid += 1
            continue
        print end.strftime('%Y-%m-%d') + ' invalid: ' + str(invalid) + ' valid: ' + str(valid)
        end = end + datetime.timedelta(days=-1)

    # print result
    if len(result) > 5:
        result = result[0:5]

    insert_cmd = 'insert into `short_ticks` (`stockid`, `date`, `committee`) VALUES (%s, %s, %s)'
    # print result
    cursor.executemany(insert_cmd, result)
    db.commit()
    db.close()


def refresh_quantity(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
    #                      port=3306, charset="utf8")
    cursor = db.cursor()

    # enddate = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    # end = datetime.datetime.strptime(enddate, "%Y-%m-%d")
    # start = end + datetime.timedelta(days=-15)
    # startdate = start.strftime("%Y-%m-%d")
    cursor.execute('select `date` from `short_ticks` where `stockid` = "'+id+'" order by date')
    datelist= list(cursor.fetchall())
    for x in range(0, len(datelist)):
        datelist[x] = datelist[x][0]
    # print datelist
    startdate = datelist[0]
    enddate = datelist[-1]
    start = datetime.datetime.strptime(startdate, '%Y-%m-%d')
    start = start + datetime.timedelta(-14)
    startdate = start.strftime('%Y-%m-%d')
    # print startdate
    # print enddate

    select_cmd = 'select `stockid`, `date`, `volume` from `stock_2016` where `date` >= "' + startdate + '" and `date` <="' + enddate + '" and `stockid` = "' + id + '" and `amount` > 0'
    cursor.execute(select_cmd)
    volume = []
    date = []
    validdate = []
    data = list(cursor.fetchall())
    for each in data:
        volume.append(each[2])
        date.append(each[1])
    volumerate = []
    # print volume
    # print date
    for x in range(4, len(volume)):
        base = float(volume[x - 4] + volume[x - 3] + volume[x - 2] + volume[x - 1] + volume[x])
        if base == 0:
            temp = 0
        else:
            temp = float(5 * volume[x]) / base
        if date[x] in datelist:
            volumerate.append(temp)
            validdate.append(date[x])
    # volumerate = volumerate[-5:]
    # date = date[-5:]
    # print volumerate
    for x in range(0, len(volumerate)):
        update_cmd = 'update `short_ticks` set `quantity_ratio` = ' + str(volumerate[x]) + ' where `stockid` ="' + id + '" ' \
                'and `date` = "' + \
                 validdate[x] + '"'
        print update_cmd
        cursor.execute(update_cmd)

#
file = open('full_list.txt')
while 1:
    line = file.readline()
    if not line:
        break
    print line[:8]
    try:
        refresh_committee(line[:8])
        refresh_quantity(line[:8])
    except:
        print
# refresh_committee('sh600000')
# refresh_quantity('sh600000')
