import urllib
from lxml import etree
from stringold import strip

import MySQLdb
import datetime


def committee_collector(id):
    url = 'http://quotes.money.163.com/trade/fjb_%s.html#01b04' % id[2:]
    response = urllib.urlopen(url).read()
    tree = etree.HTML(response)
    table = tree.xpath('//*[@id="priceRateTable"]/table/tr')
    in_amount = 0
    out_amount = 0
    for each in table:
        total = each.xpath('td[2]/text()')[0]
        total = total.replace(',', '')
        total = float(total)
        in_per = float(strip(each.xpath('td[3]/text()')[0])[:-1]) * 0.01

        in_amount += total * in_per
        out_amount += total - total * in_per

    if in_amount + out_amount != 0:
        committee = (in_amount - out_amount) / (in_amount + out_amount)
    else:
        committee = None
    return committee


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


# def add_committee():
#     date = datetime.datetime.now()
#     dtr = date.strftime('%Y-%m-%d')
#
#     data = []
#
#     file = open('full_list.txt')
#     while 1:
#         line = file.readline()
#         if not line:
#             break
#         committee = committee_collector(line[:8])
#         print line[:8]
#         if committee is None:
#             continue
#         else:
#             data.append([line[:8], dtr, committee])
#     db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
#                                  port=8161, charset="utf8")
#     cursor = db.cursor()
#     insert_cmd = 'insert into `short_ticks` (`stockid`, `date`, `committee`) VALUES (%s, %s, %s)'
#     cursor.executemany(insert_cmd, tuple(data))
#     db.commit()
#
#     file = open('full_list.txt')
#     while 1:
#         line = file.readline()
#         if not line:
#             break
#         refresh_quantity('line[:8]')
#
#     db.close()
def add_committee():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    date = datetime.datetime.now()
    dtr = date.strftime('%Y-%m-%d')

    data = []

    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        committee = committee_collector(line[:8])
        print 'CQ: ' + line[:8]
        if committee is None:
            continue
        else:
            data.append([line[:8], dtr, committee])
            cursor.execute('insert into `short_ticks` (`stockid`, `date`, `committee`) VALUES ("%s", "%s", %s)'
                           % (line[:8], dtr, committee))
            db.commit()
            refresh_quantity(line[:8])
    # insert_cmd = 'insert into `short_ticks` (`stockid`, `date`, `committee`) VALUES (%s, %s, %s)'
    # cursor.executemany(insert_cmd, tuple(data))
    # db.commit()

    # file = open('full_list.txt')
    # file = open('/root/python_script/full_list.txt')
    # while 1:
    #     line = file.readline()
    #     if not line:
    #         break
    # db.close()

# add_committee()