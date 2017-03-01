import json
import urllib2

import MySQLdb


def create_evaluate():
    data = []
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        data.append([line[:8]])
    print data
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                             port=8161)
    cursor = db.cursor()
    insert_cmd = 'insert into `stock_evaluate` (`stockid`) VALUES (%s)'
    cursor.executemany(insert_cmd, tuple(data))
    db.commit()
    db.close()


def industry():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        cursor.execute('select `industryid` from `sw_stock_info` where `stockid` = "%s"' % line[:8])
        indus = list(cursor.fetchall())[0][0]
        cursor.execute('update `stock_evaluate` set `industryid` = %s where `stockid` = "%s"' % (indus, line[:8]))
    db.close()


def count():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    cursor.execute('select count(*), `industryid` from `stock_evaluate` group by `industryid`')
    raw = list(cursor.fetchall())
    # print raw
    count = []
    inid = []

    for each in raw:
        count.append(each[0])
        inid.append(each[1])

    for x in range(0, len(inid)):
        print x
        cursor.execute('update `stock_evaluate` set `count` = %s where `industryid` = %s' % (count[x], inid[x]))
    db.close()


def weight():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        url = 'https://api.wmcloud.com:443/data/v1/' \
            'api/equity/getEqu.json?field=totalShares&ticker=' + line[2:8] + '&secID=&equTypeCD=&listStatusCD='
        req = urllib2.Request(url)
        req.add_header("Authorization", 'Bearer 4c81816b40a4dbcc659f0017b81482281bf220618d616ac81c3287697fc0e755')
        res_data = urllib2.urlopen(req)
        res = res_data.read()
        raw_data = json.loads(res)
        if 'data' in raw_data:
            raw_data = json.loads(res)['data']
            weight = raw_data[0]['totalShares']
            update_cmd = 'update `stock_evaluate` set `weight` = %s where `stockid` = "%s"' % (weight, line[:8])
            cursor.execute(update_cmd)
    db.close()


def industry_count():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    for x in range(1, 29):
        select_cmd = 'select `count` from `stock_evaluate` where `industryid` = ' + str(x)
        cursor.execute(select_cmd)
        count = list(cursor.fetchall())[0][0]
        update_cmd = 'update `sw_industry` set `count` = %s where `industryid` = %s' % (count, str(x))
        cursor.execute(update_cmd)

# count()
# create_evaluate()
# industry()
# weight()
industry_count()