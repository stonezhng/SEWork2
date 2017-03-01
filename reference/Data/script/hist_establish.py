import json
import urllib2

import MySQLdb


def hist_establish(year):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test", port=8161)
    cursor = db.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS `" + year + '`')
    except:
        1

    create_cmd = "CREATE TABLE `" + year + """`(
            `stockid` varchar(40) NOT NULL,
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
            PRIMARY KEY(`date`, `stockid`),
            INDEX(`stockid`)
            )ENGINE=MyISAM DEFAULT CHARSET=utf8;
            """
    data = []
    cursor.execute(create_cmd)
    file = open('StockList.txt')
    while 1:
        id = file.readline()
        if not id:
            break
        id = id[:8]
        url = "http://121.41.106.89:8010/api/stock/" + id + "/?start=" + year + "-01-01&end=" + year + "-12-31"
        req = urllib2.Request(url)
        req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
        res_data = urllib2.urlopen(req)
        res = res_data.read()
        json_file = json.loads(res)['data']['trading_info']
        for x in range(0, len(json_file)):
            temp = [id, str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
                        float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
                        float(json_file[x]['adj_price']), float(json_file[x]['turnover']),
                        float(json_file[x]['pe_ttm']), float(json_file[x]['pb'])]
            data.append(temp)

    insert_cmd = 'INSERT INTO `' + year + '` (stockid, date, open, high, low, close, volume, ' \
                                            'adj_price, turnover, pe_ttm, pb) ' \
                                            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    count = 1000
    data = tuple(data)
    while count < len(data):
        cursor.executemany(insert_cmd, data[count - 1000: count])
        db.commit()
        count += 1000
    cursor.executemany(insert_cmd, data[count-1000:])
    db.commit()
    db.close()

print '2010'
hist_establish('2010')
print '2011'
hist_establish('2011')
print '2012'
hist_establish('2012')
print '2013'
hist_establish('2013')
print '2014'
hist_establish('2014')
print '2015'
hist_establish('2015')
print '2016'
hist_establish('2016')
# data = []
# url = "http://121.41.106.89:8010/api/benchmark/hs300/?start=" + '2010' + "-01-01&end=" + '2010' + "-12-31"
# req = urllib2.Request(url)
# req.add_header('X-Auth-Code', '75d07493b655591137dbc905ede428ce')
# res_data = urllib2.urlopen(req)
# res = res_data.read()
# json_file = json.loads(res)['data']['trading_info']
# for x in range(0, len(json_file)):
#     temp = ['hs300', str(json_file[x]['date']), float(json_file[x]['open']), float(json_file[x]['high']),
#                 float(json_file[x]['low']), float(json_file[x]['close']), int(json_file[x]['volume']),
#                 float(json_file[x]['adj_price']), None, None, None]
#     data.append(temp)
#
# insert_cmd = 'INSERT INTO `' + '2010' + '` (stockid, date, open, high, low, close, volume, ' \
#                                       'adj_price, turnover, pe_ttm, pb) ' \
#                                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
#
# db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test", port=8161)
# cursor = db.cursor()
# cursor.executemany(insert_cmd, data)
# db.commit()
# db.close()