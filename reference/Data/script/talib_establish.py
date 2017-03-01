import MySQLdb


def create_year(year):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    create_cmd = "create table `" + \
                 year+"""_Analysis` (
        `stockid` varchar(40) NOT NULL,
        `date` varchar(40) NOT NULL,
        `dif` float,
        `dea` float,
        `macd` float,
        `slowK` float,
        `slowD` float,
        `slowJ` float,
        `boll_upper` float,
        `boll_middle` float,
        `boll_low` float,
        `ma5` float,
        `ma10` float,
        `ma20` float,
        `ma30` float,
        `ma60` float,
        `atr` float,
        `rsi` float,
        `deviation_val` float,
        `deviation_per` float,
        PRIMARY KEY(`date`, `stockid`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
    cursor.execute(create_cmd)

    data = []

    file = open('StockList.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        table_name = line[:8] + '_Analysis'
        select_cmd = 'select * from `' + table_name + '` where date >= "'+year+'-01-01" and date <= "'+year+'-12-31"'
        cursor.execute(select_cmd)
        temp = cursor.fetchall()
        data.extend(list(temp))


    insert_cmd = 'INSERT INTO `'+year+'_Analysis` ' \
                                        '(`stockid`, `date`, `dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                        '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,`deviation_val`, `deviation_per`) ' \
                                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
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
create_year('2010')
print '2011'
create_year('2011')
print '2012'
create_year('2012')
print '2013'
create_year('2013')
print '2014'
create_year('2014')
print '2015'
create_year('2015')
print '2016'
create_year('2016')

# data = []
# db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis",
#                      port=8161)
# cursor = db.cursor()
# table_name = 'hs300_Analysis'
# select_cmd = 'select * from `' + table_name + '` where date >= "' + '2016' + '-01-01" and date <= "' + '2016' + '-12-31"'
# cursor.execute(select_cmd)
# temp = cursor.fetchall()
# data = list(temp)
#
#
# insert_cmd = 'INSERT INTO `' + '2016' + '_Analysis` ' \
#                                       '(`stockid`, `date`, `dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
#                                       '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,`deviation_val`, `deviation_per`) ' \
#                                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
#
# count = 1000
# data = tuple(data)
# # while count < len(data):
# #     print data[count - 1000: count]
# #     cursor.executemany(insert_cmd, data[count - 1000: count])
# #     db.commit()
# #     count += 1000
# cursor.executemany(insert_cmd, data)
# db.commit()
# db.close()
