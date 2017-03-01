import MySQLdb


def pre_combine(year):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS `pre_" + year + '`')
    except:
        1
    create_cmd = "CREATE TABLE `pre_" + year + "`(" + """
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
            `amount` bigint,
            PRIMARY KEY(`date`, `stockid`),
            INDEX(`stockid`)
            )ENGINE=MyISAM DEFAULT CHARSET=utf8;
            """

    join_cmd = 'select `' + year + '`.`stockid`, `' + year + '`.`date`, `' + year + '`.`open`, `' + year + '`.`high`, `' + year + '`.`low`, ' \
            '`' + year + '`.`close`, `' + year + '`.`volume`, `' + year + '`.`adj_price`, `' + year + '`.`turnover`, `' + year + '`.`pe_ttm`, ' \
            '`' + year + '`.`pb`, `sina' + year + '`.amount'\
            ' from `'+year+'` left join `sina'+year+'` ' \
                'on`' +year+'`.`stockid` = `sina'+year+'`.`stockid` ' \
               'and `'+year+'`.`date` = `sina'+year+'`.`date`'

    cursor.execute(create_cmd)

    cursor.execute(join_cmd)

    data = list(cursor.fetchall())

    insert_cmd = 'INSERT INTO `pre_' + year + '` ' \
                                                '(`stockid`, `date`, open, high, low, close, volume, ' \
                                                'adj_price, turnover, pe_ttm, pb,  ' \
                                                '`amount`) ' \
                                                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    count = 1000
    data = tuple(data)
    while count < len(data):
    # print data[count - 1000]
        cursor.executemany(insert_cmd, data[count - 1000: count])
        db.commit()
        count += 1000
    cursor.executemany(insert_cmd, data[count-1000:])
    db.commit()
    db.close()


def combine(year):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS `stock_" + year + '`')
    except:
        1

    create_cmd = "CREATE TABLE `stock_" + year + "`(" + """
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
            `amount` bigint,
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
            PRIMARY KEY(`date`, `stockid`),
            INDEX(`stockid`)
            )ENGINE=MyISAM DEFAULT CHARSET=utf8;
            """

    join_cmd = 'select `pre_'+year+'`.`stockid`, `pre_'+year+'`.`date`, `pre_'+year+'`.`open`, `pre_'+year+'`.`high`, `pre_'+year+'`.`low`, ' \
                '`pre_'+year+'`.`close`, `pre_'+year+'`.`volume`, `pre_'+year+'`.`adj_price`, `pre_'+year+'`.`turnover`, `pre_'+year+'`.`pe_ttm`, ' \
                '`pre_'+year+'`.`pb`, `pre_'+year+'`.`amount`, `'+year+'_Analysis`.`dif`, `'+year+'_Analysis`.`dea`, `'+year+'_Analysis`.`macd`, ' \
                '`'+year+'_Analysis`.`slowK`, `'+year+'_Analysis`.`slowD`, `'+year+'_Analysis`.`slowJ`, ' \
                '`'+year+'_Analysis`.`boll_upper`, `'+year+'_Analysis`.`boll_middle`, `'+year+'_Analysis`.`boll_low`, ' \
                '`'+year+'_Analysis`.`ma5`, `'+year+'_Analysis`.`ma10`, `'+year+'_Analysis`.`ma20`, ' \
                '`'+year+'_Analysis`.`ma30`, `'+year+'_Analysis`.`ma60`, `'+year+'_Analysis`.`atr`, ' \
                '`'+year+'_Analysis`.`rsi`, `'+year+'_Analysis`.`deviation_val`, `'+year+'_Analysis`.`deviation_per`' \
               ' from `pre_'+year+'` LEFT JOIN `'+year+'_Analysis` ' \
                'ON `pre_' +year+'`.`stockid` = `'+year+'_Analysis`.`stockid` ' \
               'and `pre_'+year+'`.`date` = `'+year+'_Analysis`.`date`'

    cursor.execute(create_cmd)

    cursor.execute(join_cmd)

    data = list(cursor.fetchall())
    # print data[0]
    insert_cmd = 'INSERT INTO `stock_'+year+'` ' \
                                        '(`stockid`, `date`, `open`, `high`, `low`, `close`, `volume`, ' \
                                            '`adj_price`, `turnover`, `pe_ttm`, `pb`,  `amount`, ' \
                                            '`dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                        '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,' \
                                            '`deviation_val`, `deviation_per`) ' \
                                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    count = 1000
    data = tuple(data)
    while count < len(data):
        # print data[count - 1000]
        cursor.executemany(insert_cmd, data[count - 1000: count])
        db.commit()
        count += 1000
    cursor.executemany(insert_cmd, data[count-1000:])
    db.commit()
    db.close()


def benchmark(year):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis",
                         port=8161)
    cursor = db.cursor()
    join_cmd = 'select `' + year + '`.`stockid`, `' + year + '`.`date`, `' + year + '`.`open`, `' + year + '`.`high`, `' + year + '`.`low`, ' \
                '`' + year + '`.`close`, `' + year + '`.`volume`, `' + year + '`.`adj_price`, `' + year + '`.`turnover`, `' + year + '`.`pe_ttm`, ' \
                '`' + year + '`.`pb`, `' + year + '_Analysis`.`dif`, `' + year + '_Analysis`.`dea`, `' + year + '_Analysis`.`macd`, ' \
                '`' + year + '_Analysis`.`slowK`, `' + year + '_Analysis`.`slowD`, `' + year + '_Analysis`.`slowJ`, ' \
                '`' + year + '_Analysis`.`boll_upper`, `' + year + '_Analysis`.`boll_middle`, `' + year + '_Analysis`.`boll_low`, ' \
                '`' + year + '_Analysis`.`ma5`, `' + year + '_Analysis`.`ma10`, `' + year + '_Analysis`.`ma20`, ' \
                '`' + year + '_Analysis`.`ma30`, `' + year + '_Analysis`.`ma60`, `' + year + '_Analysis`.`atr`, ' \
                '`' + year + '_Analysis`.`rsi`, `' + year + '_Analysis`.`deviation_val`, `' + year + '_Analysis`.`deviation_per`' \
                ' from `' + year + '` LEFT JOIN `' + year + '_Analysis` ' \
                'ON `' + year + '`.`stockid` = `' + year + '_Analysis`.`stockid` ' \
                'and `' + year + '`.`date` = `' + year + '_Analysis`.`date` where `'+year+'`.`stockid` = "hs300"'
    cursor.execute(join_cmd)

    data = list(cursor.fetchall())
    insert_cmd = 'INSERT INTO `stock_' + year + '` ' \
                                                '(`stockid`, `date`, open, high, low, close, volume, ' \
                                                'adj_price, turnover, pe_ttm, pb,  ' \
                                                '`dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                                '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,' \
                                                '`deviation_val`, `deviation_per`) ' \
                                                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()

# print '2012'
# combine('2012')
# benchmark('2012')
# print '2013'
# combine('2013')
# benchmark('2013')
# print '2014'
# combine('2014')
# benchmark('2014')
# print '2015'
# combine('2015')
# benchmark('2015')
# print '2016'
# combine('2016')
# benchmark('2016')
# pre_combine('2015')
# db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
#                      port=8161)
# cursor = db.cursor()
# cursor.execute('select * from `sina2015`, `2015` where `sina2015`.`stockid` = `2015`.`stockid` and'
#                ' `sina2015`.`date` = `2015`.`date`')
# data = cursor.fetchall()
# for each in data:
#     if each[0] == 'sz002482':
#         print each
# # print data
# pre_combine('2016')
# pre_combine('2015')
# pre_combine('2013')
# pre_combine('2012')
# pre_combine('2014')
# pre_combine('2011')
# pre_combine('2010')
combine('2010')
combine('2011')
combine('2012')
combine('2013')
combine('2014')
combine('2015')
combine('2016')