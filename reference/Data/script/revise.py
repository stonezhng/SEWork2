import MySQLdb


def combine(year):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "StocksAnalysis",
                         port=8161)
    cursor = db.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS `s_" + year + '`')
    except:
        1

    create_cmd = "CREATE TABLE `s_" + year + "`(" + """
            `stockid` varchar(40) NOT NULL,
            `date` varchar(40) NOT NULL,
            `open` float,
            `high` float,
            `low` float,
            `close` float,
            `volume` bigint,
            `amount` bigint,
            `adj_price` float,
            `turnover` float,
            `pe_ttm` float,
            `pb` float,
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

    join_cmd = 'select `stock_'+year+'`.`stockid`, `stock_'+year+'`.`date`, `stock_'+year+'`.`open`, `stock_'+year+'`.`high`, `stock_'+year+'`.`low`, ' \
                '`stock_'+year+'`.`close`, `stock_'+year+'`.`volume`, `stock_'+year+'`.`adj_price`, `stock_'+year+'`.`turnover`, `stock_'+year+'`.`pe_ttm`, ' \
                '`stock_'+year+'`.`pb`, `stock_'+year+'`.`dif`, `stock_'+year+'`.`dea`, `stock_'+year+'`.`macd`, ' \
                '`stock_'+year+'`.`slowK`, `stock_'+year+'`.`slowD`, `stock_'+year+'`.`slowJ`, ' \
                '`stock_'+year+'`.`boll_upper`, `stock_'+year+'`.`boll_middle`, `stock_'+year+'`.`boll_low`, ' \
                '`stock_'+year+'`.`ma5`, `stock_'+year+'`.`ma10`, `stock_'+year+'`.`ma20`, ' \
                '`stock_'+year+'`.`ma30`, `stock_'+year+'`.`ma60`, `stock_'+year+'`.`atr`, ' \
                '`stock_'+year+'`.`rsi`, `stock_'+year+'`.`deviation_val`, `stock_'+year+'`.`deviation_per`, `sina2016`.`amount`' \
               ' from `stock_'+year+'` LEFT JOIN `sina2016` ' \
                'ON `stock_' +year+'`.`stockid` = `sina2016`.`stockid` ' \
               'and `stock_'+year+'`.`date` = `sina2016`.`date`'

    cursor.execute(create_cmd)

    cursor.execute(join_cmd)

    data = list(cursor.fetchall())

    insert_cmd = 'INSERT INTO `s_'+year+'` ' \
                                        '(`stockid`, `date`, open, high, low, close, volume, ' \
                                            'adj_price, turnover, pe_ttm, pb,  ' \
                                            '`dif`, `dea`, `macd`, `slowK`, `slowD`, `slowJ`, `boll_upper`, `boll_middle`, ' \
                                        '`boll_low`, `ma5`, `ma10`, `ma20`, `ma30`, `ma60`, `atr`, `rsi` ,' \
                                            '`deviation_val`, `deviation_per`, `amount`) ' \
                                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    count = 1000
    data = tuple(data)
    while count < len(data):
        # print data[count - 1000]
        cursor.executemany(insert_cmd, data[count - 1000: count])
        db.commit()
        count += 1000
    cursor.executemany(insert_cmd, data[count:])
    db.commit()
    db.close()

combine('2015')
