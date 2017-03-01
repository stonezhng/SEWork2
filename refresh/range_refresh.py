import MySQLdb
import datetime
import numpy as np


def refresh_range():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    enddate = datetime.datetime.now()
    cursor.execute('select MAX(`date`) from `range`')
    #
    # start = list(cursor.fetchall())[0][0]
    # end = enddate.strftime("%Y-%m-%d")
    #
    # cursor.execute('select DISTINCT `date` from `stock_2016` where `date`> "'+start+'" and `date` <= "'+end+'" '
    #                 'and `amount` > 0')
    date = datetime.datetime.now()
    dtr = date.strftime('%Y-%m-%d')
    cursor.execute('select DISTINCT `date` from `stock_2016` where `date` = "'+dtr+'" and `amount` > 0')
    raw = list(cursor.fetchall())
    data = []
    for each in raw:
        date = each[0]
        print date
        temp = ['stock', date]
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` <-8')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-8 and `deviation_per` <-6')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-6 and `deviation_per` <-4')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-4 and `deviation_per` <-2')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-2 and `deviation_per` <0')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=0 and `deviation_per` <2')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=2 and `deviation_per`< 4')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >= 4 and `deviation_per`< 6')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >= 6 and `deviation_per` < 8')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=0.08')
        temp.append(list(cursor.fetchall())[0][0])
        data.append(temp)
    cursor.executemany('insert into `range` (`type`, `date`, `range0`, `range1`, `range2`, `range3`, `range4`, `range5`, '
                       ' `range6`, `range7`, `range8`, `range9`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                       tuple(data))
    db.commit()
    db.close()

# refresh_range()