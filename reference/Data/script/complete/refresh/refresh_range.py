import MySQLdb
import datetime
import numpy as np


def refresh_pfd():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    enddate = datetime.datetime.now()
    cursor.execute('select MAX(`date`) from `range`')
    # startdate = enddate + datetime.timedelta(-30)
    start = list(cursor.fetchall())[0][0]
    end = enddate.strftime("%Y-%m-%d")
    # start = startdate.strftime("%Y-%m-%d")
    cursor.execute('select DISTINCT `date` from `stock_2016` where `date`> "'+start+'" and `date` <= "'+end+'" '
                    'and `amount` > 0')
    raw = list(cursor.fetchall())
    data = []
    for each in raw:
        date = each[0]
        print date
        temp = ['stock', date]
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` <-0.08')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-0.08 and `deviation_per` <-0.06')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-0.06 and `deviation_per` <-0.04')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-0.04 and `deviation_per` <-0.02')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=-0.02 and `deviation_per` <0')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=0 and `deviation_per` <0.02')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=0.02 and `deviation_per` <0.04')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=0.04 and `deviation_per` <0.06')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=0.06 and `deviation_per` <0.08')
        temp.append(list(cursor.fetchall())[0][0])
        cursor.execute('select count(*) from `stock_2016` where `date` = "' + date + '" and `deviation_per` >=0.08')
        temp.append(list(cursor.fetchall())[0][0])
        data.append(temp)
    cursor.executemany('insert into `range` (`type`, `date`, `range0`, `range1`, `range2`, `range3`, `range4`, `range5`, '
                       ' `range6`, `range7`, `range8`, `range9`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                       tuple(data))
    db.commit()
    db.close()

refresh_pfd()