import MySQLdb

import csv

db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                     port=8161, charset="utf8")
# db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
#                      port=3306, charset="utf8")
cursor = db.cursor()

cursor.execute('select `date`, `open`, `close`, `high`, `low`, `volume` from `stock_2010` where `stockid` = "sh600000" order by `date`')
raw = list(cursor.fetchall())

cursor.execute('select `date`, `open`, `close`, `high`, `low`, `volume` from `stock_2011` where `stockid` = "sh600000" order by `date`')
raw.extend(list(cursor.fetchall()))

cursor.execute('select `date`, `open`, `close`, `high`, `low`, `volume` from `stock_2012` where `stockid` = "sh600000" order by `date`')
raw.extend(list(cursor.fetchall()))

cursor.execute('select `date`, `open`, `close`, `high`, `low`, `volume` from `stock_2013` where `stockid` = "sh600000" order by `date`')
raw.extend(list(cursor.fetchall()))

cursor.execute('select `date`, `open`, `close`, `high`, `low`, `volume` from `stock_2014` where `stockid` = "sh600000" order by `date`')
raw.extend(list(cursor.fetchall()))

cursor.execute('select `date`, `open`, `close`, `high`, `low`, `volume` from `stock_2015` where `stockid` = "sh600000" order by `date`')
raw.extend(list(cursor.fetchall()))

cursor.execute('select `date`, `open`, `close`, `high`, `low`, `volume` from `stock_2016` where `stockid` = "sh600000" order by `date`')
raw.extend(list(cursor.fetchall()))

col_types = [str, float, float, float, float, float]
csvfile = file('600000.csv', 'wb')
writer = csv.writer(csvfile)
writer.writerow(['Date', 'Open', 'Close', 'High', 'Low', 'Volume'])
writer.writerows(raw)
#
# csvfile = file('600000.csv', 'rb')
# reader = csv.reader(csvfile)
# for each in reader:
#     print each
#     print type(each[2])
