import MySQLdb

db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                     port=8161)
cursor = db.cursor()
# cursor.execute('select max(`pool_index`) from `index_manager`')
cursor.execute('select * from `stock_2016` where `stockid` = "sh600145"')
raw = list(cursor.fetchall())
print raw