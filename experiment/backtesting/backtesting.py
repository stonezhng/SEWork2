import MySQLdb


def create_backtesting(userid, sid, start, end, pool_index):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    cursor.execute('insert into `backtesing`(`userid`, `sid`, `start`, `end`, `pool_index`) '
                   'VALUES (%s, %s, %s, %s, %s)' % (userid, sid, start, end, pool_index))
    db.commit()
    db.close()
