import MySQLdb


def create_recommend():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        cursor.execute('insert into `recommend` (`stockid`) VALUES ("%s")' % line[:8])
    db.commit()
    db.close()

create_recommend()