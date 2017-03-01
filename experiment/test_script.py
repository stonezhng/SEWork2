import MySQLdb


def test():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    # db = MySQLdb.connect("10.66.173.110", "cdb_outerroot", "software2015", "test",
    #                      port=3306, charset="utf8")
    cursor = db.cursor()
    cursor.execute('insert into `exp`(`content`) VALUES ("testing")')
    db.commit()
    db.close()
    print 'test'

test()