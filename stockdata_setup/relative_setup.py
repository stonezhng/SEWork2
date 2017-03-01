import MySQLdb


def create_relative():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    insert_cmd = 'insert into `relative` (`stockid`, `benchid`) VALUE ("%s", "%s")'

    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        id = line[:8]
        if id[0:2] == 'sh':
            benchid = '000001'
        elif id[0:2] == 'sz':
            benchid = '399001'

        cursor.execute(insert_cmd % (id, benchid))
        db.commit()
    db.close()

create_relative()