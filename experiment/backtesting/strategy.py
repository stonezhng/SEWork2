import MySQLdb


def create_strategy(userid, py_text, json_text):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    cursor.execute('insert into `strategy` (`userid`, `py_text`, `json_text`) VALUE (%s, %s, %s)'
                   % (userid, py_text, json_text))
    db.commit()
    db.close()