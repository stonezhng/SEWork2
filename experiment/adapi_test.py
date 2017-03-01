import MySQLdb
from MySQLdb.cursors import DictCursor
from twisted.enterprise import adbapi

class db_user:
    def init(self, id):
        self.dbpool = adbapi.ConnectionPool('MySQLdb',
                                   host='572b2568442c7.sh.cdb.myqcloud.com',
                                   user='cdb_outerroot',
                                   passwd='software2015',
                                   port=8161,
                                   db='dracarys',
                                   charset='utf8',
                                   use_unicode=True,
                                   cursorclass=DictCursor
                                   )
        return self.dbpool.runInteraction(self._id_select, id)

    def _id_select(self, txn, id):
        txn.execute('select * from `stock_2015` where `stockid` = "%s"', id)
        return txn.fetchall()

    def printResult(self, result):
        print result

    def close(self):
        self.dbpool.close()

    def run(self, id):
        self.init(id).addCallback(self.printResult)
        self.close()

t = db_user()
t.run('sh600000')
