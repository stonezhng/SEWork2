import MySQLdb


class strategy:
    def __init__(self, pool_name):
        self.db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
        self.cursor = self.db.cursor()
        self.pool_name = pool_name
        self.cursor.execute('select `stockid`, `income`, `volume`, `detail_index` from `%s`' % pool_name)
        self.relation = self.cursor.fetchall()
        # current = self.cursor.fetchall()
        # self.relation = {}
        # for each in current:
        #     self.relation[each[0]] = each[1:]
        print self.relation
        self.temp_volume = 0

    def reference(self, stock_data):
        if stock_data['rsi6'] > stock_data['rsi12']:
            # return 50
            self.order(50)
        else:
            self.sell(50)

    def order(self, volume):
        self.temp_volume = volume

    def sell(self, volume):
        self.temp_volume = -volume

    def clean(self):
        for each in self.relation:
            self.cursor.execute('update `%s` set `volume` = 0, `income` = 0 where `stockid` = "%s"'
                                % (self.pool_name, each[0]))
            self.cursor.execute('delete from `%s`' % each[3])
        self.db.commit()

    def backtesting(self, startdate, enddate):
        for each in self.relation:
            self.run(each[0], startdate, enddate)

    def run(self, stockid, startdate, enddate):
        start_year = int(startdate[0:4])
        end_year = int(enddate[0:4]) + 1

        stock_data = []

        select_cmd = 'select `date`, `open`, `high`, `low`, `close`, `deviation_val`, `deviation_per`,' \
                     '`volume`, `amount`, `amplitude`, `pe_ttm`, `pb`, `dif`, `dea`, `macd`, `slowK`, `slowD`, ' \
                     '`slowJ`,`boll_upper`, `boll_middle`, `boll_low`, `rsi6`, `rsi12`, `rsi24`,`ma5`, ' \
                     '`ma10`, `ma20`, `ma30`, `ma60` ' \
                     'from `stock_%s` where `stockid` = "%s" and `date` >= "%s" and `date` <= "%s" group by `date`'
        while start_year != end_year:
            self.cursor.execute(select_cmd % (start_year, stockid, startdate, enddate))
            raw = self.cursor.fetchall()

            for each in raw:
                temp = {}
                temp['date'] = each[0]
                temp['open'] = each[1]
                temp['high'] = each[2]
                temp['low'] = each[3]
                temp['close'] = each[4]
                temp['deviation_val'] = each[5]
                temp['deviation_per'] = each[6]
                temp['volume'] = each[7]
                temp['amount'] = each[8]
                temp['amplitude'] = each[9]
                temp['pe_ttm'] = each[10]
                temp['pb'] = each[11]
                temp['dif'] = each[12]
                temp['dea'] = each[13]
                temp['macd'] = each[14]
                temp['slowK'] = each[15]
                temp['slowD'] = each[16]
                temp['slowJ'] = each[17]
                temp['boll_upper'] = each[18]
                temp['boll_middle'] = each[19]
                temp['boll_low'] = each[20]
                temp['rsi6'] = each[21]
                temp['rsi12'] = each[22]
                temp['rsi24'] = each[23]
                temp['ma5'] = each[24]
                temp['ma10'] = each[25]
                temp['ma20'] = each[26]
                temp['ma30'] = each[27]
                temp['ma60'] = each[28]
                stock_data.append(temp)
            start_year += 1

        print stock_data

        detail_name = ''
        for each in self.relation:
            if each[0] == stockid:
                detail_name = each[3]

        insert_cmd = 'insert into `%s` (`income`, `volume`, `date`) VALUES (%s, %s, %s)'
        income = 0
        volume = 0
        for each in stock_data:
            self.reference(each)
            result = self.temp_volume
            self.temp_volume = 0
            temp_income = - (result * each['close'])
            self.cursor.execute(insert_cmd % (detail_name, temp_income, result, each['date']))
            self.db.commit()
            income += temp_income
            volume += result

        self.cursor.execute('update `%s` set income = %s, volume = %s where `stockid` = "%s"'
                            % (self.pool_name, income, volume, stockid))
        self.db.commit()

    def close(self):
        self.db.close()

s = strategy('stockpool_0')
# s.clean()
s.backtesting('2016-01-01', '2016-08-01')
s.close()



