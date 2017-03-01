import MySQLdb


def refresh_industry():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    for x in range(1, 62):
        select_cmd = 'select `weight`, `score`, `pb_index`, `pe_index`, `deviation_index`, `committee_index`, `volume_index`' \
                     'from `stock_evaluate` where `industryid` = ' + str(x)
        cursor.execute(select_cmd)
        data = list(cursor.fetchall())
        score = 0
        pb_index = 0
        pe_index = 0
        deviation_index = 0
        committee_index = 0
        volume_index = 0
        total = 0
        for each in data:
            total += each[0]
            score += each[1]*each[0]
            pb_index += each[2]*each[0]
            pe_index += each[3]*each[0]
            deviation_index += each[4]*each[0]
            committee_index += each[5]*each[0]
            volume_index += each[6]*each[0]
        score = float(score) / float(total)
        pb_index = float(pb_index) / float(total)
        pe_index = float(pe_index) / float(total)
        deviation_index = float(deviation_index) / float(total)
        committee_index = float(committee_index) / float(total)
        volume_index = float(volume_index) / float(total)

        updata_cmd = 'update `industry_tag` set `score` = '+str(score)+', `pb_index` = '+str(pb_index)+', `pe_index` = ' + str(pe_index) + \
                    ', `deviation_index` = '+str(deviation_index)+', `committee_index` = '+str(committee_index)+',' \
                    '`volume_index` = '+str(volume_index) + ' where `industryid` = '+str(x)
        cursor.execute(updata_cmd)

    i = 1
    select_cmd = 'select `score`, `industryid` from `industry_tag`  order by `score` desc'
    cursor.execute(select_cmd)
    data = list(cursor.fetchall())
    for each in data:
        updata_cmd = 'update `industry_tag` set `rank` = '+str(i) +' where `industryid` = '+ str(each[1])
        cursor.execute(updata_cmd)
        i += 1

refresh_industry()