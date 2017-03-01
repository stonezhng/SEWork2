# coding=utf-8
import MySQLdb


def pepb_refresh(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    select_cmd = 'select `close`, `date` from `stock_2016` where `stockid` = "%s" and `pb` = 0' % id
    cursor.execute(select_cmd)

    date = []
    close = []

    raw = list(cursor.fetchall())
    if not raw:
        print 'empty'
        return
    for each in raw:
        date.append(each[1])
        close.append(each[0])
    # print 'pe'
    # print date

    select_cmd = 'select `date`, `每股收益_调整后`, `每股净资产_调整后` from `season` where (`date` = "%s"  or `date` = "%s"' \
                 'or `date`  = "%s") and `stockid` = "%s" group by `date`' \
                 % ('2015-06-30', '2015-12-31', '2016-06-30', id)

    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    # ref_peval = []
    # ref_pbval = []
    ref = {}
    for each in raw:
        if each[0] == '2015-12-31':
            ref['2015-12-31'] = [each[1], each[2]]
            # ref_peval.append(each[1])
            # ref_pbval.append(each[2])
        else:
            ref[each[0]] = [2 * float(each[1]), 2 * float(each[2])]
            # ref_peval.append(2 * float(each[1]))
            # ref_pbval.append(2 * float(each[2]))

    index1 = '2016-05-01'
    index2 = '2016-09-01'

    # print date
    # print ref

    for x in range(0, len(date)):
        try:
            if date[x] < '2016':
                continue
            elif date[x] < index1:
                value = ref['2015-06-30']
                if float(value[0]) == 0:
                    pe = 0
                else:
                    pe = float(close[x]) / float(value[0])
                if float(value[1]) == 0:
                    pb = 0
                else:
                    pb = float(close[x]) / float(value[1])
            elif index1 <= date[x] < index2:
                value = ref['2015-12-31']
                if float(value[0]) == 0:
                    pe = 0
                else:
                    pe = float(close[x]) / float(value[0])
                if float(value[1]) == 0:
                    pb = 0
                else:
                    pb = float(close[x]) / float(value[1])
            elif index2 <= date[x]:
                value = ref['2016-06-30']
                if float(value[0]) == 0:
                    pe = 0
                else:
                    pe = float(close[x]) / float(value[0])
                if float(value[1]) == 0:
                    pb = 0
                else:
                    pb = float(close[x]) / float(value[1])
        except:
            continue
        update_cmd = 'update `stock_2016` set `pe_ttm` = %s, `pb` = %s where `stockid` = "%s" and `date` = "%s"' \
                     % (pe, pb, id, date[x])
        # print update_cmd
        cursor.execute(update_cmd)
    db.close()

file = open('full_list.txt')
while 1:
    line = file.readline()
    if not line:
        break
    print line[:8]
    pepb_refresh(line[:8])

# pepb_refresh('sh600000')