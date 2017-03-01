# coding=utf-8
import MySQLdb


def pe_pb_calc(id, year):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                     port=8161)
    cursor = db.cursor()
    select_cmd = 'select `close`, `date` from `stock_%s` where `stockid` = "%s"' % (year, id)
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())

    close = []
    date = []

    for each in raw:
        close.append(each[0])
        date.append(each[1])

    former = str(int(year) - 1)
    select_cmd = 'select `date`, `每股收益_调整后`, `每股净资产_调整后` from `season` where (`date` = "%s"  or `date` = "%s"' \
                 'or `date`  = "%s") and `stockid` = "%s" group by `date`' \
                 % (former+'-06-30', former+'-12-31', year+'-06-30', id)
    index1 = year + '05'
    index2 = year + '09'

    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    ref_peval = []
    ref_pbval = []
    for each in raw:
        if each[0] == former+'-12-31':
            ref_peval.append(each[1])
            ref_pbval.append(each[2])
        else:
            ref_peval.append(2*float(each[1]))
            ref_pbval.append(2*float(each[2]))

    for x in range(0, len(date)):
        try:
            if date[x] < index1:
                if float(ref_peval[0]) == 0:
                    pe = 0
                else:
                    pe = float(close[x]) / float(ref_peval[0])
                if float(ref_pbval[0]) == 0:
                    pb = 0
                else:
                    pb = float(close[x]) / float(ref_pbval[0])
            elif index1 <= date[x] < index2:
                if float(ref_peval[1]) == 0:
                    pe = 0
                else:
                    pe = float(close[x]) / float(ref_peval[1])
                if float(ref_pbval[1]) == 0:
                    pb = 0
                else:
                    pb = float(close[x]) / float(ref_pbval[1])
            elif index2 <= date[x]:
                if float(ref_peval[2]) == 0:
                    pe = 0
                else:
                    pe = float(close[x]) / float(ref_peval[2])
                if float(ref_pbval[2]) == 0:
                    pb = 0
                else:
                    pb = float(close[x]) / float(ref_pbval[2])
        except:
            continue
        update_cmd = 'update `stock_%s` set `pe_ttm` = %s, `pb` = %s where `stockid` = "%s" and `date` = "%s"' \
                     % (year, pe, pb, id, date[x])
        cursor.execute(update_cmd)
    db.close()


def create_pepb(year):
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        print year + ' ' + line[:8]
        pe_pb_calc(line[:8], year)

create_pepb('2016')
create_pepb('2015')
create_pepb('2014')
