import MySQLdb


def establish_recommend(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    cursor.execute('select `date` from `stock_2016` where `stockid` = "'+id+'" group by date desc')
    raw = list(cursor.fetchall())
    if not raw:
        cursor.execute('update `recommend` set `isStop` = %s, `isKDJ` = %s, `isBOLL` = %s, `isRSI` = %s '
                       ' where `stockid` = "%s" ' % (0, 0, 0, 0, id))
        db.commit()
        db.close()
        return

    date1 = raw[0][0]
    date2 = raw[1][0]
    cursor.execute('select `close`, `deviation_per`, `boll_middle`, `boll_upper`, `boll_low`, `slowK`, `slowD`, `rsi12` '
                   ' from `stock_2016` where '
                   '`stockid` = "'+id+'" and `date` <= "'+date1+'" and `date` >= "'+date2+'" order by `date` desc')
    raw = list(cursor.fetchall())
    close = raw[0][0]
    deviation = raw[0][1]*0.01
    boll_middle = raw[0][2]
    boll_upper = raw[0][3]
    boll_low = raw[0][4]
    rsi12 = raw[0][7]

    slowK = [raw[0][5], raw[1][5]]
    slowD = [raw[0][6], raw[1][6]]
    cursor.execute('select MAX(`date`) from `short_ticks` where `stockid` = "'+id+'"')
    date = list(cursor.fetchall())[0][0]
    if date is None:
        # cursor.execute('insert into `recommend` (`stockid`, `isStop`, `isKDJ`, `isBOLL`, `isRSI`) '
        #                'VALUES ("%s", %s, %s, %s, %s)' % (id, 0, 0, 0, 0))
        cursor.execute('update `recommend` set `isStop` = 0, `isKDJ` = 0, `isBOLL` = 0, `isRSI` = 0 where `stockid` = "'+id+'"')
    else:
        cursor.execute('select `quantity_ratio` from `short_ticks` where `stockid` = "'+id+'" and `date` = "'+date+'"')
        quantity = list(cursor.fetchall())[0][0]

        Stop = isStop(quantity, deviation)
        KDJ = isKDJ(slowK, slowD)
        BOLL = isBOLL(boll_upper, boll_low, boll_middle, close)
        rsi = isRSI(rsi12)

        # cursor.execute('insert into `recommend` (`stockid`, `isStop`, `isKDJ`, `isBOLL`, `isRSI`) '
        #            'VALUES ("%s", %s, %s, %s, %s)' % (id, Stop, KDJ, BOLL, rsi))
        cursor.execute('update `recommend` set `isStop` = %s, `isKDJ` = %s, `isBOLL` = %s, `isRSI` = %s '
                       ' where `stockid` = "%s" ' % (Stop, KDJ, BOLL, rsi, id))
    db.commit()
    db.close()


def isStop(quantity, deviation):
    if quantity is None or deviation is None:
        return 0
    elif deviation>=0.07 and quantity<=1.3  and quantity>=0.7:
        return 1
    else:
        return 0


def isKDJ(slowK, slowD):
    if slowK is None or slowD is None:
        return 0
    elif slowK[0] < slowD[0] and slowK[1] > slowD[1]:
        return 1
    else:
        return 0


def isBOLL(boll_upper, boll_low, boll_middle, close):
    if boll_upper is None or boll_low is None or boll_middle is None or close is None:
        return 0
    elif (boll_upper-boll_low)/ boll_middle < 0.1 and close >boll_middle :
        return 1
    else:
        return 0


def isRSI(rsi):
    if rsi is None:
        return 0
    elif rsi>60 and rsi<85:
        return 1
    else:
        return 0


def refresh_recommend():
    # file = open('/root/python_script/full_list.txt')
    file = open('full_list.txt')
    while 1:
        line = file.readline()
        if not line:
            break
        print 'recommend: ' + line[:8]
        establish_recommend(line[:8])

# refresh_recommend()