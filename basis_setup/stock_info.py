# encoding=utf8
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import json
import urllib2

import MySQLdb


def get_descript(id):
    url = 'https://api.wmcloud.com:443/data/v1/' \
          'api/equity/getEqu.json?field=listStatusCD,listDate,delistDate,primeOperating,secFullName' \
          '&ticker='+id[2:]+'&secID=&equTypeCD=&listStatusCD='
    req = urllib2.Request(url)
    req.add_header("Authorization", 'Bearer 4c81816b40a4dbcc659f0017b81482281bf220618d616ac81c3287697fc0e755')
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    raw_data = json.loads(res)
    if 'data' in raw_data:
        raw_data = json.loads(res)['data']
        temp = []
    # id = ''
    # if re.match(shReg, str(['ticker'])):
    #     id = 'sh' + str(raw_data[0]['ticker'])
    # elif re.match(szReg, str(raw_data[0]['ticker'])):
    #      id = 'sz' + str(raw_data[0]['ticker'])
    # temp.append(id)
        temp.append(raw_data[0]['secFullName'])
        temp.append(raw_data[0]['listDate'])
        if 'delistDate' in raw_data[0]:
            temp.append(raw_data[0]['delistDate'])
        else:
            temp.append(None)
        status = None
        if raw_data[0]['listStatusCD'] == 'L':
            status = '上市'
        elif raw_data[0]['listStatusCD'] == 'S':
            status = '暂停'
        elif raw_data[0]['listStatusCD'] == 'DE':
            status = '已退市'
        elif raw_data[0]['listStatusCD'] == 'UN':
            status = '未上市'
        temp.append(status)
        des = raw_data[0]['primeOperating']
        des = des.replace('"', "'")
        temp.append(des)
        return temp
    else:
        return [None, None, None, None, None]


def create_info():
    file = open('full_list.txt')
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    while 1:
        line = file.readline()
        if not line:
            break
        print line[:8]
        info = get_descript(line[:8])
        info.append(line[:8])
        update_cmd = 'update `sw_stock_info` set `fullName` = "%s", `listDate` = "%s", `delistDate` = "%s", ' \
                     ' `status` = "%s", `description` = "%s" where `stockid` = "%s"' % tuple(info)
        print update_cmd
        cursor.execute(update_cmd)

    db.commit()
    db.close()


# db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
#                          port=8161, charset="utf8")
# cursor = db.cursor()
# cursor.execute('update `sw_stock_info` set `delistDate` = null')
create_info()