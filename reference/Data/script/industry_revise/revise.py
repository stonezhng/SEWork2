# -*- coding: UTF-8 -*-
import json
import urllib2

import MySQLdb

def revise():
    select_cmd = 'select distinct `industry` from `company`'
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    create_cmd = """create table `industry_tag` (
        `industryid` bigint NOT NULL auto_increment,
        `industry` text,
        PRIMARY KEY(`industryid`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
       """
    cursor.execute(create_cmd)
    cursor.execute(select_cmd)
    data = tuple(cursor.fetchall())
    insert_cmd = 'insert into `industry_tag` (`industry`) VALUES (%s)'
    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()


def revise_company():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    join_cmd = 'select `company`.`stockid`, `company`.`name`, `company`.industry, `industry_tag`.`industryid`' \
               'from `company` LEFT JOIN `industry_tag` ON `company`.`industry` = `industry_tag`.`industry`'
    cursor.execute(join_cmd)
    raw = list(cursor.fetchall())

    print raw[-1]

    cursor.execute('drop table `stock_info`')

    create_cmd = """create table `stock_info` (
        `stockid` varchar(40) NOT NULL ,
        `name` text,
        `industry` text,
        `industryid` bigint,
        `fullName` text,
        `listDate` text,
        `delistDate` text,
        `status` text,
        `total` bigint,
        `description` text,
        PRIMARY KEY(`stockid`)
        )ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """

    cursor.execute(create_cmd)

    data = []
    # id = raw[0][0]
    # print id
    # info = get_descript(id)
    # # print info
    # temp = list(raw[0])
    # temp.extend(info)
    # print temp
    # data.append(temp)
    # data = tuple(data)
    for x in range(0, len(raw)):
        id = raw[x][0]
        print id
        info = get_descript(id)
        temp = list(raw[x])
        temp.extend(info)
        data.append(temp)
    insert_cmd = 'INSERT INTO `stock_info` (`stockid`, `name`, `industry`, `industryid`, `fullName`, ' \
                     '`listDate`, `delistDate`, `status`, `total`, `description`) ' \
                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    cursor.executemany(insert_cmd, data)
    db.commit()
    db.close()


def get_descript(id):
    url = 'https://api.wmcloud.com:443/data/v1/' \
          'api/equity/getEqu.json?field=listStatusCD,listDate,delistDate,primeOperating,secFullName,totalShares' \
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
        temp.append(int(raw_data[0]['totalShares']))
        temp.append(raw_data[0]['primeOperating'])
        return temp
    else:
        return [None, None, None, None, None, None]


revise_company()