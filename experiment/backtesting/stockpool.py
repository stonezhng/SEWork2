# -*- coding: UTF-8 -*-
import MySQLdb
# 创建一个股票池这些步骤：从index_manager里选到用过的股票池表名，将其加一作为新表名
# 从index_manager里选用过的详细表表名， 先放着，以后有用
# 用新表名创建一个股票池表以后，向里添加内容，每一行是一个股票id，默认持有量和收益值都是0，还有就是这id对应的详细表表名
# 这时就有用到之前存的那个已经用过的表名的最大值了，每次加一和一个股票id匹配成一行存进股票池表里
# 每添加一个条目，就需要创建一个表，表名是详细表表名
# 最后在index_manager里把这张股票池表和详细表的对应关系全部增加进去


def create_pool(idlist):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    cursor.execute('select max(`pool_index`), max(`detail_index`) from `index_manager`')
    raw = list(cursor.fetchall())
    if raw[0][0] is None:
        name = 'stockpool_0'
        detail = 0
    else:
        name = 'stockpool_' + str((raw[0][0]) + 1)
        detail = int(raw[0][1]) + 1
    create_cmd = 'create table `%s` (`stockid` varchar(40) not null, `volume` bigint default 0, `' \
                 'income` float default 0, `detail_index` text, primary key(`stockid`)) ' \
                 'ENGINE=MyISAM DEFAULT CHARSET=utf8;' % name
    cursor.execute(create_cmd)

    data = []
    detail_list = []

    insert_cmd = 'insert into `%s` (`stockid`, `detail_index`) VALUES ("%s", "%s")'

    for each in idlist:
        temp = [name]
        temp.append(each)
        temp.append('detail_'+str(detail))
        data.append(tuple(temp))
        cursor.execute(insert_cmd % tuple(temp))
        detail_list.append(detail)
        detail += 1
    print data
    # cursor.executemany(insert_cmd, tuple(data))
    db.commit()

    index = []
    for each in detail_list:
        index.append([int(name[10:]), each])
    insert_cmd = 'insert into `index_manager` (`pool_index`, `detail_index`) VALUES (%s, %s)'
    cursor.executemany(insert_cmd, tuple(index))

    for each in detail_list:
        detail_name = 'detail_' + str(each)
        cursor.execute('create table `%s` (`index` bigint auto_increment, `volume` bigint default 0, '
                       '`income` float default 0, `date` text, primary key(`index`)) '
                       ' ENGINE=MyISAM DEFAULT CHARSET=utf8;'
                       % detail_name)
    db.commit()

    db.close()


def withdraw_pool(pool_name):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161)
    cursor = db.cursor()
    pool_index = int(pool_name[10:])
    cursor.execute('select `detail_index` from `index_manager` where `pool_index` = %s' % pool_index)
    raw = cursor.fetchall()
    for each in raw:
        cursor.execute('drop table `%s`' % ('detail_' + str(each[0])))
    cursor.execute('delete from `index_manager` where `pool_index` = %s' % pool_index)
    cursor.execute('drop table `%s`' % pool_name)
    db.close()

withdraw_pool('stockpool_0')
# create_pool(['sh600000', 'sz002644', 'sh600004'])
