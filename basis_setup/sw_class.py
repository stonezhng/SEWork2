import MySQLdb
import re
import xlrd
#
# data = xlrd.open_workbook('swindustry.xlsx')
# table = data.sheets()[0]
# nrows = table.nrows
# cell_A1 = table.cell(0, 0).value
# print cell_A1


def create_sw_stock():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()

    data = xlrd.open_workbook('swindustry.xlsx')
    table = data.sheets()[0]
    nrows = table.nrows

    shReg = "(600[0-9]{3}|601[0-9]{3}|900[0-9]{3}|603[0-9]{3})"
    szReg = "(000[0-9]{3}|002[0-9]{3}|300[0-9]{3}|200[0-9]{3}|001[0-9]{3})"

    resource = []
    former = ''
    count = 0

    for x in range(1, nrows):
        industry = table.cell(x, 0).value
        if former != industry:
            count += 1
            former = industry
        id = table.cell(x, 1).value
        print id
        stockid = ''
        if re.match(shReg, id):
            stockid = 'sh' + id
        # print id
        elif re.match(szReg, id):
            stockid = 'sz' + id
        else:
            stockid = id
        name = table.cell(x, 2).value
        temp = [industry, stockid, name, count]
        resource.append(temp)
        # file.write(stockid + '\n')
    cursor.executemany('insert into `sw_stock_info` (`industry`, `stockid`, `name`, `industryid`) VALUES (%s, %s, %s, %s)', tuple(resource))
    db.commit()
    db.close()


def create_sw_industry():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    cursor.execute('select distinct(`industry`),  `industryid` from `sw_stock_info` order by `industryid`')
    industrylist = list(cursor.fetchall())
    industry = []
    for each in industrylist:
        temp = [each[0], each[1]]
        industry.append(temp)
    cursor.executemany('insert into `sw_industry`(`industry`, `industryid`) VALUES (%s, %s)', tuple(industry))
    db.commit()
    db.close()


def create_idlist():
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    cursor.execute('select distinct(`stockid`) from `sw_stock_info` order by `stockid`')
    idlist = list(cursor.fetchall())
    file = open('full_list.txt', 'w')
    for each in idlist:
        file.write(each[0] + '\n')
    file.close()

# create_sw_stock()
create_sw_industry()