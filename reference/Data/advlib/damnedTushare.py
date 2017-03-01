# -*- coding: UTF-8 -*-

import datetime
import urllib
from lxml import etree


def get_hist_ticks(id, date):
    buyin = 0
    saleout = 0

    page = 1
    while 1:
        url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol=' \
              + id + '&date=' + date + '&page=' + str(page)
        response = urllib.urlopen(url).read()
        tree = etree.HTML(response)
        # attr = tree.xpath('//*[@id="datatbl"]/tbody/tr[8]/th[2]/h5')
        # //*[@id="datatbl"]/tbody/tr[63]/th[2]/h1

        attr1 = tree.xpath('//*[@id="datatbl"]/tbody/tr[1]/th[2]/h5')
        attr2 = tree.xpath('//*[@id="datatbl"]/tbody/tr[1]/th[2]/h6')
        attr3 = tree.xpath('//*[@id="datatbl"]/tbody/tr[1]/th[2]/h1')

        if len(attr1) == 0 and len(attr2) == 0 and len(attr3) == 0:
            break

        num = 1
        while 1:
            attr1 = tree.xpath('//*[@id="datatbl"]/tbody/tr[' + str(num) + ']/th[2]/h5')
            attr2 = tree.xpath('//*[@id="datatbl"]/tbody/tr[' + str(num) + ']/th[2]/h6')
            attr3 = tree.xpath('//*[@id="datatbl"]/tbody/tr[' + str(num) + ']/th[2]/h1')
            if len(attr1) == 0 and len(attr2) == 0 and len(attr3) == 0:
                break
            else:
                if len(attr1) != 0:
                    buyin += 1
                elif len(attr2) != 0:
                    saleout += 1
            num += 1
            print 'page: ' + str(page) + ' num: ' + str(num)

        page += 1
    if saleout + buyin == 0:
        return None
    else:
        return float(saleout - buyin) / float(saleout + buyin)


def test():
    # url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php?symbol=sh600000&date=2016-06-16&page=1'
    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol=sh600000&date=2016-06-15&page=45'
    response = urllib.urlopen(url).read()
    tree = etree.HTML(response)
    attr = tree.xpath('//*[@id="datatbl"]/tbody/tr[5]/th[2]/h6')
    print attr[0].text


test()
# print get_hist_ticks('sh600000', '2016-06-12')