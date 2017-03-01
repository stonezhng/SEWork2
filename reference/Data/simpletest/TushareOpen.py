import sys
import tushare as ts
from itertools import islice
from scipy import stats
import csv
reload(sys)
sys.setdefaultencoding("utf-8")


def get_history(id, start_date, end_date):
    history = ts.get_hist_data(id, start=start_date, end=end_date)
    history.to_csv(id + "history.csv")

    csvfile = file(id+'history.csv')
    reader = csv.reader(csvfile)
    op = []
    high = []
    low = []
    close = []
    volume = []
    price_change = []
    turnover = []
    for line in islice(reader, 1, None):
        op.append(float(line[1]))
        high.append(float(line[2]))
        low.append(float(line[4]))
        close.append(float(line[3]))
        volume.append(float(line[5]))
        price_change.append(float(line[6]))
        turnover.append(float(line[14]))

    data = {'op': op, 'high': high, 'low': low, 'close': close, 'volume': volume,
           'price_change': price_change, 'turnover': turnover}
    return data


def get_open_statistics(id, start_date, end_date):
    stock_data = get_history(id, start_date, end_date)['op']
    stock_info = stats.describe(stock_data)
    print stock_info[2]
    print stock_info[3]
    print stock_info[4]
    print stock_info[5]

get_open_statistics('600000', '2015-11-11', '2015-12-22')



