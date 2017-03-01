import MySQLdb
import pytz
from datetime import datetime

from pandas import Series, DataFrame

from zipline.algorithm import TradingAlgorithm
from zipline.api import order, record, symbol

import pandas as pd

# Load data manually csv
#Date,Open,High,Low,Close,Volume,Adj Close
#1984-09-07,26.5,26.87,26.25,26.5,2981600,3.02
#...

parse = lambda x: pytz.utc.localize(datetime.strptime(x, '%Y-%m-%d'))
data = pd.read_csv('600000.csv', parse_dates=['Date'], index_col=0, date_parser=parse)
# print type(data)


def generate_data(stockid, start, end, column):
    select_cmd = 'select `%s` from `stock_%s` where `stockid` = "%s" and `date` >= "%s" and `date` <= "%s" order by `date`'

    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "dracarys",
                         port=8161, charset="utf8")
    cursor = db.cursor()
    start_year = int(start[0:4])
    end_year = int(end[0:4]) + 1

    data = {}

    date = []
    year = start_year
    while year != end_year:
        cursor.execute(select_cmd % ('date', str(year), stockid, start, end))
        raw = cursor.fetchall()
        for every in raw:
            date.append(pytz.utc.localize(datetime.strptime(every[0], '%Y-%m-%d')))
        year += 1

    for each in column:
        result = []
        year = start_year
        while year != end_year:
            cursor.execute(select_cmd % (each, str(year), stockid, start, end))
            raw = cursor.fetchall()
            for every in raw:
                result.append(every[0])
            year += 1
        data[each] = result

    columns = ['date'].extend(column)
    return DataFrame(data, index=date, columns=None)


# Define algorithm
def initialize(context):
    pass


def handle_data(context, data):
    order('Close', 10)
    record(Close=data['Close'])

data = generate_data('sh600000', '2010-01-01', '2015-01-01', ['open', 'high', 'low', 'close'])
print data

# Create algorithm object passing in initialize and
# handle_data functions
algo_obj = TradingAlgorithm(initialize=initialize,
                            handle_data=handle_data)

# Run algorithm
perf_manual = algo_obj.run(data)

# Print
perf_manual.to_csv('output.csv')
