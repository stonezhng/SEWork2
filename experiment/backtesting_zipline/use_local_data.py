import pytz
from datetime import datetime

from zipline.algorithm import TradingAlgorithm
from zipline.api import fetch_csv
from zipline.api import order, record, symbol

import pandas as pd

# Load data manually csv
#Date,Open,High,Low,Close,Volume,Adj Close
#1984-09-07,26.5,26.87,26.25,26.5,2981600,3.02
#...

parse = lambda x: pytz.utc.localize(datetime.strptime(x, '%d/%m/%Y'))
# data = fetch_csv('AAPL.csv',  date_column='Date',)
data = pd.read_csv('AAPL.csv', parse_dates=['Date'], index_col=0, date_parser=parse)
print type(data)

# print data['Close']

# Define algorithm
def initialize(context):
    pass


def handle_data(context, data):
    order(symbol('AAPL'), 10)
    record(AAPL=data['Close'])

# Create algorithm object passing in initialize and
# handle_data functions
algo_obj = TradingAlgorithm(initialize=initialize,
                            handle_data=handle_data)

# Run algorithm
print data
perf_manual = algo_obj.run(data)

# Print
perf_manual.to_csv('output.csv')