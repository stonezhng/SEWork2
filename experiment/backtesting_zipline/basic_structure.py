from collections import OrderedDict
from datetime import datetime

import pytz
from zipline.api import order, record, symbol
from zipline.algorithm import TradingAlgorithm

# Load data manually from Yahoo! finance
from zipline.api import order_target
from zipline.data import load_bars_from_yahoo

import pandas as pd
#
# start = datetime(2011, 1, 1, 0, 0, 0, 0, pytz.utc).date()
# end = datetime(2012, 1, 1, 0, 0, 0, 0, pytz.utc).date()
#
# data = load_bars_from_yahoo(stocks=['SPY'], start=start, end=end)

data = OrderedDict()
# data['600000'] = pd.read_csv('600000.csv', index_col=0, parse_dates=['Date'])
data['600000'] = pd.read_csv('600000.csv', index_col=0, parse_dates=['Date'])
# print data['600000'].head()
panel = pd.Panel(data)

panel.minor_axis = ['open', 'high', 'low', 'close', 'volume']

panel.major_axis = panel.major_axis.tz_localize(pytz.utc)


def initialize(context):
    context.security = symbol('600000')


def handle_data(context, data):
    # MA1 = data[context.security].mavg(50)
    # MA2 = data[context.security].mavg(100)
    # date = str(data[context.security].datetime)[:10]
    # current_price = data[context.security].price
    # current_positions = context.portfolio.positions[symbol('600000')].amount
    # cash = context.portfolio.cash
    # value = context.portfolio.portfolio_value
    # current_pnl = context.portfolio.pnl
    #
    # # code (this will come under handle_data function only)
    # if (MA1 > MA2) and current_positions == 0:
    #     number_of_shares = int(cash / current_price)
    #     order(context.security, number_of_shares)
    #     record(date=date, MA1=MA1, MA2=MA2, Price=current_price, status="buy", shares=number_of_shares, PnL=current_pnl,
    #            cash=cash, value=value)
    # elif (MA1 < MA2) and current_positions != 0:
    #     order_target(context.security, 0)
    #     record(date=date, MA1=MA1, MA2=MA2, Price=current_price, status="sell", shares="--", PnL=current_pnl, cash=cash,
    #            value=value)
    # else:
    #     record(date=date, MA1=MA1, MA2=MA2, Price=current_price, status="--", shares="--", PnL=current_pnl, cash=cash,
    #            value=value)
    order(symbol('600000'), 10)
    record(Close=data['Close'])

algo_obj = TradingAlgorithm(initialize=initialize, handle_data=handle_data, capital_base=100000.0)
perf_manual = algo_obj.run(panel)
perf_manual.to_csv('output.csv')