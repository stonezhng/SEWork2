
import pytz
from datetime import datetime
from zipline.api import order, symbol, record, order_target
from zipline.algorithm import TradingAlgorithm
from zipline.utils.factory import load_bars_from_yahoo
import zipline
import pandas as pd

# start = datetime(2011, 1, 1, 0, 0, 0, 0, pytz.utc).date()
# end = datetime(2012, 1, 1, 0, 0, 0, 0, pytz.utc).date()

d = pd.read_csv('600000.csv', index_col='Date', parse_dates=True, dayfirst=True)
d = d.rename(columns=str.lower)

data = pd.Panel({zipline.assets.Equity(24, '600000'): d})
data.major_axis = data.major_axis.tz_localize(pytz.utc)

# print data


def initialize(context):
    context.security = symbol('600000')


def handle_data(context, data):
    order(symbol('600000'), 10)
    record(puyh=data['Close'])
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

algo_obj = TradingAlgorithm(initialize=initialize, handle_data=handle_data)

perf_manual = algo_obj.run(data)
perf_manual.to_csv('out.csv')