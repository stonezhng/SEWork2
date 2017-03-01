from zipline.api import order, record, symbol

# python run_algo.py -f dual_moving_average.py --symbols AAPL --start 2011-1-1 --end 2012-1-1 -o dma.pickle

def initialize(context):
    pass


def handle_data(context, data):
    order(symbol('AAPL'), 10)
    record(AAPL=data.current(symbol('AAPL'), 'price'))