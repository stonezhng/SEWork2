from rqalpha.api import order_percent


def init(context):
    context.s1 = '600000.XSHG'
    context.s2 = '600004.XSHG'
    context.s3 = '600007.XSHG'
    context.s4 = '600008.XSHG'
    context.s5 = '600011.XSHG'
    context.stocks = [context.s1, context.s2, context.s3, context.s4, context.s5]

def handle_bar(context, bar_dict):



    for each in context.stocks:
        order_percent(each, 1)
