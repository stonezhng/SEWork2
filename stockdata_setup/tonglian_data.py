import tushare as ts

ts.set_token('4c81816b40a4dbcc659f0017b81482281bf220618d616ac81c3287697fc0e755')
st = ts.Market()
df = st.MktEqud(tradeDate='20150917', field='ticker,secShortName,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,turnoverRate')
print df