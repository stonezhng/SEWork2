import sys
import tushare as ts
reload(sys)
sys.setdefaultencoding("utf-8")


param = []
for i in range(1, len(sys.argv)):
    param.append(sys.argv[i])
df = ts.get_profit_data(int(param[0]), int(param[1]))
df.to_csv('profit.csv')
