import stockdata_refresh
import bench_refresh
import CQ_refresh
import evaluate_refresh
import industry_refresh
import recommend_refresh
import relative_refresh
import range_refresh

import bp_refresh
import svm_refresh
import bench_forecast_refresh

# stockdata_refresh.refresh()
#
# bench_refresh.refresh_bench()
#
# CQ_refresh.add_committee()

evaluate_refresh.refresh_index()
evaluate_refresh.refresh_rank()

industry_refresh.refresh_industry()

recommend_refresh.refresh_recommend()

relative_refresh.refresh_relative()

range_refresh.refresh_range()

bp_refresh.refresh_bp()
svm_refresh.refresh_svm()
bench_forecast_refresh.refresh_benchforecast()
