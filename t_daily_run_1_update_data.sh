#!/usr/bin/env bash
#this script runs on haha_data_source

cd ~/repo/trading/tushare_ryan

#更新当天股票数据
######################################
#Output:
# /home/ryan/DATA/DAY_Global/AG/*csv
#
######################################
python ~/repo/trading/tushare_ryan/t_daily_update_csv_from_tushare_.py;


######################################
##update SH000001 上证指数.
#
######################################

python /home/ryan/repo/trading/tushare_ryan/t_get_index.py




######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/invidual/*.csv
#
######################################
echo "daily update tushare pro fundamentals";
#----
# a full fetch. fetch everything.
#----
#python t_daily_fundamentals_2.py --fetch_data_all --force_run

#----
#full fetch. but files that has been updated in 5 days
# (which should also be the most recent Quarter) will not be check again.
#----
python t_daily_fundamentals_2.py --fetch_data_all

#----
# # fast_fetch fetch
# 1.good stock.  <<< commented now, as #fast_fetch doesn't have content.
# 2.this_q and last_q for ts.express
#----
python t_daily_fundamentals_2.py --fetch_data_all --fast_fetch  --force_run #most time, update most recent q



######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/hsgt_top10.csv
#
######################################
python t_daily_hsgt.py --fetch_data_all



######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/summary_600519.csv
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/detail_600519.csv
#
######################################
python t_top_10_holders.py --fetch


######################################
##update announcement.
#
######################################
cd /home/ryan/repo/trading/lib/China_stock_announcement_ryan/python_scraw
echo "updating today SSE announcement"
python cninfo_main.py sse ;   #/home/ryan/DATA/announcement/sse/list/2017/2017-12-26.csv

echo "updating today regulator announcement"
python cninfo_main.py regulator;  #/home/ryan/DATA/announcement/reg/list/2017/2017-12-26.csv

######################################
#
#output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/basic_todayS.csv, eg. basic_2019-03-04.csv
#
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/yyyy/feature_yyyy_q.csv. eg. 2018/cashflow_2018_4.csv
#   features:  cashflow_2018_4.csv    growth_2018_4.csv      profit_2018_4.csv
#              debtpaying_2018_4.csv  operation_2018_4.csv   report_2018_4.csv
#
#/home/ryan/DATA/pickle/gem.csv  #创业板
#                      hs300.csv ## 沪深300成份股
#                      sme.csv  #中小板
#                      sz50.csv # SZ50成份股
#                      trading_day_2019.csv
#                      ZZ500.csv #中证500成份股
#
######################################
cd ~/repo/trading/tushare_ryan
echo "updating quarter fund data of 2018 Q1 to Q4, update /home/ryan/DATA/pickle/*.pickle"
python t_daily_fundamentals.py  --fetch_data_all;


######################################
#
#
######################################
echo "updating market, security, instrument, A_stock"
python t_daily_fundamentals.py  --update_get_market;  #/home/ryan/DATA/pickle/market.csv
python t_daily_fundamentals.py  --update_get_security;   #saved to /home/ryan/DATA/pickle/security.csv

python t_daily_fundamentals.py  --update_get_instrument;  # /home/ryan/DATA/pickle/instrument.csv
python t_daily_fundamentals.py  --update_get_A_stock_instrment;  #/home/ryan/DATA/pickle/instrument_A.csv


# update forex, get forex yesterday data from td.
#bash /home/ryan/oandapybot-ubuntu/t_forex_daily_prepare.sh -e CP_YESTERDAY_FOREX_DATA_FROM_HK_VSP_TO_HAHA_BRAIN
#scp root@td:/home/ryan/DATA/DAY_Forex_local.tar.gz   /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz
#echo "saved to DAY_Forex_local.tar.gz from td to /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz"

echo "AG stock daily updated!"


# HK Stock update along with US at evening of the day, and exam on the next day morning at 8.00 AM.

######################################
### To update Fenghong required data, have to be run in home/public network on laptop daily.
### BestTV-W works. EMC-Open not work, EMC not work
## Input:
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/forecast/fd_*.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/fund_hold/fh_*.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/restricted_lifted/rl_*.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/new_stock/new_stock.pickle
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/sh_margin/sh_margin.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/sz_margin/sz_margin.csv

## Output:
## profit_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_profit_all.csv"
## forecast_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_forecast_all.csv"
## fundhold_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_fundholding_all.csv"
## xsg_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_xsg_all.csv"
## new_stock_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_new_stock.csv"
######################################
python t_fenghong.py  --fetch_reference

######################################
## input: /home/ryan/DATA/pickle/instrument_A.csv, get code from the file
## output: /home/ryan/DATA/DAY_No_Adj/*.csv
## BestTV-W works
######################################
python t_fenghong.py  --fetch_no_adj_data  --force_fetch_data



######################################  JAQS stopped working 20190305
# have to run on home, port 8910 not allowed on any public network in office.
# EMC-Open works.  BestTV-W works(faster)
# output:
# /home/ryan/DATA/DAY_JAQS/*.csv
#
######################################
#python t_monthly_jaqs_fundamental.py --fetch_data_all --force_fetch_data




#### 20190102 home thinkpad laptop replaced td to sync all days ###
#### this part has been replaced by
# python t_daily_fundamentals_2.py  --fetch_data_all @ haha_data_source ###
# and
# bash -x t_daily_sync_from_ds.sh @ haha_brain

#rsync -avz /hdd2/repo/trading/tushare_ryan  root@td:/root
#rsync -avz /home/ryan/DATA/pickle/instrument_A.csv   root@td:/root/DATA/pickle/instrument_A.csv
#
#rsync -avz root@td:/root/DATA/pickle/Stock_Fundamental/fundamentals_2/source/invidual/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/invidual
#ssh root@td reboot &


#### Now update the HK stock ####
echo "updating HK stock daily "
python ~/tushare_ryan/t_fetch_us_hk_bar.py --fetch_hk;
echo "HK daily update completed."

echo "done, script completed"
