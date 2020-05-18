#!/usr/bin/env bash
#this script runs on haha_data_source



cd /home/ryan/tushare_ryan

git pull

#### update the HK stock ####
echo "updating HK/US selected stock daily from tushare "
#python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x HK  --force_fetch;
python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x HK;
#python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x US  --force_fetch;
python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x US;

#更新当天股票数据
######################################
#Output:
# /home/ryan/DATA/DAY_Global/AG/*csv
#
######################################
python /home/ryan/tushare_ryan/t_daily_update_csv_from_tushare_.py;


######################################
##update SH000001 上证指数.
#
######################################

# python /home/ryan/tushare_ryan/t_get_index.py --fetch_index #script file no longer exist


######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/hsgt_top10.csv
#
######################################
python t_daily_hsgt.py --fetch_hsgt_top_10
python t_daily_hsgt.py --fetch_moneyflow_all #time consuming
python t_daily_hsgt.py --fetch_moneyflow_daily  #time consuming (a little)



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
cd /home/ryan/tushare_ryan
echo "updating quarter fund data of 2018 Q1 to Q4, update /home/ryan/DATA/pickle/*.pickle"
python t_daily_fundamentals.py  --fetch_data_all;


######################################
#
#
######################################
echo "updating market, security, instrument, A_stock"
python t_daily_fundamentals.py  --update_get_market;  #/home/ryan/DATA/pickle/market.csv
python t_daily_fundamentals.py  --update_get_security;   #saved to /home/ryan/DATA/pickle/security.csv #stop work. 20200513

python t_daily_fundamentals.py  --update_get_instrument;  # /home/ryan/DATA/pickle/instrument.csv
python t_daily_fundamentals.py  --update_get_A_stock_instrment;  #/home/ryan/DATA/pickle/instrument_A.csv #stop work. 20200513


# update forex, get forex yesterday data from td.
#bash /home/ryan/oandapybot-ubuntu/t_forex_daily_prepare.sh -e CP_YESTERDAY_FOREX_DATA_FROM_HK_VSP_TO_HAHA_BRAIN
#scp root@td:/home/ryan/DATA/DAY_Forex_local.tar.gz   /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz
#echo "saved to DAY_Forex_local.tar.gz from td to /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz"

echo "AG stock daily updated!"




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
# python t_daily_fundamentals_2.py --fetch_data_all  #time consuming.

#----
# # fast_fetch fetch
# 1.good stock.  <<< commented now, as #fast_fetch doesn't have content.
# 2.this_q and last_q for ts.express
#----
python t_daily_fundamentals_2.py --fetch_data_all --fast_fetch  #most time, update most recent q.  #time consuming.





######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/summary_600519.csv
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/detail_600519.csv
#
######################################
# not run because time consuming. 20200518
# python t_top_10_holders.py --fetch  #time consuming




# HK Stock update along with US at evening of the day, and exam on the next day morning at 8.00 AM.


######################################
## input: /home/ryan/DATA/pickle/instrument_A.csv, get code from the file
## output: /home/ryan/DATA/DAY_No_Adj/*.csv
## BestTV-W works
######################################
# not run because time consuming. 20200518
# python t_fenghong.py  --fetch_no_adj_data  --force_fetch_data #time consuming



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



echo "updating HK/US all stocks daily from tushare "
python ~/tushare_ryan/t_fetch_us_hk_bar.py -x HK; #time consuming

#14000 companies, and tushare no longer have updates since 2020/04/27
#python ~/tushare_ryan/t_fetch_us_hk_bar.py -x US;
echo "HK/US daily update completed."

echo "done, script completed"
