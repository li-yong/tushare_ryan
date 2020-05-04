#!/usr/bin/env bash
#更新当天股票数据 of the US.
# Fetch: t_daily_run_1_update_data_us.sh, Runs at 05.30AM GMT +8  (or Tuesday -- Sunday).

# Exam: t_daily_sync_to_66_us.sh,  run on 8.00 AM GMT +8 (Tuesday -- Sunday)
# Plan completed in 1 hour
# Plan the result completed by 20:00 GMT+8 prior to the US stock market opening.


# HK move to get along with AG, check AG script.

#### stopping working since 2019
#echo "updating Forbes "
#R -f /home/ryan/tushare_ryan/get_forbes.r

### Backup will be taken at the scp to haha_66

echo "updating US stock daily "
#################
# get sp500/dow consistant stocks
# output: ~/DATA/pickle/INDEX_US_HK/*.csv
python t_daily_get_us_index.py



#################
# update Index (dow, sp), get sp500/dow daily
# output: /DATA/DAY_Global/US_INDEX/#dow.csv  sp500.csv
bash -x t_get_dow_sp500.sh

#####################################
#fetch daily us/hk stocks
# output: /DATA/DAY_Global/US/*.csv
#         /DATA/DAY_Global/KH/*.csv

#python ~/tushare_ryan/t_fetch_us_hk_bar.py -x US; #very long list, 17000+ stocks
python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x US ;
#python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x HK ;
echo "Done, daily US update completed."
