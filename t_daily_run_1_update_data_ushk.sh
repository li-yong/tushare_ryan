#!/usr/bin/env bash
#更新当天股票数据 of the US.
# Fetch: t_daily_run_1_update_data_us.sh, Runs at 05.30AM GMT +8  (or Tuesday -- Sunday).

# Exam: t_daily_sync_to_66_us.sh,  run on 8.00 AM GMT +8 (Tuesday -- Sunday)
# Plan completed in 1 hour
# Plan the result completed by 20:00 GMT+8 prior to the US stock market opening.


# HK move to get along with AG, check AG script.

####
echo "updating Forbes "
R -f /home/ryan/tushare_ryan/get_forbes.r

### Backup will be taken at the scp to haha_66

echo "updating US stock daily "
python t_daily_get_us_index.py


#update Index (dow, sp)
bash -x t_get_dow_sp500.sh
python t_format_dow_sp_index_csv.py


python ~/tushare_ryan/t_fetch_us_hk_bar.py --fetch_us;
echo "Done, daily US (CH,MG,US) update completed."
