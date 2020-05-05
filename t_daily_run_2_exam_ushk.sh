#!/usr/bin/env bash
#exam start
#rm -rf /home/ryan/DATA/result/today.del
#mv  /home/ryan/DATA/result/today ~/DATA/result/today.del
#mkdir  /home/ryan/DATA/result/today

cd   /home/ryan/repo/trading/tushare_ryan

################################################
#input:  /home/ryan/DATA/DAY_Global/CH|KG..|/
#output:  /home/ryan/DATA/result/key_points_AG_today_selected.csv
################################################
## add --force_run to force run
python t_daily_get_key_points.py  -x CH  --calc_base #slow (10hours).  run every 7 days. output: /home/ryan/DATA/result/key_points_CH.csv
python t_daily_get_key_points.py  -x CH  --calc_today  #fast. output: /home/ryan/DATA/result/key_points_CH_today.csv
python t_daily_get_key_points.py  -x CH  --today_selection #fast. output: /home/ryan/DATA/result/key_points_CH_today_selected.csv



#python t_daily_get_key_points.py  -x MG  --calc_base
#python t_daily_get_key_points.py  -x MG  --calc_today
#python t_daily_get_key_points.py  -x MG  --today_selection

python t_daily_get_key_points.py  -x US  --selected --calc_base
python t_daily_get_key_points.py  -x US  --selected --calc_today
python t_daily_get_key_points.py  -x US  --selected --today_selection

python t_daily_get_key_points.py  -x HK  --selected --calc_base
python t_daily_get_key_points.py  -x HK  --selected --calc_today
python t_daily_get_key_points.py  -x HK  --selected --today_selection


#/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_us.csv
python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x US --selected

#/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_hk.csv
python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x HK --selected
python t_summary.py -x HK --action generate_report

#this is local run
python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x MG
