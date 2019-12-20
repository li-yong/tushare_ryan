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


python t_daily_get_key_points.py  -x KG  --calc_base
python t_daily_get_key_points.py  -x KG  --calc_today
python t_daily_get_key_points.py  -x KG  --today_selection


python t_daily_get_key_points.py  -x KH  --calc_base
python t_daily_get_key_points.py  -x KH  --calc_today
python t_daily_get_key_points.py  -x KH  --today_selection


python t_daily_get_key_points.py  -x MG  --calc_base
python t_daily_get_key_points.py  -x MG  --calc_today
python t_daily_get_key_points.py  -x MG  --today_selection

python t_daily_get_key_points.py  -x US  --calc_base
python t_daily_get_key_points.py  -x US  --calc_today
python t_daily_get_key_points.py  -x US  --today_selection




### HK First, because it take less time  ########
python t_daily_pattern_Hit_Price_Volume.py -0  -m 30 -x KG #it was 22 ->222 ->30
#python t_daily_pattern_Hit_Price_Volume.py -1 -2 -4   -m 30 -x KG #it was 22 ->222 ->30
python t_summary.py -x KG --action generate_report

python t_daily_pattern_Hit_Price_Volume.py -0  -m 30 -x KH #it was 22 ->222 ->30
#python t_daily_pattern_Hit_Price_Volume.py  -1 -2 -4   -m 30 -x KH #it was 22 ->222 ->30
python t_summary.py -x KH --action generate_report

#this is local run
python t_daily_pattern_Hit_Price_Volume.py -0 -m 30 -x CH;
#python t_daily_pattern_Hit_Price_Volume.py  -1 -2 -4  -m 30 -x CH;
python t_summary.py -x CH --action generate_report

python t_daily_pattern_Hit_Price_Volume.py -0 -m 30 -x MG;
#python t_daily_pattern_Hit_Price_Volume.py  -1 -2 -4  -m 30 -x MG;
python t_summary.py -x MG --action generate_report

python t_daily_pattern_Hit_Price_Volume.py -0 -m 30 -x US;
#python t_daily_pattern_Hit_Price_Volume.py  -1 -2 -4  -m 30 -x US;
python t_summary.py -x US --action generate_report
