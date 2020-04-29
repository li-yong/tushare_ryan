#!/usr/bin/env bash
#exam start
#rm -rf /home/ryan/DATA/result/today.del
#mv  /home/ryan/DATA/result/today ~/DATA/result/today.del
#mkdir  /home/ryan/DATA/result/today

full_or_daily=$1

if [ $full_or_daily == "FULL" ]; then
    echo "$0: FULL RUN"
elif [ $full_or_daily == "DAILY" ]; then
    echo "$0: DAILY RUN"
else
    echo "Have to specify FULL or DAILY. Quiting";
    exit
fi


sleep 2

cd /home/ryan/tushare_ryan

#@todo: focus on date after 2005? when the gu quan fengzhi starts
#@todo:  each fields name and meaning, adding to the code py script.

#### Process AG ########
#mysqldump -uroot -padmin888.@_@  ryan_stock_db order_tracking_stock > order_tracking_stock.sql.backup_`hostname`_`date +"%Y-%m-%d_%H_%M_%S"`


########################################################  Step1 Others  ################################################

##########################
#input:
#    /home/ryan/DATA/announcement/reg/list/2019/2019-01-28.csv
#    /home/ryan/DATA/announcement/sse/list/2019/2019-01-28.csv
#output: /home/ryan/DATA/result/today/announcement.csv
##########################
if [ $full_or_daily == "FULL" ]; then
     echo "NOT RUN"
     #rm -f /home/ryan/DATA/result/today/announcement.csv
     #python t_daily_announcement.py
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "NOT RUN"
    #python t_daily_announcement.py
fi




################################################ OBSOLATED, JAQS
#input: "//home/ryan/DATA/DAY_JAQS/*.csv"
#output: "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv".
# header: year_quarter,code,name,close,symbol,ps,ps_ttm,pe,pb,price_div_dps,float_mv,net_assets,pcf_ncf,pcf_ncfttm,pcf_ocf,pcf_ocfttm
################################################
if [ $full_or_daily == "FULL" ]; then
    echo "NOT RUN"
    #rm -f /home/ryan/DATA/result/jaqs_quarterly_fundamental.csv
    #python t_monthly_jaqs_fundamental.py --calc_quartly_report  --force_run  #<<< 16 min
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "NOT RUN"
    #python t_monthly_jaqs_fundamental.py --calc_quartly_report
fi


#python t_monthly_jaqs_fundamental.py --calc_quartly_report --force_run

################################################
#input: "/home/ryan/DATA/DAY_No_Adj/*.csv"
#         /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/*.csv
#output: analyze_one:"/home/ryan/DATA/result/fenghong_profit_analysis.csv"
#        analyze_two: /home/ryan/DATA/result/fenghong_score.csv'
################################################
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fenghong_profit_analysis.csv
    rm -f /home/ryan/DATA/result/fenghong_score.csv

    python t_fenghong.py --analyze_one --force_run  #<<< 3 min
    python t_fenghong.py --analyze_two --force_run  #<<< 1 min

fi

if [ $full_or_daily == "DAILY" ]; then
    python t_fenghong.py --analyze_one
    python t_fenghong.py --analyze_two
fi


#python t_fenghong.py --analyze_one  --force_run #fenhong run on laptop + home only, because it fetches data on public network. <<< NOT TRUE. 20181125
#python t_fenghong.py --analyze_two  --force_run


######################################################## Step2 FUND2 ################################################
################################################
#input:
#output:
#
################################################
#bash -x t_quarterly_fund_2.sh  #<<< stuck

#########################
#input:  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/hsgt_top10.csv
#output : /home/ryan/DATA/result/hsgt_top_10_selected.csv
#########################
python t_daily_hsgt.py --analyze


#########################
#input:  /home/ryan/DATA/DAY_Global/AG_MoneyFlow/*.csv
#output : /home/ryan/DATA/tmp/moneyflow_ana/history_moneyflow_hit.csv
#         /home/ryan/DATA/tmp/moneyflow_ana/today_moneyflow_hit.csv
#########################
python t_daily_hsgt.py --analyze_moneyflow --mf_ana_pre_days 3 --mf_ana_test_hold_days 5

######################### merge_local_basic
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_*.csv
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv
#########################

if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv; # WARNNING, will spend hours to generate.
    python t_daily_fundamentals_2.py --merge_local_basic --force_run  # <<< ??? hour. update weekly.
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py --merge_local_basic --fast_fetch ## only merge last trading days to
fi


######################### merge_local
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/yyyymmdd/*.csv  <<< updated. 20190218. This file have dup records if tushare source has.
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv [fina_mainbz_sum.csv,balancesheet.csv,dividend.csv,fina_audit.csv,fina_mainbz.csv,forecast.csv]
#########################

if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv; # WARNNING, will spend hours to generate.
    python t_daily_fundamentals_2.py --merge_local --force_run  # <<< 2.5 hour. update everyday, since its output is other matrics input.;
fi


#03_07 23:09:00 file updated in 3 days, not process. /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/income.csv
if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py --merge_local --fast_fetch
fi




#########################  merge_individual
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/*.csv
#This step is very quickly. 20190219
#########################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/*.csv  # dangerous, will spend hours to generate
    python t_daily_fundamentals_2.py --merge_individual --force_run  #<<<< 1 hour
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py --merge_individual --fast_fetch #@todo, bug: only merge the latest day, but not the days which has not been in the target.
fi



######################### sum_mainbz
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz.csv
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_sum.csv
#########################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_sum.csv  # dangerous, will spend hours to generate
    python t_daily_fundamentals_2.py  --sum_mainbz --force_run  #<<< 20 min
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --sum_mainbz
fi



######################### merge_quarterly
#merge all 9 tables to one table, "merged_all_"+end_date+".csv"
#input:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
#output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_20190630.csv  <<< These files removed the dup records.
#                                                                             the file has not on-market will not show up.
#        /home/ryan/DATA/result/jaqs/jaqs_all.pickle
#        /home/ryan/DATA/result/jaqs/ts_all.pickle
########################
#merge all quarter data

#this will update jaqs_all.pickle, and generate a place holder ts_all.pickle
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/result/jaqs/jaqs_all.pickle.  JAQS is DEAD.
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/*.csv
    python t_daily_fundamentals_2.py  --merge_quarterly --big_memory  --force_run # <<<  2 hour . jaqs_all.pickle regenated, while ts_all.pickle is invalid.

    #generate real ts_all.pickle based on the merged/*.csv just generated.
    rm -f /home/ryan/DATA/result/jaqs/ts_all.pickle
    python t_daily_fundamentals_2.py --big_memory #  <<< 5 min ,  ts_all.pickle is valid now. geneated from --merge_quarterly output.
fi

#only merge 1q before data. Use when the script runs stable.
if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --merge_quarterly --big_memory --fast_fetch  #<<< only merage current, e.g. 20181231
fi

######################### extract_latest
#
#input:source/*.csv
#output: source/latest/*.csv
##########################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/*.csv
    python t_daily_fundamentals_2.py  --extract_latest --force_run #<<< 20 min
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --extract_latest
fi

######################### disclosure_date_notify_day
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date.csv
#output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date_notify.csv
######################
if [ $full_or_daily == "FULL" ]; then
        #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date_notify.csv
    python t_daily_fundamentals_2.py  --disclosure_date_notify_day 15 --force_run # <<<< 1 sec
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --disclosure_date_notify_day 15 --force_run #refresh every run.
fi



######################### percent_mainbz_f
#
#input:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz.csv
#output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_mainbz_percent.csv <<<  ts_code,name,end_date,bz_cnt,perc_cost,perc_profit,perc_sales,bz_cost,bz_item,bz_profit,bz_sales,curr_type
######################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_mainbz_percent.csv
    python t_daily_fundamentals_2.py  --percent_mainbz_f --force_run  #<<<< 4 min
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --percent_mainbz_f
fi



######################### analyze
#input: merged/*.csv
#output:step1: report/step1/rpt_"+end_date+".csv", "ts_code,name,, stopProcess,bonus cnt, garbage cnt, bonusReason, garbageReason,curAssliaM"
#       step2: report/step2/rpt_"+end_date+"2.csv", "ts_code,name,score,garbageReason,score*"
#       step3: report/step3/rpt_"+end_date+"3.csv", "ts_code,name,sos,score,garbageReason,score*"
#       step4: report/step4/multiple_years_score.csv, "ts_code,name,score_over_years,score_avg,number_in_top_30,201806,201803..."
#       step5: report/step5/multiple_years_score.csv. "scoreA,ts_code,name,score_over_years,score_avg,number_in_top_30,201806,201803..."
#       step6: report/step6/multiple_years_score.csv ,  multiple_years_score_selected.csv. "code,name,scoreA,V_C_P,ValuePrice,CurrentPrice"
#       step7: report/step7/verify_fund_increase.csv, code	name	score_over_years	score_avg	number_in_top_30	inc201806
#       step8: report/step8/multiple_years_score.csv ,  code,name,scoreA,V_C_P,ValuePrice,CurrentPrice,ktr_cnt_win,ktr_win_p,ktr_inc_avg,name_stock_basics,score_over_years,score_avg,number_in_top_30,inc201806,201806,
#              report/step8/multiple_years_score_selected.csv,  code,name,scoreA,V_C_P,ValuePrice,CurrentPrice,ktr_cnt_win,ktr_win_p,ktr_inc_avg,name_stock_basics,score_over_years,score_avg,number_in_top_30,inc201806,201806,
#
#########################
#run this at 01/31(Q4), 04/31(Q1), 07/31(Q2), 10/31(Q3). fully and daily are different sets.
# with --big_memory, these two files will be updated. These two files should have been update in above merge_quarterly step.
#      /home/ryan/DATA/result/jaqs/jaqs_all.pickle  << everytime when --big_memory specified.  JAQS is DEAD.
#      /home/ryan/DATA/result/jaqs/ts_all.pickle  << everytime when --big_memory specified.

if [ $full_or_daily == "FULL" ]; then
    #rm -fr /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step[1-8]
    python t_daily_fundamentals_2.py  --analyze  --fully_a --big_memory --force_run  # <<<<  18 hour !!!
fi

if [ $full_or_daily == "DAILY" ]; then
    rm -fr /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step[4-8]
    # 30min. force_run is required.
    #force_run is required because it calculated the dymanic changing data that keep updating. eg. one 20190308, the data is not fixed
    #of 20181231, the daily run keeps calculate rpt_20181231 for every step.
    python t_daily_fundamentals_2.py  --analyze  --daily_a --big_memory --force_run
fi


###################
#input: fund_base_report+"/step1/rpt_"+year_q[ann_date]+".csv"
#
#output:
#       /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/white_horse.csv
#       /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/hen_cow.csv
#       /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/freecashflow_price_ratio.csv
###################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/white_horse.csv
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/hen_cow.csv
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/freecashflow_price_ratio.csv
    python t_daily_fundamentals_2.py  --wh_hencow_fcf --force_run  # <<<< 1 min
fi


if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --wh_hencow_fcf --force_run #refresh every run.
fi




##########################
#input: #"/home/ryan/DATA/DAY_JAQS/*.csv"  <<<
#         /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/*ts_code*_basic.csv.  e.g 600519.SH_basic.csv
#        "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_indicator.csv"
#output: "/home/ryan/DATA/result/pe_pb_rank.csv"
#        "/home/ryan/DATA/result/pe_pb_rank_selected.csv"
##########################
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/pe_pb_rank.csv
    rm -f /home/ryan/DATA/result/pe_pb_rank_selected.csv
    python t_daily_pe_pb_roe_history.py --force_run  # <<< 2min
fi



if [ $full_or_daily == "DAILY" ]; then
    python t_daily_pe_pb_roe_history.py --force_run #refresh every run.
fi




######################################################## Step3 FUND1 ################################################

################################################
#need to update every quarter.
#output: "/home/ryan/DATA/result/fundamental.csv"
################################################
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fundamental.csv
    python t_daily_fundamentals.py --process_hist_data --force_run  #<<< 1 hour 30 min
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "Do Nothing on DAILY, t_daily_fundamentals.py --process_hist_data"
fi


################################################
## input "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv"   "/home/ryan/DATA/result/fundamental.csv" <<< NO LONGER correct.
## input "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv"   "/home/ryan/DATA/result/fundamental.csv"
## input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/merged_all_20181231.csv
## output: /home/ryan/DATA/result/fundamental_peg.csv << false
##
################################################

if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fundamental_peg.csv
    python t_daily_fundamentals.py --calc_peg --force_run #time consuming. 5 hours?  24572 items. modifed check from 2017_1.
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals.py --calc_peg
fi

################################################
## need to update every quarter.
## output: /home/ryan/DATA/result/latest_fundamental_quarter.csv --> Fundamental_Quarter_Report_2018_3.csv
## insert column: result_value_quarter_fundation == esp, pe, pb, npr, roe, totalasset
#@todo: the link is not setup on haha_65 latest_fundamental_quarter.csv <<< done
#@todo: remove duplicate columns (name); code to be string; @column sequence; @drop (filter out) garbage company.
#################################################
#rm -f /home/ryan/DATA/result/Fundamental_Quarter_Report_2018_4.csv ###
#rm -f /home/ryan/DATA/result/latest_fundamental_quarter.csv  ### This is a link.

if [ $full_or_daily == "FULL" ]; then
    python t_daily_fundamentals.py --exam_quarterly --force_run #<<< 3 min
fi


if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals.py --exam_quarterly --force_run #refresh every run.
fi


################################################
#input: dump_csv_q."/home/ryan/DATA/result/latest_fundamental_quarter.csv --> Fundamental_Quarter_Report_2018_2.csv
##output: /home/ryan/DATA/result/latest_fundamental_day.csv --> /home/ryan/DATA/result/today/fundamentals.csv
## insert column : result_value_today ==  fvalues, turnover, amount
##				   result_value_sum == result_value_today + result_value_quarter_fundation
#@todo: mktcap	nmc  keep 0 decimal
#outstanding ?
#totals?
################################################
#rm -f /home/ryan/DATA/result/latest_fundamental_day.csv
#rm -f /home/ryan/DATA/result/today/fundamentals.csv

python t_daily_fundamentals.py --exam_daily  --force_run #refresh every run.  #<<< 20 sec

################################################
##update this year peg
##input: "/home/ryan/DATA/result/fundamental_peg.csv"
##output: "/home/ryan/DATA/result/fundamental_peg_2018_2.csv"
##        "/home/ryan/DATA/result/fundamental_peg_2018_2_selected.csv"
# peg1:
# peg4:
# ps:
# @todo: code need in str, it is 123 for 000123. << should be a liboffice displaying issue
# @todo: name should be clean, left only one.
# @todo: column sequenece.
# @todo: dapan Gu, not applicable to peg
################################################
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fundamental_peg_2019_1.csv
    rm -f /home/ryan/DATA/result/fundamental_peg_2019_1_selected.csv
    python t_daily_fundamentals.py --this_year_quarter  --force_run   #<<< 3 sec
fi


if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals.py --this_year_quarter --force_run #refresh every run.
fi


################################################
#use result of --calc_peg.  insert df_ps columns to csv_d, csv_q
#input: latest_fundamental_peg.csv; dump_csv_d; dump_csv_q; fund_ps_csv
#output:
#t_daily_fundamentals.py: fundmental ps, peg inserted into /home/ryan/DATA/result/latest_fundamental_day.csv
#t_daily_fundamentals.py: fundmental ps, peg inserted into /home/ryan/DATA/result/latest_fundamental_quarter.csv

################################################
python t_daily_fundamentals.py --calc_ps --force_run # <<< 3 sec. Quick so always forcerun

################################################
#input and output: /home/ryan/DATA/result/Fundamental_Quarter_Report_2018_4.csv
#
#insert columns result_value_2, score_sum to csv_q
# result_value_2: ps_perc, peg_1_perc, peg_4_perc
## peg_1 = price_q / egr_1 , egr_1 = 100 * ((eps_this / eps_last_1) - 1)
## peg_4= price_q / egr_4, egr_4 = 100 * ( (eps_this / eps_last_4) -1 )
################################################
if [ $full_or_daily == "FULL" ]; then
    python t_daily_fundamentals.py --calc_fund_2 --force_run  #<<< 50 sec
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals.py --calc_fund_2 --force_run #refresh every run.
fi


################################################
#input: csv_q: /home/ryan/DATA/result/latest_fundamental_quarter.csv, soft link to --> Fundamental_Quarter_Report_2018_2.csv
#output: "/home/ryan/DATA/result/industry_top.csv"
#@todo: calculation:
#score_sum : same as csv_q
################################################
rm -f /home/ryan/DATA/result/industry_top.csv
python t_daily_fundamentals.py --industry_rank_f #<<<  2 sec
#python t_daily_fundamentals.py --industry_rank_f --force_run

################################################
#input: csv_q
#output: "/home/ryan/DATA/result/area_top.csv"
################################################
rm -f /home/ryan/DATA/result/area_top.csv
python t_daily_fundamentals.py --area_rank_f  #<<<  2 sec
#python t_daily_fundamentals.py --area_rank_f --force_run

######################################################## Step4 SUMMARY ################################################

################################################
#input:  /home/ryan/DATA/DAY_Global/AG/
#output:  /home/ryan/DATA/result/key_points_AG_today_selected.csv
################################################
## add --force_run to force run

if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/key_points_AG_today_selected.csv
    python t_daily_get_key_points.py  -x AG  --calc_base --force_run  #  <<< 2 hour, slow (10hours).  run every 7 days. output: /home/ryan/DATA/result/key_points_AG.csv
    python t_daily_get_key_points.py  -x AG  --calc_today  --force_run # <<<< 4 min, fast. output: /home/ryan/DATA/result/key_points_AG_today.csv
    python t_daily_get_key_points.py  -x AG  --today_selection --force_run  #fast. output: /home/ryan/DATA/result/key_points_AG_today_selected.csv
fi

if [ $full_or_daily == "DAILY" ]; then
    #python t_daily_get_key_points.py  -x AG  --calc_base  #slow (10hours).  run every 7 days. output: /home/ryan/DATA/result/key_points_AG.csv
    python t_daily_get_key_points.py  -x AG  --calc_today  #fast. output: /home/ryan/DATA/result/key_points_AG_today.csv
    python t_daily_get_key_points.py  -x AG  --today_selection #fast. output: /home/ryan/DATA/result/key_points_AG_today_selected.csv
fi


##############################
# input:
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/summary_600519.csv
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/detail_600519.csv
#
#
# output:
#  /home/ryan/DATA/result/top_10_holder_summary_full.csv
#  /home/ryan/DATA/result/top_10_holder_details_full.csv
#  /home/ryan/DATA/result/top_10_holder_summary_latest.csv"
#  /home/ryan/DATA/result/top_10_holder_detail_latest.csv"
###############################
python t_top_10_holders.py --analyze



##############################
# indicator
# input: DAY_Global/AG
# output: ~/DATA/result/macd_selection_M.csv
#         ~/DATA/result/macd_selection_W.csv
#         ~/DATA/result/macd_selection_D.csv
#
#         ~/DATA/result/kdj_selection_M.csv
#         ~/DATA/result/kdj_selection_W.csv
#         ~/DATA/result/kdj_selection_D.csv
#
###############################
python t_daily_indicator_kdj_macd.py --indicator KDJ --period M
python t_daily_indicator_kdj_macd.py --indicator KDJ --period W
python t_daily_indicator_kdj_macd.py --indicator KDJ --period D

python t_daily_indicator_kdj_macd.py --indicator MACD --period M
python t_daily_indicator_kdj_macd.py --indicator MACD --period W
python t_daily_indicator_kdj_macd.py --indicator MACD --period D



##############################
# fibonacci indicator
# input: DAY_Global/AG,  DAY_Global/AG/INDEX
# output: ~/DATA/result/fib.csv
# 	  ~/DATA/result/fib_index.csv
###############################
python t_fibonacci.py --verify --begin_date "2018-01-01"  --min_sample=500 
python t_fibonacci.py --verify --begin_date "2018-01-01"  --min_sample=500 --index


##############################
# fibonacci indicator
# input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/market/pro_concept.csv
# output: /home/ryan/DATA/result/concept_top.csv
###############################
python t_daily_fundamentals_2.py  --concept_top

################################################
#run1
#run2
#run3
#run4
#run5
#output: /home/ryan/DATA/result/today/talib_and_pv_no_db_filter_AG.csv
################################################
rm -f /home/ryan/DATA/result/today/talib_and_pv_no_db_filter_AG.csv
python t_daily_pattern_Hit_Price_Volume.py -m 7 -x AG --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit 

#python t_daily_pattern_Hit_Price_Volume.py -0  -m 30 -x AG  #it was 22 ->222 ->30

#python t_daily_pattern_Hit_Price_Volume.py -1 -2 -4  -x AG
#python t_daily_pattern_Hit_Price_Volume.py -1 -2 -4  -m 22  #Only run pv test.

#############
# output: /hdd/DATA/result/report_2019-03-29_AG.txt
#############
python t_summary.py -x AG --action generate_report


#############
# output: /home/ryan/DATA/result/report_2019-03-29_AG_short.csv
#############
python t_summary.py -x AG --action analyze_report

#Possible good candidates:
#  df_support_resist_line_today <<  SHORT term. with fund or fund_2 other fundamental indicators (whitehorse, ROE, etc)
#                                   at lower price, bottom price now?

#  df_low_price_year  <<< SHORT term
#  df_low_vol_year <<< SHORT term
#  df_pv_break  <<< SHORT
#  df_decrease_gap  <<< SHORT
#  df_disclosure_date_notify <<< SHORT


#  df_pe_pb_roe_history <<<  MIDDLE term
#  df_fund <<<  last Q status, reflect recent.   MIDDLE term

#  df_fund_2  <<<  last Y status, reflect long history. LONG term.
#  df_freecashflow_price_ratio  <<<< cheap.  LONG term.
#  df_peg_ps  <<< earn quickly. Price/(EarningsToGrowth) (PEG) Ratio.   LONG term.
#  df_disclosure_date_notify <<< news is going to announce. SHORT term.
#  df_reduced_quarter_year  <<< inner merge of df_fund and df_fund_2. standfor good value company.  LONG term.
#  df_fenghong <<< LONG term, but not as important as df_fund.
#  df_whitehorse  <<< LONG
#  df_freecashflow_price_ratio  <<< LONG
#  df_hen_cow  <<< LONG


# The stratage is :  IN LONG term scope,  FIND stocks cheap in SHORT term. ( SELECT SHORT FROM LONG)
#
#
# Example:  find CHEAP stock, and the stock are in SHORT TERM (S) and LONG TERM (L) df.
#        cat /home/ryan/DATA/result/report_2019-03-08_AG.txt |grep == | grep -vE "0 cheap.*" | grep -v "0s" | grep -v "0l"



#python t_daily_update_order_tracking_stock.py



