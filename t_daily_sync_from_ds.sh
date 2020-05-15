#!/usr/bin/env bash


######################################
#
# haha_brain ---> haha_data_source
#
######################################
#rsync -az   /home/ryan/tushare_ryan/  haha_data_source:/home/ryan/tushare_ryan/
ssh haha_data_source "cd ~/tushare_ryan;  git pull"
rsync -az /home/ryan/DATA/result/*.csv haha_data_source:/home/ryan/DATA/result/
rsync -az /home/ryan/DATA/result/*.txt haha_data_source:/home/ryan/DATA/result/
rsync -az /home/ryan/DATA/result/today haha_data_source:/home/ryan/DATA/result/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/*.csv haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/  haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/



######################################
#
# haha_data_source ---> haha_brain
#
######################################

rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/AG/  /home/ryan/DATA/DAY_Global/AG/
rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/AG_INDEX/  /home/ryan/DATA/DAY_Global/AG_INDEX/
#rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/US_INDEX/  /home/ryan/DATA/DAY_Global/US_INDEX/ #no longer there
rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/HK/  /home/ryan/DATA/DAY_Global/HK/
rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/AG_MoneyFlow/  /home/ryan/DATA/DAY_Global/AG_MoneyFlow/


rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/stooq/US/  /home/ryan/DATA/DAY_Global/stooq/US/
rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/stooq/US_INDEX/  /home/ryan/DATA/DAY_Global/stooq/US_INDEX/

#US/HK has too long list to fit in haha_data_source (1GB memory)
rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/US/  /home/ryan/DATA/DAY_Global/US/
rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/HK/  /home/ryan/DATA/DAY_Global/HK/
rsync -az   haha_data_source:/home/ryan/DATA/DAY_Global/US/  /home/ryan/DATA/DAY_Global/US/ 
#rsync -az   haha_power:/home/ryan/DATA/DAY_Global/CH/  /home/ryan/DATA/DAY_Global/CH/
#rsync -az   haha_power/ryan/DATA/DAY_Global/KG/  /home/ryan/DATA/DAY_Global/KG/

#rsync -az   haha_power/ryan/DATA/DAY_Global/MG/  /home/ryan/DATA/DAY_Global/MG/


rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/



rsync -az haha_data_source:/home/ryan/DATA/pickle/daily_update_source/  /home/ryan/DATA/pickle/daily_update_source/
rsync -az haha_data_source:/home/ryan/DATA/announcement/  /home/ryan/DATA/announcement/
rsync -az haha_data_source:/home/ryan/DATA/DAY_No_Adj/  /home/ryan/DATA/DAY_No_Adj/
rsync -az haha_data_source:/home/ryan/DATA/DAY_JAQS/  /home/ryan/DATA/DAY_JAQS/

rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/  /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/


rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/


#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -E "201[7-9]"`;
#do
#    echo "sync file to normal directory /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done



#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -v 201`;
#do
#    echo "sync file to symbol link /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done



#rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181232/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181231/
#rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/
#rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/
#rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20191231/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20191231/


#rsync -az haha_data_source:/home/ryan/DATA/pickle/DOW_SP/  /home/ryan/DATA/pickle/DOW_SP/
rsync -az haha_data_source:/home/ryan/DATA/pickle/Forbes/  /home/ryan/DATA/pickle/Forbes/
rsync -az haha_data_source:/home/ryan/DATA/pickle/INDEX/  /home/ryan/DATA/pickle/INDEX/
rsync -az haha_data_source:/home/ryan/DATA/pickle/INDEX_US_HK/  /home/ryan/DATA/pickle/INDEX_US_HK/

rsync -az haha_data_source:/home/ryan/DATA/pickle/gem.csv  /home/ryan/DATA/pickle/gem.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/instrument_A.csv  /home/ryan/DATA/pickle/instrument_A.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/instrument.csv  /home/ryan/DATA/pickle/instrument.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/security.csv  /home/ryan/DATA/pickle/security.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/trading_day_2019.csv  /home/ryan/DATA/pickle/trading_day_2019.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/hs300.csv  /home/ryan/DATA/pickle/hs300.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/market.csv  /home/ryan/DATA/pickle/market.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/sme.csv  /home/ryan/DATA/pickle/sme.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/sz50.csv  /home/ryan/DATA/pickle/sz50.csv
rsync -az haha_data_source:/home/ryan/DATA/pickle/ZZ500.csv  /home/ryan/DATA/pickle/ZZ500.csv


rsync -az haha_data_source:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/

