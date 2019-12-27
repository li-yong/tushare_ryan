#!/usr/bin/env bash


full_or_daily=$1

if [ $full_or_daily == "FULL" ]; then
    echo "$0: FULL RUN"
elif [ $full_or_daily == "DAILY" ]; then
    echo "$0: DAILY RUN"
else
    echo "Have to specify FULL or DAILY. Quiting";
    exit
fi




######################################
#
# commit and sync git
#
######################################

cd ~/tushare_ryan; git commit -a -m'1'; git push;


ssh haha_power "cd ~/tushare_ryan; git pull; "


######################################
#
# haha_power ---> haha_brain
#
######################################

#bash -x t_backtest_sync_65_result_to_local.sh

rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged.dev    haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev   haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source.dev   haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2

######################################
#
# haha_brain ---> haha_power
#
######################################
rsync -az /home/ryan/DATA/DAY_Global/AG/  haha_power:/home/ryan/DATA/DAY_Global/AG/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/

if [ $full_or_daily == "FULL" ]; then
    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/
fi

if [ $full_or_daily == "DAILY" ]; then
    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/2019/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/2019/
fi



rsync -az /home/ryan/DATA/pickle/daily_update_source/  haha_power:/home/ryan/DATA/pickle/daily_update_source/
rsync -az /home/ryan/DATA/pickle/*.csv  haha_power:/home/ryan/DATA/pickle/

rsync -az /home/ryan/DATA/announcement/  haha_power:/home/ryan/DATA/announcement/
rsync -az /home/ryan/DATA/DAY_No_Adj/  haha_power:/home/ryan/DATA/DAY_No_Adj/
rsync -az /home/ryan/DATA/DAY_JAQS/  haha_power:/home/ryan/DATA/DAY_JAQS/


rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/


rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/  haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/

#rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/

for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep 201 | grep -E 201[7-9]`;
do
    echo "sync file to normal directory /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
done


#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep 201 | grep -Ev 201[7-9]`;
#do
#    echo "sync file to normal directory /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done



#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -v 201`;
#do
#    echo "sync file to symbol link /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done

rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181231/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181231/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20191231/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20191231/



if [ $full_or_daily == "FULL" ]; then
    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/ haha_power:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/
fi



###################
# Refesh nfs cache on haha_65
# ryan@haha_65 are configured as sudoer without password. https://phpraxis.wordpress.com/2016/09/27/enable-sudo-without-password-in-ubuntudebian/
#At the end of the /etc/sudoers file add this line:
#username     ALL=(ALL) NOPASSWD:ALL
# ####################


ssh haha_power "nohup bash -x ~/tushare_ryan/t_daily_run_2_exam.sh $full_or_daily > ~/no_hup_run_exam_ag.log  2>&1  &"
echo "nohup bash -x ~/tushare_ryan/t_daily_run_2_exam.sh $full_or_daily > ~/no_hup_run_exam_ag.log  2>&1  &"
echo "Daily Stock exam(finding B/S points) is running on haha_65, output to ~/no_hup_run_2_exam_ag.log, db tbl order_tracking_stock will be update on haha65."

