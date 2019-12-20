#!/usr/bin/env bash

cd ~/tushare_ryan; git commit -a -m'1'; git push;
ssh haha_65 "cd ~/tushare_ryan; git pull"
ssh haha_66 "cd ~/tushare_ryan; git pull"

######################################
#
# haha_brain ---> haha_66
#
######################################
rsync -avz   /home/ryan/DATA/DAY_Global/CH/  haha_66:/hdd/DATA/DAY_Global/CH/
rsync -avz   /home/ryan/DATA/DAY_Global/KG/  haha_66:/hdd/DATA/DAY_Global/KG/
rsync -avz   /home/ryan/DATA/DAY_Global/KH/  haha_66:/hdd/DATA/DAY_Global/KH/
rsync -avz   /home/ryan/DATA/DAY_Global/KH.product/  haha_66:/hdd/DATA/DAY_Global/KH.product/
rsync -avz   /home/ryan/DATA/DAY_Global/MG/  haha_66:/hdd/DATA/DAY_Global/MG/
rsync -avz   /home/ryan/DATA/DAY_Global/US/  haha_66:/hdd/DATA/DAY_Global/US/


#this is remote run.  US_HK is one day delay off the GMT +8.00. -x will trigger yesterday's date as --exam_date.
ssh haha_65 "nohup bash -x ~/tushare_ryan/t_daily_run_2_exam_ushk.sh  >~/no_hup_run_exam_ushk.log  2>&1  &"
echo "Done! Daily HK US Stock exam(finding B/S points) is running on haha_65, output to ~/no_hup_run_exam_ushk.log"


