# coding: utf-8

import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import os
import time
import numpy as np
#import matplotlib.pyplot as plt
import pandas
import re

from datetime import datetime, timedelta
import finlib

from optparse import OptionParser
import sys


import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S',level=logging.DEBUG)


def update_holc(todayS_l, base_dir, pickle_only, add_miss):
    dump= "/home/ryan/DATA/pickle/daily_update_source/"+todayS_l+"ts_ud.pickle"
    todayS_l = datetime.strptime(todayS_l, '%Y-%m-%d').strftime('%Y-%m-%d') #ensure input todayS_l is in format yyyy-mm-dd
    todayS_s = datetime.strptime(todayS_l, '%Y-%m-%d').strftime('%Y%m%d')

    if not finlib.Finlib().is_a_trading_day_ag(dateS=todayS_s):
        logging.error("date is not a trading day "+todayS_s)
        return

    today_all = pd.DataFrame()

    if add_miss and  os.path.isfile(dump):
        logging.error("usage error, file dump exists, removing --add_miss to update csv(s). " + dump)
        exit(0)

    if not os.path.isfile(dump):
        if add_miss:
            today_all = ts.get_day_all(todayS_s)
        else:
            today_all = ts.get_today_all()
            today_all.to_pickle(dump)
            logging.info("Pickle saved to "+dump)
    else:
        logging.info("read pickle from "+dump)
        today_all=pandas.read_pickle(dump)

    #ryan add 20180511. update code_map_csv
    if os.path.isfile("/home/ryan/DATA/pickle/instrument_A.csv"):
        os.remove("/home/ryan/DATA/pickle/instrument_A.csv")
    finlib.Finlib().get_A_stock_instrment(today_df=today_all)



    if pickle_only:
        logging.info("Save pickle only, exit")
        exit(0)


    a=today_all['code']
    #today_all.to_csv("~/get_day_all.12.08.csv")


    security_csv = "/home/ryan/DATA/pickle/security.csv"
    code_name_map = today_all[['code','name']]  #only select code and name column

    code_name_map = finlib.Finlib().add_market_to_code(df=code_name_map)  #600000 --> SH600000

        #code_name_map.set_value(index, code_name_map.columns.get_loc('code'), code_S)
        #code_name_map.set_value(index, code_name_map.columns.get_loc('code'), code_S)
        #code_name_map.iloc[index, code_name_map.columns.get_loc('code')]=code_S



    code_name_map.to_csv(security_csv, encoding='UTF-8')
    logging.info("saved code<->name map to "+security_csv)

    for i in range(0, a.__len__()):
        code= a.iloc[i]
        sys.stdout.write(code+" ")

        o = today_all.iloc[i]['open']
        h = today_all.iloc[i]['high']
        l = today_all.iloc[i]['low']

        volume = today_all.iloc[i]['volume']

        if add_miss:
            volume = volume*100 #


        if add_miss:
            c = today_all.iloc[i]['price']
            settlement = 0
            turnoverratio = 0
            amount = 0
        else:
            settlement = today_all.iloc[i]['settlement']
            c = today_all.iloc[i]['trade']
            turnoverratio = round(today_all.iloc[i]['turnoverratio']/100,8)
            amount = today_all.iloc[i]['amount']

        if re.match('^6',code):
            csv_f= base_dir+'/SH'+code+'.csv'
            code_S="SH"+code
        elif re.match('^[0|3]', code):
            csv_f = base_dir + '/SZ' + code + '.csv'
            code_S = "SZ" + code
        elif re.match('^[9]', code): #B Gu
            logging.info(("ignore B GU "+code))
            continue
        else:
            logging.info(("Fatal: UNKNOWN CODE "+code))
            continue
            #exit(1)



       # if re.match('^00', code):
       #     csv_f = base_dir + '/SZ' + code + '.csv'
       #     code_S = "SZ" + code
       #     #print 1

        csv_append_s=code_S+","+todayS_l+","+str(o)+","+str(h)+","+str(l)+","+str(c)+"," \
                     +str(volume)+","+str(amount)+","+str(turnoverratio) +"\n"
                     #+ "," + str(turnoverratio)+ "," + "1\n"


        if os.path.isfile(csv_f):

            df_tmp = pd.read_csv(csv_f, converters={'code': str}, skiprows=1, header=None,
                                 names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt','tnv'])

            if df_tmp.__len__() == 0:
                logging.info("empty file "+csv_f)
                with open(csv_f, "a") as fh:
                    fh.write(csv_append_s)
                fh.close()
                continue

            last_row = df_tmp[-1:]

            last_date = last_row['date'].values[0]
            last_date = datetime.strptime(last_date, '%Y-%m-%d').strftime('%Y%m%d')
            next_date = datetime.strptime(last_date, '%Y%m%d') + timedelta(1)
            a_week_before_date = datetime.strptime(todayS_s, '%Y%m%d') - timedelta(7)

            # if next_date > datetime.datetime.today():
            #if next_date.strftime('%Y-%m-%d') > todayS:
            if next_date.strftime('%Y%m%d') > todayS_s:
                logging.info("file already updated, not fetching again. " + csv_f + ". updated to " + last_date)
                continue

            #file exist, append.
            with open(csv_f,"a") as fh:
                fh.write(csv_append_s)
            fh.close()
        else:
            #should not be here unless a new stock added today
            logging.info(("WARN: STOCK FILE NOT EXIST? NEW STOCK?"+ csv_f))
            #time.sleep(2)
            fh=open(csv_f, "w")
            fh.write(csv_append_s)
            fh.close()



def main():
    parser = OptionParser()

    parser.add_option("-b", "--base_dir", dest="base_dir", default='/home/ryan/DATA/DAY_Global/AG', type="str",
                      help="base_dir, default /home/ryan/DATA/DAY_Global/AG")

    parser.add_option("-a", "--add_miss", action="store_true",
                      dest="add_miss", default=False,
                      help="adding miss data. Use with -e/--exam_date to specify date")

    parser.add_option("-p", "--pickle_only", action="store_true",
                      dest="pickle_only", default=False,
                      help="get today data, save to pickel, then exit")

    parser.add_option("-e", "--exam_date", dest="exam_date",
                      help="exam_date, YYYY-MM--DD, no default value, missing will calc the nearest trading day, most time is today")

    (options, args) = parser.parse_args()

    base_dir = options.base_dir
    add_miss = options.add_miss
    pickle_only = options.pickle_only
    exam_date = options.exam_date

    logging.info("base_dir: " + str(base_dir))
    logging.info("pickle_only: " + str(pickle_only))
    logging.info("exam_date: " + str(exam_date))


    dump=''

    if exam_date is None:
        exam_date = finlib.Finlib().get_last_trading_day()
        exam_date = datetime.strptime(exam_date, '%Y%m%d').strftime('%Y-%m-%d')
        logging.info("exam_date reset to: " + exam_date)
        dump = "/home/ryan/DATA/pickle/daily_update_source/" + exam_date + "ts_ud.pickle"
    elif ((not add_miss) and (not os.path.isfile(dump))):
        logging.error(
            "expecting --add_miss.  pickle file " + dump + " does not exist, at this situation --exam_date without --add_miss fetchs wrong data from tushare for date which is the past.")
        exit(0)









    # if debug:
    #    print "debug is boolean True"



    # This script Run every day after marketing closing.
    # It update the csv files in base_dir
    # base_dir='/home/ryan/DATA/DAY'

    # debug, use when missing one day
    # add_miss=False
    # add_miss=True
    #  for i in {8..0}; do X=`date -d "-$i day" '+%Y-%m-%d'`; python t_daily_update_csv_from_tushare_.py -a -e $X;  done
    if add_miss:
         todayS_l = exam_date
    else:
        # todayS_l = datetime.today().strftime('%Y-%m-%d')
        todayS_l = exam_date

    logging.info(("Update Data of " + todayS_l))


    update_holc(todayS_l, base_dir, pickle_only, add_miss)

    logging.info("Script Completed.")
    os._exit(0)

### MAIN ####
if __name__ == '__main__':
    main()
