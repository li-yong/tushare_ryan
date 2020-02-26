
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np

import pandas
import math
import re
from scipy import stats
import finlib
import datetime
import traceback
import sys
import tushare.util.conns as ts_cs
import finlib

from sklearn.cluster import KMeans
from calendar import monthrange

import stockstats
import tabulate
import os
import logging

from optparse import OptionParser

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)


# past < t3 < t2 < t1 < now. dx is the price of tx.
def get_trend(d1,d2,d3):
    if (d3 == d2):
        if d2 == d1:
            trend = 'none'
        elif d2 > d1:
            trend = 'down'
        elif d2 < d1:
            trend = 'up'

    elif d3 > d2:
        if (d2 >= d1):
            trend="down"
        elif d2 < d1:
            trend="down_up"
    elif d3 < d2:
        if (d2 <= d1):
            trend="up"
        elif d2 > d1:
            trend="up_down"

    return(trend)


def _w_shape(csv_f, period):
    csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv' #ryan debug
    period = 'D'

    rtn = {
        "reason":[''],
        "strength":[''],
        "action":[''],
        "code":[''],
        "date":[''],
        "p_cur":[''],
        "p_w_head":[''],
    }

    if not os.path.exists(csv_f):
        logging.info('file not exist. '+csv_f)
        return(rtn)

    sys.stdout.write(csv_f+": ")

    df_rtn = pd.DataFrame()

    df = pd.read_csv(csv_f, converters={'code': str}, header=None, skiprows=1,
                     names=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'tnv'])

    if df.__len__() < 100:
        logging.info('file less than 100 records. '+csv_f)
        return(rtn)

    this_code = df.iloc[0]['code'] #'SH603999'
    this_date='' #monthly period, end date of the month.
    this_reason=''
    this_strength=''
    this_action=''

    # logging.info(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #ryan debug
    # logging.info(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    df = df[-100:] #last 3 months.


    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    #df_sorted = df.sort_values(by='close')

    p = df.iloc[-1].close
    stage = "null"


    df = df.reset_index().drop('index', axis=1)
    stage == 'null'

    for i in reversed(range(df.__len__())):
        if i <= 3:
            return

        r1=df.loc[i]
        r2=df.loc[i-1]
        r3=df.loc[i-2]

        trend = get_trend(r3.close, r2.close, r1.close)

        logging.info(trend+" "+str(r1.date)+str(r1.close)+ ". v_point:"+str(r2.date)+str(r2.close)+" "+str(r3.date)+str(r3.close))

        if trend == 'up':
            if stage == 'null':
                p0 = r2.close
                logging.info("init s0"+", middle point:"+str(r2.date)+" "+ str( r2.close))
                stage = "s0"
                continue

            if stage in ["s0","s2","s4"]:
                logging.info("moving up on "+stage)
                continue

        if trend == 'down':
            if stage == 'null':
                logging.info("not match")
                break

            if stage in ["s1","s3"]:
                logging.info("moving down on "+stage)
                continue


        if trend == 'up_down':
            if stage == 'null':
                logging.info("not match")
                break

            if stage in ["s0"]:
                logging.info("spot p1 in "+stage+", middle point:"+str(r2.date)+" "+ str( r2.close))
                p1 = r2.close
                stage = "s1"
                continue


            if stage in ["s1"]:
                logging.info("spot p2 in "+stage+", middle point:"+str(r2.date)+" "+ str(r2.close))
                p2 = r2.close
                stage = "s2"
                continue

            if stage in ["s2"]:
                logging.info("spot p3 in "+stage+", middle point:"+str(r2.date)+" "+ str( r2.close))
                p3 = r2.close
                stage = "s3"
                continue

            if stage in ["s3"]:
                logging.info("spot p4 in "+stage+", middle point:"+str(r2.date)+" "+ str( r2.close))
                p4 = r2.close
                stage = "s4"
                continue


        if trend == 'down_up':
            if stage == 'null':
                logging.info("not match")
                break

            if stage in ["s0"]:
                logging.info("spot p1 in "+stage+", middle point:"+str(r2.date)+" "+ str( r2.close))
                stage = "s1"
                p1 = r2.close
                continue

            if stage in ["s2"]:
                logging.info("spot p3 in "+stage+", middle point:"+str(r2.date)+" "+ str( r2.close))
                p3 = r2.close
                stage = "s3"
                continue



    rtn = {
        "reason":[this_reason],
        "strength":[this_strength],
        "action":[this_action],
        "code":[this_code],
        "date":[this_date],
        "k1":[round(_k1,1)],
        "d1":[round(_d1,1)],
        "j1":[round(_j1,1)],

        "k2": [round(_k2,1)],
        "d2": [round(_d2,1)],
        "j2": [round(_j2,1)],

    }

    return(rtn)



def main():
    logging.info("\n")
    logging.info("SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()



    parser.add_option("--indicator",  type="str",
                      dest="indicator_f", default=None,
                      help="indicator, one of [KDJ|MACD]")


    parser.add_option("--period",  type="str",
                      dest="period_f", default=None,
                      help="period, one of [M|W|D]")

    parser.add_option("-a", "--analyze", action="store_true",
                      dest="analyze_f", default=False,
                      help="analyze based on [MACD|KDJ] M|W|D result.")

    (options, args) = parser.parse_args()

    indicator = options.indicator_f
    period = options.period_f
    analyze_f = options.analyze_f

    _w_shape(csv_f="",period='D')



### MAIN ####
if __name__ == '__main__':
    main()
