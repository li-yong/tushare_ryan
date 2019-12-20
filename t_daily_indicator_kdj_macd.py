# coding: utf-8

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


def _kdj(csv_f, period):

    rtn = {
        "reason":[''],
        "strength":[''],
        "action":[''],
        "code":[''],
        "date":[''],
        "k1":[''],
        "d1":[''],
        "j1":[''],
        "k2": [''],
        "d2": [''],
        "j2": [''],

    }

    if not os.path.exists(csv_f):
        logging.info('file not exist. '+csv_f)
        return(rtn)

    #sys.stdout.write(csv_f+": ")

    df_rtn = pd.DataFrame()

    #csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv' #ryan debug

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

    # print(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #ryan debug
    # print(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    df = df[-1000:] #last 4 years.








    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily = df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    #KDJ, default to 9 days
    period_kdj = 9
    df_period = df_period[-1 * (period_kdj * 3):]

    stock = stockstats.StockDataFrame.retype(df_period)

    df_kdj = stock[['close', 'kdjk', 'kdjd', 'kdjj']]


    if df_kdj.__len__() < 2: #at least two month.
        return(rtn)

    d2 = df_kdj.iloc[-2]
    d1 = df_kdj.iloc[-1]

    this_date = d1.name.strftime("%Y-%m-%d") #'2019-03-31'
    _k1=d1.kdjk
    _d1=d1.kdjd
    _j1=d1.kdjj

    _k2=d2.kdjk
    _d2=d2.kdjd
    _j2=d2.kdjj


    if abs(_k1) >= 90:
        this_reason="excessive buy, k "+str(round(_k1,0))
        this_strength = round(_k1/100, 2)
        this_action = 'SELL'
        print('excessive buy. Should sell. '+csv_f)
    elif abs(_k1) <= 10:
        this_reason = "excessive sell, k "+str(round(_k1,0))
        this_strength = round((1 - _k1/100),2)
        this_action = 'BUY'
        print('excessive sell. Should buy. '+csv_f)



    if (_d2 - _k2)/abs(_d2) >= 0.1 : #d more than 10% of k. Means low price. undervalued
        if _k1 >= _d1:
            this_strength = round((_k1- _d1) / abs(_d1),2)
            this_reason = "K cross over D,  K "+str(_k1)+", D "+str(_d1)+", strength "+str(this_strength)
            this_action = 'BUY'
            print("K cross over D, should buy. "+csv_f)

        elif (_d1 - _k1)/abs(_d1) <= 0.05: # k still less than d, but very close
            t = round((_d1 - _k1) / abs(_d1), 2)
            this_reason = 'K is less than 5% to D, '+str(t)
            this_strength = round((1-t), 2)
            this_action = 'BUY_EARLY'
            print("K is less than 5% to D, early stage, should buy little "+csv_f)


    if (_k2- _d2)/abs(_k2) >= 0.1 : #k more than 10% of d. Means high price. overvalued
        if (_d1 - _k1)/abs(_d1) >= 0.05: #k less than d more than 5%
            t = round((_d1 - _k1) / abs(_d1), 2)
            this_reason = 'K cross down D, K '+str(_k1)+", D "+str(_d1)+", Percent "+str(t)
            this_strength = round(t, 2)
            this_action = 'SELL'
            print("K cross down D, should sell. " + csv_f)

        #elif (d1.kdjk - d1.kdjd)/ d1.kdjk <= 0.05: #k more than d, but very close
        #    print("K is less than 5% to D, early stage, should sell little")
        #return

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

def kdj(period):
    output_csv = "/home/ryan/DATA/result/kdj_selection_"+period+".csv" #head: code, name, date, action(b/s), reason, strength.
    # Term: Middle(because based on Monthly data,
    # Type: Cheap

    df_rtn = pd.DataFrame()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)

    #stock_list = stock_list.head(100) #ryan debug

    cnt = stock_list.__len__()
    i = 0
    for c in stock_list['code']:
        i+=1
        csv_f = '/home/ryan/DATA/DAY_Global/AG/'+c+".csv"
        logging.info(str(i) + " of " + str(cnt)+" "+c)

        r = _kdj(csv_f = csv_f, period = period)

        if r['action'] != ['']:
            df= pd.DataFrame(data=r)
            df_rtn = df_rtn.append(df)

    if df_rtn.empty:
        print("KDJ no qualified  stock found.")
    else:
        df_rtn = pd.merge(df_rtn, stock_list, how='inner', on='code')
        df_rtn = df_rtn[['code','name','date','action','strength','reason','k1','d1','j1','k2','d2','j2']]
        df_rtn.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info("kdj selection saved to "+output_csv+" . len "+str(df_rtn.__len__()))


def _macd(csv_f, period):

    rtn = {
        "reason":[''],
        "strength":[''],
        "action":[''],
        "code":[''],
        "date":[''],
        "k1":[''],
        "d1":[''],
        "j1":[''],
        "k2": [''],
        "d2": [''],
        "j2": [''],

    }

    if not os.path.exists(csv_f):
        logging.info('file not exist. '+csv_f)
        return(rtn)

    #csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv' #ryan debug

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

    # print(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #debug
    # print(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    df = df[-1000:] #last 4 years.

    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily = df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    df_period = df_period[-100:]
    #print(tabulate.tabulate(df_period[-10:], headers='keys', tablefmt='psql'))





    '''
    MACD: (12-day EMA - 26-day EMA)
    Signal Line: 9-day EMA of MACD
    MACD Histogram: 2*(MACD - Signal Line)
    '''
    stock = stockstats.StockDataFrame.retype(df_period)
    df_macd = stock[['macd', 'macds', 'macdh']] #macds: # MACD signal line, macdh: # MACD histogram
    df_macd.rename(columns={"macd": "DIF_main", "macds": "DEA_signal", "macdh": "MACD_histogram", }, inplace=True)
    df_macd = df_macd.round({'DIF_main':1,'DEA_signal':1,'MACD_histogram':1})

    #print(tabulate.tabulate(df_macd[-2:], headers='keys', tablefmt='psql'))

    if df_macd.__len__() < 2: #at least two records.
        return(rtn)

    d2 = df_macd.iloc[-2]
    d1 = df_macd.iloc[-1]

    this_date = d1.name.strftime("%Y-%m-%d") #'2019-03-31'
    dif1=d1.DIF_main
    dea1=d1.DEA_signal
    macd1=d1.MACD_histogram

    dif2=d2.DIF_main
    dea2=d2.DEA_signal
    macd2=d2.MACD_histogram

    if dif2 < dea2 and dif1 > dea1:
        this_reason = "dif up over sig. "
        this_strength = 1
        this_action = 'BUY_CHK'
        print(this_reason + csv_f)

    if dif2 > dea2 and dif1 < dea1:
        this_reason = "dif down cross sig. "
        this_strength = 1
        this_action = 'SELL_CHK'
        print(this_reason + csv_f)

    if dif1 > 50 and dea1 > 50 :
        if (macd1 < macd2) and macd1 > 0 and macd1 <10:
            this_reason="price high, macd down near 0. "
            this_strength = 1
            this_action = 'SELL_EARLY'
            print(this_reason+csv_f)
        elif dif2 > 0 and dea2 > 0 and macd1 <0 and macd2 > 0:
            this_reason="price high, macd down over 0. "
            this_strength = 1
            this_action = 'SELL_MUST'
            print(this_reason+csv_f)


    if dif1 < -50 and dea1 < -50 :
        if macd2 < 0 and (macd1 > macd2) and macd1 < 0 and macd1 > -10 :
            this_reason="price low, macd up near 0. "
            this_strength = 1
            this_action = 'BUY_EARLY'
            print(this_reason+csv_f)
        elif dif2 < -50 and dea2 < -50 and macd1 > 0 and macd2 < 0:
            this_reason="price low, macd up over 0. "
            this_strength = 1
            this_action = 'BUY_MUST'
            print(this_reason+csv_f)


    rtn = {
        "reason":[this_reason],
        "strength":[this_strength],
        "action":[this_action],
        "code":[this_code],
        "date":[this_date],

        "dif1":[round(dif1,1)],
        "dea1":[round(dea1,1)],
        "macd1":[round(macd1,1)],

        "dif2": [round(dif2,1)],
        "dea2": [round(dea2,1)],
        "macd2": [round(macd2,1)],

    }


    return(rtn)

def macd(period):
    output_csv = "/home/ryan/DATA/result/macd_selection_"+period+".csv" #head: code, name, date, action(b/s), reason, strength.
    # Term: Middle(because based on Monthly data,
    # Type: Cheap

    df_rtn = pd.DataFrame()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)

    #stock_list = stock_list.head(10) #debug

    cnt = stock_list.__len__()
    i = 0
    for c in stock_list['code']:
        i+=1
        csv_f = '/home/ryan/DATA/DAY_Global/AG/'+c+".csv"
        logging.info(str(i) + " of " + str(cnt)+" "+c)

        r = _macd(csv_f = csv_f, period=period)

        if r['action'] != ['']:
            df= pd.DataFrame(data=r)
            df_rtn = df_rtn.append(df)

    if df_rtn.empty:
        print("MACD no qualified stock found.")
    else:
        df_rtn = pd.merge(df_rtn, stock_list, how='inner', on='code')
        df_rtn = df_rtn[['code','name','date','action','reason','strength']]
        df_rtn.to_csv(output_csv, encoding='UTF-8', index=False)


        logging.info("MACD selection saved to "+output_csv+" . len "+str(df_rtn.__len__()))


def calculate(indicator, period):
    if indicator == 'KDJ':
        kdj(period=period)

    if indicator == 'MACD':
        macd(period=period)

def analyze(indicator):
    dir = "/home/ryan/DATA/result"
    if indicator == 'MACD':
        input_csv_m=dir+"/macd_selection_M.csv"
        input_csv_w=dir+"/macd_selection_W.csv"
        input_csv_d=dir+"/macd_selection_D.csv"

    if indicator == 'KDJ':
        input_csv_m=dir+"/kdj_selection_M.csv"
        input_csv_w=dir+"/kdj_selection_W.csv"
        input_csv_d=dir+"/kdj_selection_D.csv"

    df_m = pd.read_csv(input_csv_m)
    df_m.rename(columns={'action':'action_m','date':'date_m','reason':'reason_m','strength':'strength_m',}, inplace=True)

    df_w = pd.read_csv(input_csv_w)
    df_w.rename(columns={'action':'action_w','date':'date_w','reason':'reason_w','strength':'strength_w',}, inplace=True)

    df_d = pd.read_csv(input_csv_d)
    df_d.rename(columns={'action':'action_d','date':'date_d','reason':'reason_d','strength':'strength_d',}, inplace=True)

    df_merge = pd.merge(df_m, df_w, on=['code','name'], how='outer')
    df_merge = pd.merge(df_merge, df_d, on=['code','name'], how='outer')

    cols = ['code', 'name','date_m', 'date_w', 'date_d',
           'action_m','action_w','action_d',  'reason_m', 'reason_w', 'reason_d',
           'strength_m','strength_w',  'strength_d']

    df_merge = df_merge[cols]
    print(tabulate.tabulate(df_merge[-20:], headers='keys', tablefmt='psql'))

    # df_merge[df_merge.action_d.str.contains(r'BUY.*', na=True)]




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

    if indicator == None:
        print("missing indicator [MACD|KDJ]")

    if analyze_f:
        analyze(indicator=indicator)
    elif not period == None:
        calculate(indicator, period)




### MAIN ####
if __name__ == '__main__':
    main()
