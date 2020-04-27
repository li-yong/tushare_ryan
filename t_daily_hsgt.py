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

from optparse import OptionParser
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S',level=logging.DEBUG)


import os
import tabulate


global debug_global
global force_run_global
global myToken




def set_global(debug=False,force_run=False):
    global debug_global
    global force_run_global
    global myToken

    ### Global Variables ####
    myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'


    debug_global=False
    force_run_global=False

    if force_run:
        force_run_global=True

    if debug:
        debug_global=True


def fetch_hsgt_top_10():
    # 获取沪股通、深股通每日前十大成交详细数据
    input_output_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/hsgt_top10.csv"

    ts.set_token(myToken)
    pro = ts.pro_api()

    if os.path.exists(input_output_csv):
        df = pd.read_csv(input_output_csv, converters={'trade_date': str})
    else:
        df = pd.DataFrame(columns=['trade_date'])

    fetching_days = 30

    for i in range(fetching_days, -1, -1):  # i from 30, 29, 28... 0

        trade_date = datetime.datetime.today() - datetime.timedelta(i)
        trade_date_s = (datetime.datetime.today() - datetime.timedelta(i)).strftime('%Y%m%d')

        if trade_date.isoweekday() in [6, 7] and (not force_run_global) :
            print("skip, weekend " + trade_date_s)
            continue

        if (not df[df['trade_date'] == trade_date_s].empty) and (not force_run_global):
            print("skip, already have records on " + trade_date_s)
            continue

        sys.stdout.write("fetching hsgt_top 10 " + trade_date_s)

        df_h = pro.hsgt_top10(trade_date=trade_date_s, market_type='1')
        sys.stdout.write(", get len " + str(df_h.__len__()) + "\n")
        df = df.append(df_h)

    cols = ['trade_date', 'ts_code', 'name', 'close', 'net_amount', 'amount', 'buy', 'change', 'market_type', 'rank',
            'sell']

    df = df[cols]
    df = df.sort_values(['trade_date', 'net_amount', 'rank'], ascending=[True, False, True], inplace=False)

    df.to_csv(input_output_csv, encoding='UTF-8', index=False)
    print("hsgt_top_10 saved to " + input_output_csv)


def analyze_hsgt_top_10():
    input_csv =  "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/hsgt_top10.csv"
    output_csv =  "/home/ryan/DATA/result/hsgt_top_10_selected.csv"

    if os.path.exists(input_csv):
        df = pd.read_csv(input_csv, converters={'trade_date': str, 'ts_code':str})
        #df = pd.read_csv(input_csv)
    else:
        logging.info("abort, no such file "+input_csv)
        return()

    #recent 10 days most source in money stocks

    period_days = 10  #two weeks
    #period_days = 5   #two weeks
    period_days = 3   #two weeks

    df = df.tail(n = period_days * 10 ) #assume 10 records per day
    #print(df.tail(40))
    #print(tabulate.tabulate(df, headers='keys', tablefmt='psql'))

    df = df.groupby(['name','ts_code'])['net_amount'].sum().sort_values().reset_index()
    df = df.sort_values(by=['net_amount'], ascending=[False])
    df = df[df['net_amount'] > 0 ].reset_index().drop('index', axis=1)
    df = df.rename(columns={'ts_code': 'code'}, inplace=False)
    df = finlib.Finlib().add_market_to_code(df=df)

    df.to_csv(output_csv, encoding='UTF-8', index=False)
    print(df)
    print("Based on recent "+str(period_days) +" days selected hsgt_top_10 was saved to " + output_csv)


def fetch_moneyflow_dd(fetch_whole = False, fetch_today = True):
    ts.set_token(myToken)
    pro = ts.pro_api()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # SH600519

    i = 0
    csv_dir = "/home/ryan/DATA/DAY_Global/AG_MoneyFlow"
    if not os.path.isdir(csv_dir):
        os.mkdir(csv_dir)

    if fetch_today:
        df_today = pro.moneyflow(trade_date=finlib.Finlib().get_last_trading_day())
        df_today = finlib.Finlib().ts_code_to_code(df_today)

    for index, row in stock_list.iterrows():
        i += 1
        print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
        name, code = row['name'], row['code']
        ts_code =  finlib.Finlib().add_market_to_code(pd.DataFrame([code], columns=['code']), dot_f=True, tspro_format=True).iloc[0]['code']

        csv_f = csv_dir + "/" + code + ".csv"
        if fetch_today:
            df_tmp = df_today[df_today['code']==code]
            
            if os.path.exists(csv_f):
                df_old = pd.read_csv(csv_f)
                df_new = pd.concat([df_old, df_tmp],sort=False).drop_duplicates().reset_index().drop('index', axis=1)
            else:
                df_new = df_tmp
            df_new.to_csv(csv_f,  encoding='UTF-8', index=False)

        elif fetch_whole:
            #SH600519 --> 600519.SH
            df = pro.moneyflow(ts_code=ts_code)
            #df = pro.moneyflow(ts_code=code, start_date = '20200101', end_date = trade_day)
            #ts_code = '002149.SZ', start_date = '20190115', end_date = '20190315
            df = finlib.Finlib().ts_code_to_code(df)
            df = df.reindex(index=df.index[::-1])
            df.to_csv(csv_f,  encoding='UTF-8', index=False)
            print("len "+str(df.__len__())+" "+csv_f)

    #df_4 = pro.moneyflow(trade_date=trade_day)
    #df_4 = df_4.sort_values('net_mf_amount', ascending=False, inplace=False)
    #print(tabulate.tabulate(df_4, headers='keys', tablefmt='psql'))


### MAIN ####
if __name__ == '__main__':


    logging.info("\n")
    logging.info("SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()



    parser.add_option("-u", "--debug", action="store_true",
                      dest="debug_f", default=False,
                      help="debug mode, using merge.dev, report.dev folder")

    parser.add_option("--force_run", action="store_true",
                      dest="force_run_f", default=False,
                      help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("-f", "--fetch_data_all", action="store_true",
                      dest="fetch_all_f", default=False,
                      help="fetch all hsgt ")

    parser.add_option("--fetch_hsgt_top_10", action="store_true",
                      dest="fetch_hsgt_top_10_f", default=False,
                      help="fetch hsgt ")

    parser.add_option("--fetch_moneyflow_dd", action="store_true",
                      dest="fetch_moneyflow_dd_f", default=False,
                      help="fetch money flow dd")

    parser.add_option("-a", "--analyze", action="store_true",
                      dest="analyze_f", default=False,
                      help="analyze hsgt ")



    (options, args) = parser.parse_args()
    fetch_all_f = options.fetch_all_f
    fetch_hsgt_top_10_f = options.fetch_hsgt_top_10_f
    fetch_moneyflow_dd_f = options.fetch_moneyflow_dd_f
    analyze_f = options.analyze_f
    force_run_f = options.force_run_f
    debug_f = options.debug_f

    logging.info("fetch_all_f: " + str(fetch_all_f))
    logging.info("fetch_hsgt_top_10_f: " + str(fetch_hsgt_top_10_f))
    logging.info("fetch_moneyflow_dd_f: " + str(fetch_moneyflow_dd_f))
    logging.info("analyze_f: " + str(analyze_f))

    set_global(debug=debug_f,force_run=force_run_f)


    if fetch_all_f:
        fetch_hsgt_top_10()
        fetch_moneyflow_dd(fetch_whole=True, fetch_today=False)
    elif fetch_hsgt_top_10_f:
        fetch_hsgt_top_10()
    elif fetch_moneyflow_dd_f:
        #todayS = finlib.Finlib().get_last_trading_day()
        fetch_moneyflow_dd(fetch_whole=False, fetch_today=True)
    elif analyze_f:
        analyze_hsgt_top_10()


    pass
