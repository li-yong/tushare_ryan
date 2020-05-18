# coding: utf-8

import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt
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
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#this script comparing a stock's data in history, to get the lower PE/PB/ROE in the history (cheap & high grow rate) stock.


def select_from_result():
    input_csv = "/home/ryan/DATA/result/pe_pb_rank.csv"
    output_csv = "/home/ryan/DATA/result/pe_pb_rank_selected.csv"
    df_result = pd.DataFrame()

    df_rank = pd.read_csv(input_csv, converters={'end_date': str})

    df_result = df_rank[df_rank['pe_ttm_perc'] < 0.1]
    df_result = df_result[df_result['pb_perc'] < 0.1]
    df_result = df_result[df_result['roe'] > 15]

    df_result.to_csv(output_csv, encoding='UTF-8', index=False)
    print("\nPE/PB/ROE self percent result saved to " + output_csv + " len " + str(df_result.__len__()))


######
#
######
def get_indicator_history():
    output_csv = "/home/ryan/DATA/result/pe_pb_rank.csv"
    df_result = pd.DataFrame()

    #extract roe from fina_indicator.csv
    file = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_indicator.csv"

    df_fina_indicator = pd.read_csv(file, converters={'end_date': str})

    stock_list = finlib.Finlib().get_A_stock_instrment()  #600519
    #stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False) #SH600519
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  #600519.SH

    #stock_list = stock_list[stock_list['code']=='SH600519'] #ryan debug

    #for code_m in stock_list['code']: #code_m: SH600519
    for i in range(stock_list.__len__()):  #code_m: SH600519
        code_m = stock_list.iloc[i]['code']
        stock_name = stock_list.iloc[i]['name']

        #extract PE, PB, PE_TTM
        file_code = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/" + code_m + "_basic.csv"

        if not os.path.exists(file_code):
            print("\nno such file " + file_code)
            continue

        if os.stat(file_code).st_size < 10:
            print('empty input file ' + file_code)
            continue

        sys.stdout.write(code_m)

        df_sub = pd.read_csv(file_code, converters={'code': str})

        if not 'pe_ttm' in df_sub.columns:
            print("no column pe_ttm in the csv, " + file_code)
            continue

        pe_ttm_all = df_sub['pe_ttm']

        if pe_ttm_all.__len__() == 0:
            print("empty df, pe_ttm_all")
            continue

        pe_ttm_today = pe_ttm_all[-1:].values[0]
        pe_ttm_perc = stats.percentileofscore(pe_ttm_all, pe_ttm_today) / 100
        pe_ttm_perc = round(pe_ttm_perc, 2)

        pe_all = df_sub['pe']
        pe_today = pe_all[-1:].values[0]
        pe_perc = stats.percentileofscore(pe_all, pe_today) / 100
        pe_perc = round(pe_perc, 2)

        pb_all = df_sub['pb']
        pb_today = pb_all[-1:].values[0]
        pb_perc = stats.percentileofscore(pb_all, pb_today) / 100
        pb_perc = round(pb_perc, 2)

        ps_ttm_all = df_sub['ps_ttm']
        ps_ttm_today = ps_ttm_all[-1:].values[0]
        ps_ttm_perc = stats.percentileofscore(ps_ttm_all, ps_ttm_today) / 100
        pb_perc = round(pb_perc, 2)

        ps_all = df_sub['ps']
        ps_today = ps_all[-1:].values[0]
        ps_perc = stats.percentileofscore(ps_all, ps_today) / 100
        ps_perc = round(ps_perc, 2)

        latest_date = df_sub['trade_date'].iloc[-1]

        reg = re.match("(\d{4})(\d{2})(\d{2})", str(latest_date))

        if reg:
            yyyy = reg.group(1)
            mm = reg.group(2)
            dd = reg.group(3)
            latest_date = yyyy + "-" + mm + "-" + dd

        close = df_sub['close'].iloc[-1]

        dict = {
            'code': code_m,
            'name': stock_name,
            'date': latest_date,
            'close': close,
            'pe_ttm_perc': pe_ttm_perc,
            'pe_perc': pe_perc,
            'ps_ttm_perc': ps_ttm_perc,
            'ps_perc': ps_perc,
            'pb_perc': pb_perc,
            'pe': pe_today,
            'pe_ttm': pe_ttm_today,
            'ps': ps_today,
            'ps_ttm': ps_ttm_today,
            'pb': pb_today,
        }

        ################

        code_ts = code_m

        df_tmp = df_fina_indicator[df_fina_indicator['ts_code'] == code_ts]

        if df_tmp.__len__() == 0:
            pass
        else:
            roe = float(df_tmp.iloc[0]['roe'])
            dict['roe'] = roe

        ###==================

        df_tmp = pd.DataFrame(dict, index=[i])

        df_result = df_result.append(df_tmp)

        pass

    df_result.to_csv(output_csv, encoding='UTF-8', index=False)
    print("\nPE/PB/ROE self percent result saved to " + output_csv + " len " + str(df_result.__len__()))


def main():
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))
    get_indicator_history()
    select_from_result()


### MAIN ####
if __name__ == '__main__':
    main()

    print('script completed')
