# coding: utf-8

import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt
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
import signal
from optparse import OptionParser

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S',level=logging.DEBUG)



# This script is to analysis top 10 holder of stocks.

#reference
#top_10_holder_summary_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/_summary.csv"
#top_10_holder_detail_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/_detail.csv"

top_10_holder_summary_csv = "/home/ryan/DATA/result/top_10_holder_summary_full.csv"
top_10_holder_detail_csv = "/home/ryan/DATA/result/top_10_holder_detail_full.csv"


#output csv
latest_holder_summary_csv = "/home/ryan/DATA/result/top_10_holder_summary_latest.csv"
latest_holder_detail_csv = "/home/ryan/DATA/result/top_10_holder_detail_latest.csv"



def get_top_10_holder_data():
    #code,name_x,quarter,name,hold,h_pro,sharetype,status
    # 600519,贵州茅台,2019-03-31,中国贵州茅台酒厂(集团)有限责任公司,77877.20,61.99,流通A股,未变
    # 600519,贵州茅台,2019-03-31,香港中央结算有限公司,12078.66,9.62,流通A股,2083.28
    # 600519,贵州茅台,2019-03-31,贵州茅台酒厂集团技术开发公司,2781.21,2.21,流通A股,未变
    # 600519,贵州茅台,2019-03-31,易方达资产管理(香港)有限公司－客户资金(交易所),1226.28,0.98,流通A股,未变

    #vim summary_600519.csv
    # code,name,quarter,amount,changed,props
    # 600519,贵州茅台,2019-03-31,97380.22,1986.03,77.53

    logging.info("getting security")
    df_code_name_map = finlib.Finlib().get_A_stock_instrment()

    df_summary = pd.DataFrame()
    df_detail = pd.DataFrame()

    leng = df_code_name_map.__len__()

    i=0




    for code in list(df_code_name_map['code']):

        code = str(code)
        name = df_code_name_map[df_code_name_map['code']==code]['name'].values[0]
        i+=1
        logging.info(code + " "+ name + " " + str(i + 1) + '/' + str(leng))

        df_a_sum = df_a_detail = pd.DataFrame()

        top_10_summary_csv = '/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/summary_' + str(code) + '.csv'
        top_10_detail_csv = '/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/detail_' + str(code) + '.csv'

        #if os.path.isfile(top_10_summary_csv) and os.path.isfile(top_10_detail_csv):
        if finlib.Finlib().is_cached(top_10_summary_csv, day=3) and finlib.Finlib().is_cached(top_10_detail_csv, day=3):
            pass
        else:
            try:
                exc_info = sys.exc_info()
                logging.info("getting top 10 holder of " + code)
                a = ts.top10_holders(code=code)

                df_a_sum = a[0]
                df_a_detail = a[1]


                df_a_sum = pd.DataFrame([name] * df_a_sum.__len__(), columns=['name']).join(df_a_sum)  #
                df_a_sum = pd.DataFrame([code] * df_a_sum.__len__(), columns=['code']).join(df_a_sum)
                #
                df_a_detail = pd.DataFrame([name] * df_a_detail.__len__(), columns=['name_x']).join(df_a_detail)  #
                df_a_detail = pd.DataFrame([code] * df_a_detail.__len__(), columns=['code']).join(df_a_detail)  #

                df_a_sum.to_csv(top_10_summary_csv, encoding='UTF-8', index=False)
                df_a_detail.to_csv(top_10_detail_csv, encoding='UTF-8', index=False)
                logging.info('top 10 holder summary and detail csv files were saved.')
            except:
                logging.info("\tcaught exception, top10_holders, code " + code)
            finally:
                pass
                if exc_info == (None, None, None):
                    pass  # no exception
                else:
                    traceback.print_exception(*exc_info)
                del exc_info
                #sys.exc_clear()  #python3: AttributeError: module 'sys' has no attribute 'exc_clear'


def load_top_10_holder_data():
    logging.info("getting security")
    df_code_name_map = finlib.Finlib().get_A_stock_instrment()

    df_summary = pd.DataFrame()
    df_detail = pd.DataFrame()

    leng = df_code_name_map.__len__()

    i=0

    for code in list(df_code_name_map['code']):

        code = str(code)
        name = df_code_name_map[df_code_name_map['code']==code]['name'].values[0]
        i+=1
        logging.info(code + " "+ name + " " + str(i + 1) + '/' + str(leng))

        df_a_sum = df_a_detail = pd.DataFrame()

        top_10_summary_csv = '/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/summary_' + str(code) + '.csv'
        top_10_detail_csv = '/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/detail_' + str(code) + '.csv'

        if (finlib.Finlib().is_cached(top_10_summary_csv, day=10) and finlib.Finlib().is_cached(top_10_detail_csv, day=10)):
            df_a_sum = pd.read_csv(top_10_summary_csv, converters={'code': str})
            df_a_detail = pd.read_csv(top_10_detail_csv, converters={'code': str})
        else:
            logging.warn("file is not update more than 3 days. "+top_10_summary_csv+" or "+top_10_detail_csv)

        #memory issues
        df_summary = df_summary.append(df_a_sum)
        df_detail = df_detail.append(df_a_detail)

    df_summary.to_csv(top_10_holder_summary_csv,  encoding='UTF-8', index=False)
    df_detail.to_csv(top_10_holder_detail_csv,  encoding='UTF-8', index=False)
    return(df_summary,df_detail)



def analyze_summary(debug=False):

    df_result = pd.DataFrame()
    if debug:
        df_summary=pd.read_csv(top_10_holder_summary_csv, converters={'code': str}) #debug
    else:
        df_summary = pd.read_csv(top_10_holder_summary_csv, converters={'code': str})
        df_details = pd.read_csv(top_10_holder_detail_csv, converters={'code': str})

    df = df_summary
    df = df[df['quarter'] > '2019']

    codes=df['code'].unique()
    leng = codes.__len__()

    i=0
    for code in codes:
        i += 1
        logging.info("" + code +" "+str(i) +'/'+str(leng))


        df_tmp = df[df['code'] == code]
        df_tmp = df_tmp.reset_index().drop('index', axis=1)

        last_record = df_tmp.iloc[0]  # the topest (newest record)
        change_percent = 100* last_record['changed']/(last_record['amount'] - last_record['changed'])
        df_tmp = pd.DataFrame([change_percent] * 1, columns=['change_percent']).join(df_tmp)  #

        df_result = df_result.append(df_tmp)
        pass


    df_result = df_result.reset_index().drop('index',axis=1)
    df_result.to_csv(latest_holder_summary_csv, encoding='UTF-8', index=False)
    logging.info("latest holder summary saved to "+latest_holder_summary_csv)
    return(df_result)


def analyze_detail(debug=False):
    df_result = pd.DataFrame(columns=['name','investment','hold_stocks'])
    df_details = pd.read_csv(top_10_holder_detail_csv, converters={'code': str})
    if debug:
        df_details = df_details.head(1000)# debug

    df = df_details
    df = df[df['quarter'] > '2019']

    names=df['name'].unique()
    leng = names.__len__()

    i=0
    for name in names:
        logging.info("" + name +" "+str(i+1) +'/'+str(leng))

        df_tmp = df[df['name'] == name]
        df_tmp = df_tmp.reset_index().drop('index', axis=1)
        df_tmp = pd.DataFrame([0] * df_tmp.__len__(), columns=['investment']).join(df_tmp)

        investment = 0
        hold_stocks=''
        for j in range(df_tmp.__len__()):
            code = df_tmp.iloc[j]['code']
            code_name = df_tmp.iloc[j]['name_x']
            h_pro = df_tmp.iloc[j]['h_pro']
            hold_stocks += code + "_" + code_name+"_"+str(h_pro)+" "
            logging.info('\t '+ code + " " + code_name)

            hold = df_tmp.iloc[j]['hold']  #单位万股

            code_m = finlib.Finlib().add_market_to_code_single(code)
            price = finlib.Finlib().get_price(code_m)
            investment += price * hold  #单位万元
            #df_tmp.iloc[j, df_tmp.columns.get_loc('investment')] = investment

        df_result = df_result.append(pd.DataFrame(
            {
                'name': [name],
                'investment': [int(investment)],
                'hold_stocks': [hold_stocks],

            }))


        i += 1


    #df_result = df_result.reset_index().drop('index',axis=1)

    cols = [ 'name', 'investment', 'hold_stocks']  # adjust column order, sort column order.
    df_result = df_result[cols]
    df_result = df_result.sort_values('investment', ascending=False)

    df_result.to_csv(latest_holder_detail_csv, encoding='UTF-8', index=False)
    logging.info("latest holder detaill saved to "+latest_holder_detail_csv)
    return(df_result)


### MAIN ####
def main():

    logging.info("\n")
    logging.info("SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-f", "--fetch", action="store_true",
                      dest="fetch_data_f", default=False,
                      help="fetch top 10 holder")

    parser.add_option("-a", "--analyze", action="store_true",
                      dest="analyze_f", default=False,
                      help="analyze top_10_holder")

    (options, args) = parser.parse_args()
    fetch_data_f = options.fetch_data_f
    analyze_f = options.analyze_f


    logging.info("fetch_data_f: " + str(fetch_data_f))
    logging.info("analyze_f: " + str(analyze_f))

    if fetch_data_f:
        ########################
        # download reference data from tushare.
        #########################
        get_top_10_holder_data()




    elif analyze_f:
        ########################
        # load reference data from tushare.
        #########################
        (df_summary, df_detail) = load_top_10_holder_data()

        ########################
        # analyze the fenghong of each time, rows are not merged by code
        #########################
        df_result = analyze_summary()

        ########################
        # Merge fenghong by code, give score
        #########################
        #df_result = analyze_detail(debug=True)
        df_result = analyze_detail(debug=False)


    logging.info('script completed')
    os._exit(0)


if __name__ == '__main__':
    main()
