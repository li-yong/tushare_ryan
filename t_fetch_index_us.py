# coding: utf-8


import finsymbols
import os
import pickle
import pandas as pd

import pprint
import sys
from bs4 import BeautifulSoup
from finsymbols.symbol_helper import *




import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S',level=logging.DEBUG)


logging.info("\n")
logging.info("SCRIPT STARTING " + " ".join(sys.argv))



HKHS_Prepare = False
HKHS_Prepare = True

if HKHS_Prepare:
    #manually made csv from https://zh.wikipedia.org/zh-cn/%E6%81%92%E7%94%9F%E6%8C%87%E6%95%B8
    #this is to covert the short code to full code, eg 1 --> 00001
    csv = "/home/ryan/Downloads/hkhs_index.csv"
    df = pd.read_csv(csv,dtype=str)

    #appending a new column
    new_value_df = pd.DataFrame([0] * df.__len__(), columns=['code_full'])  #
    df = new_value_df.join(df)

    for i in range(df.__len__()):
        code = df.iloc[i]['code']
        code_len = len(code)
        #hk code is 5 char

        full_code='0'*(5-code_len)+code
        logging.info(full_code)

        df.iloc[i, df.columns.get_loc('code_full')]=full_code


    ###### HK Heng SHeng ################
    csv= "/home/ryan/DATA/pickle/INDEX_US_HK/hkhs.csv"
    df = df.drop('code', axis=1)
    df.rename(columns={"code_full": "code"}, inplace=True)
    df=df[['code','name','flow_perc', 'weight']]
    df.to_csv(csv, encoding='UTF-8', index=False)
    logging.info("HKHS saved to "+csv)

pass


########## SP 500 ##########
csv= "/home/ryan/DATA/pickle/INDEX_US_HK/sp500.csv"
logging.info("getting SP500")
sp500 = finsymbols.get_sp500_symbols()

#conver list of dict to df
sp500_df = pd.DataFrame(sp500)
sp500_df.rename(columns={"symbol": "code"}, inplace=True)
cols=['code','company','headquarters','industry','sector']
sp500_df = sp500_df[cols]
sp500_df.to_csv(csv, encoding='UTF-8', index=False)
logging.info("SP500 saved to "+csv)





logging.info('script completed')
os._exit(0)





# http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download

logging.info("getting nasdaq")
nasdaq = finsymbols.get_nasdaq_symbols()
logging.info("dumpping nasdaq")
pickle.dump(nasdaq, open( "/home/ryan/DATA/pickle/INDEX_US/nasdaq.pickle", "wb" ) )



logging.info("getting amex")
amex = finsymbols.get_amex_symbols()
logging.info("dumpping amex")
pickle.dump(amex, open( "/home/ryan/DATA/pickle/INDEX_US/amex.pickle", "wb" ) )



logging.info("getting nyse")
nyse = finsymbols.get_nyse_symbols()
logging.info("dumpping nyse")
pickle.dump(nyse, open( "/home/ryan/DATA/pickle/INDEX_US/nyse.pickle", "wb" ) )





'''
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
from optparse import OptionParser

parser = OptionParser()

parser.add_option("-k", "--fetch_hk", action="store_true",
                  dest="fetch_hk", default=False,
                  help="fetch HK stock")

parser.add_option("-u", "--fetch_us", action="store_true",
                  dest="fetch_us", default=False,
                  help="fetch US stock")


(options, args) = parser.parse_args()


fetch_hk=options.fetch_hk
fetch_us=options.fetch_us


# This script to get US,HK stock daily bar.

# delete /home/ryan/DATA/pickle/instrument.csv to fetch again.



def get_hk_us(df,cons, start_date, appendix,todayS):
    default_date_d = datetime.datetime.strptime(start_date, '%Y-%m-%d')

    fast_fetch = True #for full fetch
    fast_fetch = False #for daily update

    for i in range(df.__len__()):
        a_wait_time = 0 #looks no wait works even for a frequent fetch. Just re-request the conns


        start_date_req = start_date  #start_date_req: date sent to the api

        code = df.iloc[i]['code']
        name = df.iloc[i]['name']
        #cons = ts.get_apis()  #<<<<< renew connection every time

        sys.stdout.write(str(i)+"/"+str(df.__len__())+" get " + str(code) + " " + str(name) + ". " + appendix+". ") #print without newline
        #a_csv = '/home/ryan/DATA/DAY_Global/' + str(code) + '.' + appendix  # WUBA.CH
        a_csv = '/home/ryan/DATA/DAY_Global/' + appendix+'/'+str(code) + '.' + appendix  # DATA/DAY_Global/CH/WUBA.CH
        df_tmp=pandas.DataFrame()  #exists csv
        a_stock_df=pandas.DataFrame() #data fetched this time.


        #resume the last update. Enable this when run mutliple times in same day. To get a fully update quickly.
        if fast_fetch:
            a_wait_time = 0
            if os.path.isfile(a_csv):
                continue

        if os.path.isfile(a_csv):

            if os.stat(a_csv).st_size == 0:
                logging.info("empty file, skip. "+a_csv)
                continue

            df_tmp = pd.read_csv(a_csv, converters={'code': str})
            last_row = df_tmp[-1:]
            last_date = last_row['datetime'].values[0]
            next_date = datetime.datetime.strptime(last_date, '%Y-%m-%d') + datetime.timedelta(1)
            a_week_before_date = datetime.datetime.strptime(todayS, '%Y-%m-%d') - datetime.timedelta(7)

            #if next_date > datetime.datetime.today():
            if next_date.strftime('%Y-%m-%d') > todayS:
                logging.info("file already updated, not fetching again. " + a_csv+". updated to "+last_date)
                continue

            #last date in csv is 7 days ago, most likely the source is not update, so skip this csv.
            #logging.info("Next "+next_date.strftime('%Y-%m-%d'))
            #logging.info("a week before "+ a_week_before_date.strftime('%Y-%m-%d'))
            if next_date.strftime('%Y-%m-%d') < a_week_before_date.strftime('%Y-%m-%d'):
                logging.info("file too old to updated, not fetching. " + a_csv+". updated to "+last_date)
                #continue



            if next_date > default_date_d: #csv already have data
                start_date_req = next_date.strftime('%Y-%m-%d')
                sys.stdout.write("append exist csv from "+start_date_req+". ")
            else:
                sys.stdout.write("will do a full update, since "+start_date_req+". ")


        try:
            exc_info = sys.exc_info()
            print("code "+str(code))
            a_stock_df = ts.bar(code, conn=cons, asset='X', adj='qfq', start_date=start_date_req, end_date='')
            #time.sleep(2)

            if str(a_stock_df) == 'None':
                sys.stdout.write("ts.bar return None. retry " + str(code) +". 1st. ")
                ts.close_apis(cons)
                time.sleep(a_wait_time)
                cons = ts.get_apis()
                a_stock_df = ts.bar(code, conn=cons, asset='X', adj='qfq', start_date=start_date_req, end_date='')
        except:
            logging.info("\tcaught exception when getting data, try to renew cons")
            ts.close_apis(cons)
            time.sleep(a_wait_time)

            cons = ts.get_apis()
            a_stock_df = ts.bar(code, conn=cons, asset='X', adj='qfq', start_date=start_date_req, end_date='')
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info


        if str(a_stock_df) == 'None':
            logging.info("ts.bar return None for code. Retry exhausted. "+str(code))
            continue

       #At this stage, a_stock_df should be valid.

        try:
            exc_info = sys.exc_info()
            a_stock_df = a_stock_df.reindex(index=a_stock_df.index[::-1]) #revert

            a_stock_df = a_stock_df.reset_index()

            sys.stdout.write(" len "+str(a_stock_df.__len__())+". ")

            df_name = pd.DataFrame([name] * a_stock_df.__len__(), columns=['name']);
            df_short_name = pd.DataFrame([appendix] * a_stock_df.__len__(), columns=['short_name']);
            a_stock_df = a_stock_df.join(df_name) #append column
            a_stock_df = a_stock_df.join(df_short_name) #append column

            #logging.info(a_stock_df.head(2)) #debug.


            cols=['code','datetime','open','high','low','close','vol','name']
            a_stock_df = a_stock_df[cols] #re-arrange adjust columns

            a_stock_df['open'] = a_stock_df['open'].round(2)
            a_stock_df['high'] = a_stock_df['high'].round(2)
            a_stock_df['low'] = a_stock_df['low'].round(2)
            a_stock_df['close'] = a_stock_df['close'].round(2)
            a_stock_df['vol'] = a_stock_df['vol'].round(2)
            a_stock_df['datetime'] = a_stock_df['datetime'].astype('str')

            a_stock_df = df_tmp.append(a_stock_df, ignore_index=True)

            a_stock_df.to_csv(a_csv, encoding='UTF-8', index=False)
            logging.info("saved "+a_csv)
        except:
            logging.info("\tcaught exception when processing to csv")
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info


        pass


df_instrument = finlib.Finlib().get_instrument()

#instrument.csv
#market,category,name,short_name
#31,2,香港主板,KH
#40,11,中国概念股,CH
#41,11,美股知名公司,MG
#48,2,香港创业板,KG
#74,13,美国股票,US  <<<<<  13,74,APLE,Apple,APLE




cons = ts.get_apis()
start_date = '2010-01-01'


if fetch_hk:
    todayS = datetime.datetime.today().strftime('%Y-%m-%d')

    df_KH = df_instrument.query("market==31 and category==2").reset_index().drop('index', axis=1)  # 1973
    df_KG = df_instrument.query("market==48 and category==2").reset_index().drop('index', axis=1)  # 425

    logging.info("df_KH " + str(df_KH.__len__()))
    logging.info("df_KG " + str(df_KG.__len__()))

    get_hk_us(df_KH,cons,start_date,'KH', todayS=todayS) #31,2,香港主板,KH
    get_hk_us(df_KG,cons,start_date,'KG', todayS=todayS) #48,2,香港创业板,KG

if fetch_us:
    yesterday = datetime.datetime.today() - datetime.timedelta(1)
    todayS = yesterday.strftime('%Y-%m-%d')

    df_CH = df_instrument.query("market==40 and category==11").reset_index().drop('index', axis=1)  # 78
    df_MG = df_instrument.query("market==41 and category==11").reset_index().drop('index', axis=1)  # 289
    df_US = df_instrument.query("market==74 and category==13").reset_index().drop('index', axis=1)  # 11278

    logging.info("df_CH " + str(df_CH.__len__()))
    logging.info("df_MG " + str(df_MG.__len__()))
    logging.info("df_US " + str(df_US.__len__()))

    get_hk_us(df_CH,cons,start_date,'CH', todayS=todayS) #40,11,中国概念股,CH
    get_hk_us(df_MG,cons,start_date,'MG', todayS=todayS) #41,11,美股知名公司,MG
    get_hk_us(df_US,cons,start_date,'US', todayS=todayS) #74,13,美国股票,US


logging.info('script completed')
os._exit(0)

'''
