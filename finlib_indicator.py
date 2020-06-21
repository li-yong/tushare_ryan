# coding: utf-8

import tushare as ts
import tushare.util.conns as ts_cs
import tushare.stock.trading as ts_stock_trading

import talib
import pickle
import os
import os.path
import pandas as pd
import time
import numpy as np
import tabulate
import collections
import stat

# import matplotlib.pyplot as plt
# from pandas.plotting import register_matplotlib_converters
# register_matplotlib_converters()

import pandas
import mysql.connector
from sqlalchemy import create_engine
import re
import math
from datetime import datetime, timedelta
from scipy import stats
import sys
import traceback
from jaqs.data.dataapi import DataApi
import glob
import stockstats

import finlib
import logging
import yaml
import warnings

# warnings.filterwarnings("error")
warnings.filterwarnings("default")

class Finlib_indicator:
    def add_ma_ema(self,df):
        logging.info("adding ma to df")

        #sma short
        close_sma_2 = df['close'].rolling(window=2).mean()
        close_sma_5 = df['close'].rolling(window=5).mean()
        close_sma_8 = df['close'].rolling(window=8).mean()
        close_sma_13 = df['close'].rolling(window=13).mean()


        #sma long
        close_sma_9 = df['close'].rolling(window=9).mean()
        close_sma_21 = df['close'].rolling(window=21).mean()
        close_sma_55 = df['close'].rolling(window=55).mean()


        df['close_sma_2'] = close_sma_2
        df['close_sma_5'] = close_sma_5
        df['close_sma_8'] = close_sma_8
        df['close_sma_13'] = close_sma_13

        df['close_sma_9'] = close_sma_9
        df['close_sma_21'] = close_sma_21
        df['close_sma_55'] = close_sma_55



        #ema short
        close_ema_2 = df['close'].ewm(span=2, min_periods=0, adjust=False, ignore_na=False).mean()  # exponential weighted.
        close_ema_5 = df['close'].ewm(span=5, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_8 = df['close'].ewm(span=8, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_10 = df['close'].ewm(span=10, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_13 = df['close'].ewm(span=13, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_20 = df['close'].ewm(span=20, min_periods=0, adjust=False, ignore_na=False).mean()

        #ema long
        close_ema_9 = df['close'].ewm(span=9, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_21 = df['close'].ewm(span=21, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_55 = df['close'].ewm(span=55, min_periods=0, adjust=False, ignore_na=False).mean()

        df['close_ema_2'] = close_ema_2
        df['close_ema_5'] = close_ema_5
        df['close_ema_8'] = close_ema_8
        df['close_ema_13'] = close_ema_13

        df['close_ema_9'] = close_ema_9
        df['close_ema_21'] = close_ema_21
        df['close_ema_55'] = close_ema_55

        df['close_ema_10'] = close_ema_10
        df['close_ema_20'] = close_ema_20

        return (df)


    #Average True Range
    def ATR(self, df, n):
        ### Prepare.  Adding t-1 days' value to t ###
        df1 = df[['date', 'close', 'volume']]
        df1 = df1.rename(columns={'date':'date_pre','close': 'close_pre', 'volume':'volume_pre'})
        df1 = df1.shift(periods=1)
        df = df.merge(df1, left_index=True, right_index=True)
        df = df.drop(columns=['date_pre'],axis=1)

        #### ATR  ####
        TR_l = [0]
        for i in range(1, df.__len__()):
            TR = max(df.at[i, 'high'], df.at[i, 'close_pre']) - min(df.at[i ,'low'], df.at[i, 'close_pre'])
            TR_l.append(TR)
        TR_s = pd.Series(TR_l).rename("TR")
        df = df.join(TR_s)
        ATR = TR_s.ewm(span = n, min_periods = n).mean().rename("ATR_"+str(n))
        df = df.join(ATR)

        return df



    def upper_body_lower_shadow(self,df):
        ###### Upper_shadow, Body, Lower_shadow ####
        df_a = pd.DataFrame([[0,0,0]] * df.__len__(), columns=['upper_shadow','body','lower_shadow'])
        df = df.merge(df_a, left_index=True, right_index=True)

        for i in range(df.__len__()):
            upper_shadow = df.at[i, 'high'] - max(df.at[i, 'open'], df.at[i, 'close'])
            body = abs(df.at[i, 'close'] - df.at[i, 'open'] )
            lower_shadow = min(df.at[i, 'open'], df.at[i, 'close']) - df.at[i, 'low']

            df.iloc[i, df.columns.get_loc('upper_shadow')] = upper_shadow
            df.iloc[i, df.columns.get_loc('body')] = body
            df.iloc[i, df.columns.get_loc('lower_shadow')] = lower_shadow


        return df





    #Keltner Channel
    def KELCH(self, df, n):
        KelChM = ((df['high'] + df['low'] + df['close']) /3 ).rolling(n).mean().rename('KelChM_' + str(n))
        KelChU = ((4 * df['high'] - 2 * df['low'] + df['close']) / 3).rolling(n).mean().rename('KelChU_' + str(n))
        KelChD = ((-2 * df['high'] + 4 * df['low'] + df['close']) / 3).rolling(n).mean().rename('KelChD_' + str(n))

        df = df.join(KelChU)
        df = df.join(KelChM)
        df = df.join(KelChD)
        return df


    def sma_jincha_sicha_duotou_koutou(self,df,short,middle,long):
        stock = stockstats.StockDataFrame.retype(df)

        trend_short = trend_middle = trend_long = 'NA'

        df_sma_short = stock['close_'+str(short)+'_sma']
        df_sma_middle = stock['close_'+str(middle)+'_sma']
        df_sma_long = stock['close_'+str(long)+'_sma']
        df_sma_60 = stock['close_'+str(60)+'_sma']
        df_sma_200 = stock['close_'+str(200)+'_sma']


        print("stockstats sma short,middle,long "+str(df_sma_short[-1])+" "+str(df_sma_middle[-1])+" "+str(df_sma_long[-1]))


        df_ema_short = stock['close_'+str(short)+'_ema']
        df_ema_middle = stock['close_'+str(middle)+'_ema']
        df_ema_long = stock['close_'+str(long)+'_ema']
        print("stockstats ema short,middle,long "+str(df_ema_short[-1])+" "+str(df_ema_middle[-1])+" "+str(df_ema_long[-1]))



        ma_short = df_sma_short[-1]
        ma_middle = df_sma_middle[-1]
        ma_long = df_sma_long[-1]

        ma_short_p1 = df_sma_short[-2]
        ma_middle_p1 = df_sma_middle[-2]
        ma_long_p1 = df_sma_long[-2]

        if ma_short > ma_middle and ma_short_p1 < ma_middle_p1:
            logging.info("short up across middle, jin cha minor")
        elif ma_short < ma_middle and ma_short_p1 > ma_middle_p1:
            logging.info("short down across middle, si cha minor")

        if ma_middle > ma_long and ma_middle_p1 < ma_long_p1:
            logging.info("middle up across long, jin cha major")
        elif ma_middle < ma_long and ma_middle_p1 > ma_long_p1:
            logging.info("middle down across long, si cha major")




        if ma_short > ma_middle*1.05 :
            trend_short='up'
        elif ma_short*1.05 < ma_middle:
            trend_short = 'down'

        if ma_middle > ma_long*1.05:
            trend_middle = 'up'
        elif ma_middle*1.05 < ma_long:
            trend_middle = 'down'

        if (ma_short >ma_middle > ma_long):
            trend_long = 'up'
            logging.info("duo tou pai lie")
            if df['low'][-1] > ma_short:
                logging.info("verify strong up trend")
            logging.info("check back last 30 bars")
            for i in range(30):
                if (df_sma_short[-i] > df_sma_middle[-i] > df_sma_long[-i]):
                    logging.info("duo tou lasts "+str(i)+ "days")
                    continue

                if (df_sma_short[-i] < df_sma_middle[-i]  < df_sma_long[-i]):
                    logging.info("latest kong tou pailie is "+str(i) + " days before at "+df.iloc[-i].name)
                    break

        if (ma_short < ma_middle < ma_long):  #more interesting enter when price is up break
            trend_long = 'down'
            logging.info("kong tou pai lie")


        pass
        return()

    def price_counter(self,df):
        rtn_dict = {}
        ser_price = df['close'].append(df['open']).append(df['high']).append(df['low'])
        ser_price = ser_price[ser_price>0]

        #round determin the precision.
        common_prices = collections.Counter(round(ser_price,0)).most_common()

        sum = 0
        occu_list = []
        for i in common_prices:
            sum += i[1]
            occu_list.append(i[1])

        new_dict = {}

        for i in common_prices:
            price =  i[0]
            frequency_percent = round(stats.percentileofscore(occu_list,i[1]),1)
            occurrence_percent = round(i[1]*100/sum,1)
            new_dict[price] = {'price':price, 'frequency_percent':frequency_percent,
                               'occurrence_percent':occurrence_percent,
                               'occurrence':i[1], 'sum':sum}

        sorted_price_list = list(collections.OrderedDict(sorted(new_dict.items(), reverse=True)).keys()) #[71,70,...44]

        rtn_dict['price_freq_dict'] = new_dict
        rtn_dict['sorted_price_list'] = sorted_price_list


        current_price = df['close'].iloc[-1]
        logging.info("current price "+str(current_price))
        rtn_dict['current_price'] = current_price

        v = min(sorted_price_list, key=lambda x: abs(x - current_price))
        idx = sorted_price_list.index(v)

        if v > current_price:
            idx_h1 = idx
            idx_l1 = idx +1
        elif v== current_price:
            idx_h1 = idx-1
            idx_l1 = idx+1
        elif v < current_price:
            idx_h1 = idx-1
            idx_l1 = idx

        if idx_h1 -4  >= 0:
            H5 =  new_dict[sorted_price_list[idx_h1-4]]
            rtn_dict['H5'] = H5
            logging.info("H5, price "+str(H5['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(H5['frequency_percent'])+" freq "+str(H5['occurrence_percent']))
        if idx_h1 -3  >= 0:
            H4 =  new_dict[sorted_price_list[idx_h1-3]]
            rtn_dict['H4'] = H4
            logging.info("H4, price "+str(H4['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(H4['frequency_percent'])+" freq "+str(H4['occurrence_percent']))
        if idx_h1 -2  >= 0:
            H3 =  new_dict[sorted_price_list[idx_h1-2]]
            rtn_dict['H3'] = H3
            logging.info("H3, price "+str(H3['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(H3['frequency_percent'])+" freq "+str(H3['occurrence_percent']))
        if idx_h1 -1  >= 0:
            H2 =  new_dict[sorted_price_list[idx_h1-1]]
            rtn_dict['H2'] = H2
            logging.info("H2, price "+str(H2['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(H2['frequency_percent'])+" freq "+str(H2['occurrence_percent']))
        if idx_h1 >= 0:
            H1 =  new_dict[sorted_price_list[idx_h1]]
            rtn_dict['H1'] = H1
            logging.info("H1, price "+str(H1['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(H1['frequency_percent'])+" freq "+str(H1['occurrence_percent']))


        if idx_l1 <= sorted_price_list.__len__():
            L1 = new_dict[sorted_price_list[idx_l1]]
            rtn_dict['L1'] = L1
            logging.info("L1, price "+str(L1['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(L1['frequency_percent'])+" freq "+str(L1['occurrence_percent']))

        if idx_l1 + 1 <= sorted_price_list.__len__():
            L2 = new_dict[sorted_price_list[idx_l1 + 1]]
            rtn_dict['L2'] = L2
            logging.info("L2, price "+str(L2['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(L2['frequency_percent'])+"  freq "+str(L2['occurrence_percent']))

        if idx_l1 +2 <= sorted_price_list.__len__():
            L3 = new_dict[sorted_price_list[idx_l1 + 2]]
            rtn_dict['L3'] = L3
            logging.info("L3, price "+str(L3['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(L3['frequency_percent'])+" freq "+str(L3['occurrence_percent']))

        if idx_l1 +3 <= sorted_price_list.__len__():
            L4 = new_dict[sorted_price_list[idx_l1 + 3]]
            rtn_dict['L4'] = L4
            logging.info("L4, price "+str(L4['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(L4['frequency_percent'])+" freq "+str(L4['occurrence_percent']))

        if idx_l1 +4 <= sorted_price_list.__len__():
            L5 = new_dict[sorted_price_list[idx_l1 + 4]]
            rtn_dict['L5'] = L5
            logging.info("L5, price "+str(L5['price'])+ ", freq perc in "+str(df.__len__())+" bars "+str(L5['frequency_percent'])+" freq "+str(L5['occurrence_percent']))


        pass
        return(rtn_dict)







'''
         {58.0: {'price': 58.0,
          'frequency_rank': 1,
          'frequency_perc': 100.0,
          'occurrence': 141,
          'sum': 1176,
          'occurrence_perc': 12.0},
          
         59.0: {'price': 59.0,
          'frequency_rank': 2,
          'frequency_perc': 96.42857142857143,
          'occurrence': 120,
          'sum': 1176,
          'occurrence_perc': 10.2},
'''
