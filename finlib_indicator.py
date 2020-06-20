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
        close_ema_13 = df['close'].ewm(span=13, min_periods=0, adjust=False, ignore_na=False).mean()

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


        df_sma_short = stock['close_'+str(short)+'_sma']
        df_sma_middle = stock['close_'+str(middle)+'_sma']
        df_sma_long = stock['close_'+str(long)+'_sma']
        df_sma_60 = stock['close_'+str(60)+'_sma']
        df_sma_200 = stock['close_'+str(200)+'_sma']


#        print(str(df_sma_short[-1])+" "+str(df_sma_middle[-1])+" "+str(df_sma_long[-1]))

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

        if (ma_short >ma_middle > ma_long):
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




        pass
        return()
