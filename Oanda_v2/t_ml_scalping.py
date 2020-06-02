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
import datetime
import traceback
import sys
import tushare.util.conns as ts_cs
sys.path.append('/home/ryan/tushare_ryan/')
import finlib

from sklearn.cluster import KMeans
from calendar import monthrange

import stockstats
import tabulate
import os
import logging

from optparse import OptionParser

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)





profit_sum = 0

def close_short_order(transaction,row):
    global profit_sum
    print(__file__ + " " + "close an short order ")
    transaction["state"] = "none"
    transaction["point_quit"] = row['close']
    transaction["date_quit"] = row['date']
    transaction["point_gain"] = transaction["point_enter"] - transaction["point_quit"]
    print(__file__ + " " + "profit " + str(transaction["point_gain"]))
    profit_sum += transaction["point_gain"]

def close_long_order(transaction,row):
    global profit_sum
    print(__file__ + " " + "close an long order ")
    transaction["state"] = "none"
    transaction["point_quit"] = row['close']
    transaction["date_quit"] = row["date"]
    transaction["point_gain"] = transaction["point_quit"] - transaction["point_enter"]
    print(__file__ + " " + "profit " + str(transaction["point_gain"]))
    profit_sum += transaction["point_gain"]




def main():
    file_in = '/home/ryan/DATA/DAY_Forex_local/USD_CAD.csv'

    df = pd.read_csv(file_in, names=['code', 'date', 'open', 'high', 'low', 'close', 'volume'])
    # USDCAD,2020-05-25 00:00:00,1.39859,1.39864,1.39852,1.39852,18

    close_sma_2 = df['close'].rolling(window=2).mean()
    close_sma_5 = df['close'].rolling(window=5).mean()
    close_sma_8 = df['close'].rolling(window=8).mean()
    close_sma_13 = df['close'].rolling(window=13).mean()

    df['close_sma_2'] = close_sma_2
    df['close_sma_5'] = close_sma_5
    df['close_sma_8'] = close_sma_8
    df['close_sma_13'] = close_sma_13

    # https://stackoverflow.com/questions/48775841/pandas-ema-not-matching-the-stocks-ema
    close_ema_2 = df['close'].ewm(span=2, min_periods=0, adjust=False, ignore_na=False).mean  # exponential weighted.
    close_ema_5 = df['close'].ewm(span=5, min_periods=0, adjust=False, ignore_na=False).mean
    close_ema_8 = df['close'].ewm(span=8, min_periods=0, adjust=False, ignore_na=False).mean
    close_ema_13 = df['close'].ewm(span=13, min_periods=0, adjust=False, ignore_na=False).mean

    df['close_ema_2'] = close_ema_2
    df['close_ema_5'] = close_ema_5
    df['close_ema_8'] = close_ema_8
    df['close_ema_13'] = close_ema_13

    transaction = {
        "state": "none",  # [Long, Short, none]
        "point_enter": 0,
        "date_enter": 0,
        "point_quit": 0,
        "date_quit": 0,

        "point_tp": 0,
        "point_sl": 0,
        "point_gain": 0,
        "bar_passed": 0,
    }

    market = {
        "trend": "none",  # ['bull', "bear", "none"}
    }


    for index, row in df.iterrows():
        cur_close = row['close']
        close_sma_5 = row['close_sma_5']
        close_sma_8 = row['close_sma_8']
        close_sma_13 = row['close_sma_13']

        market_trend_pre = market['trend']

        # tp_sl_check(transaction, cur_close)
        if transaction['state'] == "short":
            if cur_close < transaction['point_tp']:
                #close_short_order(transaction,row) #take profit
                pass
            elif cur_close >  transaction['point_sl']:
                close_short_order(transaction, row)  # stop loss
        elif transaction['state'] == "long":
            if cur_close > transaction['point_tp']:
                #close_long_order(transaction,row) #take profit
                pass
            elif cur_close < transaction['point_sl']:
                close_long_order(transaction, row)  # stop loss

        if close_sma_5 > close_sma_8 and close_sma_8 > close_sma_13:
            market['trend'] = "bull"
        elif close_sma_13 > close_sma_8 and close_sma_8 > close_sma_5:
            market['trend'] = "bear"

        if market_trend_pre == "bear" and market['trend'] == "bull":  # from bear to bull
            if transaction['state'] == "short":
                close_short_order(transaction, row)


            if transaction['state'] == "none":
                print(__file__ + " " + "make an long order, price "+str(row['close'])+" date "+row['date'])
                transaction["state"] = "long"
                transaction["point_enter"] = cur_close
                transaction["date_enter"] = row['date']
                transaction["point_tp"] = cur_close + 0.00010
                transaction["point_sl"] = cur_close - 0.00020
            elif transaction['state'] == "long":
                print(__file__ + " " + "hold an long order ")

        if market_trend_pre == "bull" and market['trend'] == "bear":  # from bull to bear
            if transaction['state'] == "long":
                close_long_order(transaction, row)


            if transaction['state'] == "none":
                print(__file__ + " " + "make an short order, price " + str(row['close']) + " date " + row['date'])
                transaction["state"] = "short"
                transaction["point_enter"] = cur_close
                transaction["date_enter"] = row['date']
                transaction["point_tp"] = cur_close - 0.00010
                transaction["point_sl"] = cur_close + 0.00020
            elif transaction['state'] == "short":
                print(__file__ + " " + "hold an short order ")

        pass

    print("profit_sum: " + str(profit_sum))


if __name__ == '__main__':
    main()
