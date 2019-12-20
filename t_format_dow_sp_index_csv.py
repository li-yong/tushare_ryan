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


def convert(input_csv, code):
    df = pd.read_csv(input_csv, skiprows=1, converters={'date': str}, header=None,
                     names=['date', 'o', 'h', 'l', 'c', 'vol'])

    df = df.reset_index().drop('index', axis=1)

    new_value_df = pd.DataFrame([code] * df.__len__(), columns=['code'])
    df = new_value_df.join(df)  # the inserted column on the head


    df.to_csv(input_csv, encoding='UTF-8', index=False)
    print('converted(overwrite) format file '+input_csv)




### MAIN ####
if __name__ == '__main__':
    ########################
    # The source csv is retrived by t_get_dow_sp500.sh
    # This script run after the t_get_dow_sp500.sh, to adding code column and change to ohlc cloumns sequence.
    #########################
    dow_f = '/home/ryan/DATA/pickle/DOW_SP/dow.csv'
    sp_f = '/home/ryan/DATA/pickle/DOW_SP/sp500.csv'


    df_result = convert(input_csv=dow_f, code='dow')
    df_result = convert(input_csv=sp_f, code='sp')


    print('script completed')

