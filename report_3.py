# coding: utf-8
# encoding= utf-8

import pandas as pd
from pandas import DataFrame

import finlib
from scipy import stats
import finlib_indicator

import tushare as ts
import datetime
import sys
import talib 
import logging
import time
import os
import re
import constant
import math
import time
import numpy as np
import matplotlib.pyplot as plt
import copy

import akshare as ak
import glob
from scipy.stats import variation

########################### graham instrinsic value

def coefficient_variation_price_amount():

    # a = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')
    # a = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG')
    a = finlib.Finlib().get_stock_configuration(selected=False, stock_global='AG')

    df_stock_list = a['stock_list']
    df_csv_dir = a['csv_dir']
    df_out_dir = a['out_dir']
    df_rtn = pd.DataFrame()

    i=1
    for index, row in df_stock_list.iterrows():
        name, code = row['name'], row['code']
        print(str(i)+" of "+str(df_stock_list.__len__())+" , "+str(code)+" "+name)
        i+=1

        csv = df_csv_dir + "/"+ code+".csv"
        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv).tail(100)
        cv_close = round(variation(df['close']), 2)
        cv_amount = round(variation(df['amount']), 2)
        df_rtn = df_rtn.append(pd.DataFrame().from_dict({'name':[name],'code':[code],'cv_close':[cv_close], 'cv_amount':[cv_amount]}))

    df_rtn = df_rtn.reset_index().drop('index',axis=1)
    print(finlib.Finlib().pprint(df_rtn.sort_values(by='cv_close',ascending=False)))



def result_effort_ratio(csv_o):

    def _apply_func(tmp_df):
        logging.info(tmp_df.iloc[0]['name'])
        df=copy.copy(tmp_df)
        df = df.tail(10)

        #only check incrase days vol/price relationship.
        df = df[df['pct_chg'] > 1]
        df = df[df['pct_chg']<9.8]
        df = df[df['close']>1.01*df['open']]
        df = df.reset_index().drop('index',axis=1)

        if df.__len__()<2:
            return(pd.DataFrame())

        df['inc'] = round(100 * (df['close'] - df['open']) / df['open'], 2)  # inday inc

        df['date_1'] = df['date'].shift(1)
        df['inc_1'] = df['inc'].shift(1)
        df['pct_chg_1'] = df['pct_chg'].shift(1)
        df['v_1'] = df['volume'].shift(1)

        # df['result_effort_ratio'] = round((df['inc'] / df['inc_1']) / (df['volume'] / df['v_1']), 2)
        df['result_effort_ratio'] = round((df['pct_chg'] / df['pct_chg_1']) / (df['volume'] / df['v_1']), 2)
        # print(finlib.Finlib().pprint(df.tail(5)))
        return(df.tail(3))



    if finlib.Finlib().is_cached(csv_o,day=1):
        dfg = pd.read_csv(csv_o)
        logging.info(__file__ + " " + "loaded price mv from" + csv_o + " len " + str(dfg.__len__()))
    else:
        df = finlib.Finlib().load_all_ag_qfq_data(days=300)
        df = finlib.Finlib().add_stock_name_to_df(df=df)
        dfg = df.groupby(by='code').apply(lambda _d: _apply_func(_d))

        dfg = dfg.fillna(np.inf)
        dfg = dfg[dfg['result_effort_ratio']!=-np.inf]
        dfg = dfg[dfg['result_effort_ratio']!= np.inf]

        dfg = finlib.Finlib().filter_days(df=dfg, date_col='date', within_days=5)

        dfg = dfg.sort_values(by="result_effort_ratio")
        dfg.to_csv(csv_o, encoding="UTF-8", index=False)
        logging.info(__file__ + " " + "saved " + csv_o + " len " + str(dfg.__len__()))

    return(dfg)



    print(1)


def xiao_hu_xian(csv_out,debug=False):
    if finlib.Finlib().is_cached(csv_out,day=1):
        logging.info("loading from "+csv_out)
        return(pd.read_csv(csv_out))


    csv_basic = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv"
    df_basic = finlib.Finlib().regular_read_csv_to_stdard_df(csv_basic)

    df_rtn = pd.DataFrame()

    ZT_P= 8 #ag_all_300_days.csv

    df = finlib.Finlib().load_all_ag_qfq_data(days=200)
    # csv_f = "/home/ryan/DATA/DAY_Global/AG_qfq/ag_all_200_days.csv"
    # df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)

    # day_n1 =  datetime.datetime.strptime(df.date.max(), "%Y%m%d")
    # day_n2 =  day_n1 - datetime.timedelta(days=1)

    day_n1 = df.date.iloc[-1]
    day_n2 = df.date.iloc[-2]

    df_n1= df[df['date']==day_n1]
    df_n2= df[df['date']==day_n2]

    df_n1=df_n1[(df_n1['pct_chg']>ZT_P) & (df_n1['pct_chg']< 20)]
    df_n2=df_n2[(df_n2['pct_chg']>ZT_P) & (df_n2['pct_chg']< 20)]

    # last_trading_day = finlib.Finlib().get_last_trading_day()
    # last_trading_day_dt = datetime.datetime.strptime(last_trading_day, "%Y%m%d")

    for c in df_n1.code.append(df_n2.code).unique():
        df_s=df[df['code']==c]

        if df_s.__len__()<90:
            continue

        if df_s.iloc[-1].pct_chg < ZT_P and df_s.iloc[-2].pct_chg < ZT_P :
            if debug:
                logging.info("skip, not hit zhangting in the latest two days "+c+" "+df_s.iloc[-1].date)
            continue

        # df_s['date_dt']=df_s['date'].apply(lambda _d: datetime.datetime.strptime(_d, "%Y%m%d"))

        df_s_ZT = df_s[df_s['pct_chg']>ZT_P]
        df_s_ZT['date_dt']=df_s_ZT['date'].apply(lambda _d: datetime.datetime.strptime(_d, "%Y%m%d"))

        if df_s_ZT.__len__()<2:
            continue

        df_s_ZT = finlib.Finlib().add_stock_name_to_df(df_s_ZT)

        n_days = (df_s_ZT.iloc[-1].date_dt - df_s_ZT.iloc[-2].date_dt).days
        if n_days > 300:
            if debug:
                logging.info("skip, two zt more than 300 days "+str(n_days))
            continue

        if n_days < 3:
            if debug:
                logging.info("skip, two zt less than 3 days "+str(n_days))
            continue

        if debug:
            logging.info("hit condition 0. ZT in last two 3 days, and previous ZT in [300,3] days. "+str(n_days))

        #include zt day
        df_since_1st_zt = df_s[df_s['date'] >= df_s_ZT.iloc[-2].date]


        #following 3 days not lower than close_of_zt
        if df_since_1st_zt[1:4]['low'].min() < df_s_ZT.iloc[-2].close:
            continue
        if debug:
            logging.info("hit condition 1. 3 days no lower than ztclose")

        if df_since_1st_zt[1:]['close'].min() < df_s_ZT.iloc[-2].close:
            continue

        if debug:
            logging.info("hit condition 2. later close no lower than ztclose")


        df_s_basic = pd.merge(left=df_since_1st_zt, right=df_basic[df_basic['code']==c], on=['code','date'],how='inner')
        df_s_basic = df_s_basic[['code','date','pct_chg','turnover_rate','turnover_rate_f','volume_ratio']]

        sum_tv_4_days = round(df_s_basic.head(4)['turnover_rate_f'].sum(),2) #换手率（自由流通股）

        if sum_tv_4_days > 30:
            continue

        if debug:
            logging.info("hit condition 3. zt_day+following3days turnover <30. " + str(sum_tv_4_days))

        sum_tv_90_days = round(df_s_basic.head(90)['turnover_rate'].sum(),2)
        sum_tv_all_days = round(df_s_basic['turnover_rate'].sum(),2)
        sum_tv_all_days_avg = round(df_s_basic['turnover_rate'].mean(),2)

        df_rtn = df_rtn.append({
            "code": c,
            "name": df_s_ZT.iloc[-2]['name'],
            "dateS": df_s_ZT.iloc[-2]['date'],
            "dateE": df_s_ZT.iloc[-1]['date'],
            "ndays": n_days,
            "last4D_tv":sum_tv_4_days,
        }, ignore_index=True)

        logging.info(c+" "+df_s_ZT.iloc[-2]['name']+", ZhangTing, "+df_s_ZT.iloc[-2]['date']+" --> "+df_s_ZT.iloc[-1]['date']+", "+ str(n_days)+" days, last 4Days turnover sum "+ str(sum_tv_4_days))

    if df_rtn.empty:
        logging.info("xiao hu xian, no hit")
    else:
        df_rtn = finlib.Finlib().add_industry_to_df(df=df_rtn,source='wg')
        df_rtn = finlib.Finlib().add_amount_mktcap(df=df_rtn, mktcap_unit='100M')
        df_rtn = finlib.Finlib().add_tr_pe(df=df_rtn, df_daily=finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day()), df_ts_all=finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years()))
        df_rtn = finlib.Finlib().add_stock_increase(df=df_rtn)

    logging.info(finlib.Finlib().pprint(df_rtn))
    df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
    logging.info("xiao hu xian result saved to "+csv_out)

    return(df_rtn)


def bar_support_resist_strategy(csv_out,debug=False):
    if finlib.Finlib().is_cached(csv_out,day=1):
        logging.info("loading from "+csv_out)
        return(pd.read_csv(csv_out))


    df_rtn = pd.DataFrame()

    bar_up= 0
    bar_dn = 0

    df = finlib.Finlib().load_all_ag_qfq_data(days=300)

    for c in df.code.append(df.code).unique():
        c='SH600519'
        dfs=df[df['code']==c].reset_index().drop('index',axis=1)

        if dfs.__len__()<90:
            continue

        df_m = finlib.Finlib().daily_to_monthly_bar(df_daily=dfs)['df_monthly']
        df_w = finlib.Finlib().daily_to_monthly_bar(df_daily=dfs)['df_weekly']

        df_m['bar_mid'] = df_m['open']+(df_m['close']-df_m['open'])*0.5

        df_m = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_m,short=5,middle=10,long=20) #close_10_sma
        df_w = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_w,short=5,middle=10,long=20) #close_5_sma

        #Weekly Bar Checks
        df_w1=df_w[['date','code','close','pre_close','close_5_sma']]
        _, df_c_cross_dn_maw5, df_c_cross_up_maw5  = finlib_indicator.Finlib_indicator().slow_fast_across(df=df_w1,fast_col_name='close',slow_col_name='close_5_sma')


        ## start
        bar_up = 0; bar_dn=0; pct_gain = 0; pct_lost = 0;
        l_spt =0; l_spt_mid=0; l_rst=0;

        for index,row in df_w.tail(26).iterrows():
            print(row['date'])

            if row['pct_chg']>0 and row['open']<row['close']:
                l_spt = row['open']

                if bar_up == 0:
                    bar_up += 1 

                if bar_up > 0:
                    bar_up +=1

                if bar_dn >0: #revert. dn to up
                    bar_dn =0
                    bar_up +=1

            if row['pct_chg']<0 and row['open']>row['close']:
                bar_dn +=1
                l_rst= row['open']






        df_rtn = df_rtn.append({
            "code": c,
        }, ignore_index=True)


    df_rtn = finlib.Finlib().add_industry_to_df(df=df_rtn,source='wg')

    logging.info(finlib.Finlib().pprint(df_rtn))
    df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
    logging.info("bar support resist stratgy result saved to "+csv_out)

    return(df_rtn)


def daily_UD_tongji(out_csv,ndays=1):
    df_rtn = pd.DataFrame()

    if finlib.Finlib().is_cached(out_csv, day=ndays):
        logging.info("loading from " + out_csv)
        df_rtn = pd.read_csv(out_csv)
        return(df_rtn)


    today =  finlib.Finlib().get_last_trading_day()
    today = datetime.datetime.strptime(today, '%Y%m%d')
    theday = today - datetime.timedelta(days=ndays)
    theday = int(theday.strftime('%Y%m%d'))
    today = int(today.strftime('%Y%m%d'))


    pro = ts.pro_api(token="4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b", timeout=3)

    # limit_list: 每日涨跌停统计
    df_rtn = pro.limit_list(start_date=str(theday), end_date=str(today))
    df_rtn = finlib.Finlib().ts_code_to_code(df_rtn)
    df_rtn = finlib.Finlib().filter_days(df=df_rtn, date_col='trade_date', within_days=5)
    df_rtn.to_csv(out_csv,encoding='UTF-8', index=False)
    logging.info(f"UD_tongji result csv saved to {out_csv}")

    df_D = df_rtn[df_rtn['limit']=='D']
    df_U = df_rtn[df_rtn['limit']=='U']

    # fl_ratio: 封单手数/流通股本
    logging.info("Down Limit, by fl_ratio\n"+finlib.Finlib().pprint(df_D.sort_values(by='fl_ratio').tail(10)))
    logging.info("UP Limit, by fl_ratio\n"+finlib.Finlib().pprint(df_U.sort_values(by='fl_ratio').tail(10)))
    # logging.info(finlib.Finlib().pprint(df_rtn[['code','name','fc_ratio','fl_ratio','first_time','last_time']]))
    logging.info("end of daily_UD_tongji\n\n")
    return(df_rtn)

def _td_setup_9_consecutive_close_4_day_lookup(adf,pre_n_day=4,consec_day=9):
    # https://oxfordstrat.com/indicators/td-sequential-3/
    adf = adf.reset_index().drop('index', axis=1)
    # pre_n_day=4
    adf['anno_setup']=""
    adf['last_completed_stg1_anno']=""
    adf['last_completed_stg1_start_date']=""
    adf['last_completed_stg1_end_date']=""
    adf['last_completed_stg1_close']=""

    adf['close_b4']=adf['close'].shift(periods=pre_n_day)
    adf['close_gt_pre_4d'] = adf['close']-adf['close'].shift(periods=pre_n_day)>0
    adf['close_gt_pre_4d_-1']=adf['close_gt_pre_4d'].shift(periods=1)

    adf.loc[(adf['close_gt_pre_4d']==True) & (adf['close_gt_pre_4d_-1']==False),['anno_setup']]="UP_D1_of_"+str(consec_day)
    adf.loc[(adf['close_gt_pre_4d']==False) & (adf['close_gt_pre_4d_-1']==True),['anno_setup']]="DN_D1_of_"+str(consec_day)

    adf = adf.drop(columns=['close_gt_pre_4d','close_gt_pre_4d_-1'])



    pre_anno_setup=''
    dn_cnt = 0
    up_cnt = 0
    last_completed_stg1_anno = ''
    last_completed_stg1_high=''
    last_completed_stg1_low=''
    last_completed_stg1_open =''
    last_completed_stg1_close =''
    last_completed_stg1_start_date =''
    last_completed_stg1_end_date = ''
    last_completed_stg1_perfect =''


    adf['C_DN_DAYS_B4']=0
    adf['C_UP_DAYS_B4']=0

    adf['df_setup_index'] = -1
    df_setup = pd.DataFrame(columns=[
        'code','df_setup_index','completed_or_not','bar_length',
        'start_date','dir_up_or_dn',
        'end_date','high','open','low','close',
        'bar_1','bar_2','bar_3','bar_4','bar_5',
        'bar_6','bar_7','bar_8','bar_9','bar_10',
        ])

    df_setup_index = 0
    setup_dir = 'NO'


    for index, row in adf.iterrows():
        date=row['date']
        close=row['close']
        close_b4=row['close_b4']
        anno_setup=row['anno_setup']

        if anno_setup=='DN_D1_of_'+str(consec_day):
            setup_dir='DN'
            dn_cnt = 1

            adf.at[index, 'C_DN_DAYS_B4'] = dn_cnt

            adf.at[index, 'last_completed_stg1_anno'] = last_completed_stg1_anno
            adf.at[index, 'last_completed_stg1_high'] = last_completed_stg1_high
            adf.at[index, 'last_completed_stg1_low'] = last_completed_stg1_low
            adf.at[index, 'last_completed_stg1_open'] = last_completed_stg1_open
            adf.at[index, 'last_completed_stg1_close'] = last_completed_stg1_close
            adf.at[index, 'last_completed_stg1_start_date'] = last_completed_stg1_start_date
            adf.at[index, 'last_completed_stg1_end_date'] = last_completed_stg1_end_date
            adf.at[index, 'last_completed_stg1_perfect'] = last_completed_stg1_perfect

            continue
        elif anno_setup=='UP_D1_of_'+str(consec_day):
            setup_dir='UP'
            up_cnt = 1
            adf.at[index, 'C_UP_DAYS_B4'] = up_cnt

            adf.at[index, 'last_completed_stg1_anno'] = last_completed_stg1_anno
            adf.at[index, 'last_completed_stg1_high'] = last_completed_stg1_high
            adf.at[index, 'last_completed_stg1_low'] = last_completed_stg1_low
            adf.at[index, 'last_completed_stg1_open'] = last_completed_stg1_open
            adf.at[index, 'last_completed_stg1_close'] = last_completed_stg1_close
            adf.at[index, 'last_completed_stg1_start_date'] = last_completed_stg1_start_date
            adf.at[index, 'last_completed_stg1_end_date'] = last_completed_stg1_end_date
            adf.at[index, 'last_completed_stg1_perfect'] = last_completed_stg1_perfect

            continue

        if setup_dir=='DN':
            if close < close_b4:
                dn_cnt+=1
                adf.at[index,'anno_setup']='DN_D'+str(dn_cnt)+"_of_"+str(consec_day)
                adf.at[index, 'C_DN_DAYS_B4'] = dn_cnt

                if dn_cnt == consec_day:
                    df_setup = adf.iloc[index-consec_day+1:index+1]
                    last_completed_stg1_high = df_setup['high'].max()
                    last_completed_stg1_low = df_setup['low'].min()
                    last_completed_stg1_open = df_setup.head(1)['open'].values[0]
                    last_completed_stg1_close = df_setup.tail(1)['close'].values[0]
                    last_completed_stg1_start_date = df_setup.head(1)['date'].values[0]
                    last_completed_stg1_end_date = df_setup.tail(1)['date'].values[0]
                    last_completed_stg1_anno = df_setup.tail(1)['anno_setup'].values[0]

                    last_completed_stg1_perfect = ''
                    if adf.at[index-1,'low'] < adf.at[index-2,'low'] and adf.at[index-1,'low'] < adf.at[index-3,'low']:
                        if adf.at[index,'low'] < adf.at[index-2,'low'] and adf.at[index,'low'] < adf.at[index-3,'low']:
                            last_completed_stg1_perfect='True'


            else:
                # print("Down breaked")
                adf.at[index,'anno_setup'] = 'DN_Break_At_'+str(dn_cnt)
                pre_anno_setup=''
                dn_cnt=0
                adf.at[index, 'C_DN_DAYS_B4'] = dn_cnt
                df_setup_index += 1


        if setup_dir=='UP' :
            if close > close_b4:
                up_cnt+=1
                adf.at[index, 'anno_setup']='UP_D'+str(up_cnt)+"_of_"+str(consec_day)
                adf.at[index, 'C_UP_DAYS_B4']=up_cnt

                # if up_cnt >=9:
                if up_cnt ==consec_day:
                    df_setup = adf.iloc[index-consec_day+1:index+1]
                    last_completed_stg1_high = df_setup['high'].max()
                    last_completed_stg1_low = df_setup['low'].min()
                    last_completed_stg1_open = df_setup.head(1)['open'].values[0]
                    last_completed_stg1_close = df_setup.tail(1)['close'].values[0]
                    last_completed_stg1_start_date = df_setup.head(1)['date'].values[0]
                    last_completed_stg1_end_date = df_setup.tail(1)['date'].values[0]
                    last_completed_stg1_anno = df_setup.tail(1)['anno_setup'].values[0]

                    last_completed_stg1_perfect = ''
                    if adf.at[index - 1, 'high'] > adf.at[index - 2, 'high'] and adf.at[index - 1, 'high'] > adf.at[
                        index - 3, 'high']:
                        if adf.at[index, 'high'] > adf.at[index - 2, 'high'] and adf.at[index, 'high'] > adf.at[
                            index - 3, 'high']:
                            last_completed_stg1_perfect = 'True'

            else:
                # print("Up breaked")
                adf.at[index, 'anno_setup'] = 'UP_Break_At_'+str(up_cnt)
                pre_anno_setup=''
                up_cnt=0
                adf.at[index, 'C_UP_DAYS_B4'] = up_cnt
                df_setup_index += 1

        adf.at[index, 'last_completed_stg1_anno'] = last_completed_stg1_anno
        adf.at[index, 'last_completed_stg1_high'] = last_completed_stg1_high
        adf.at[index, 'last_completed_stg1_low'] = last_completed_stg1_low
        adf.at[index, 'last_completed_stg1_open'] = last_completed_stg1_open
        adf.at[index, 'last_completed_stg1_close'] = last_completed_stg1_close
        adf.at[index, 'last_completed_stg1_start_date'] = last_completed_stg1_start_date
        adf.at[index, 'last_completed_stg1_end_date'] = last_completed_stg1_end_date
        adf.at[index, 'last_completed_stg1_perfect'] = last_completed_stg1_perfect

    return(adf)

def _td_countdown_13_day_lookup(adf,cancle_countdown = True):
    # https://oxfordstrat.com/indicators/td-sequential-3/
    pre_n_day=2
    '''
    Countdown Cancellation
    Long Trades: Developing countdown is canceled when: (a)  The price action rallies and generates a sell setup
    cancle_countdown:
    
    '''

    adf['anno_stg2']=""
    adf['anno_bar10']=""
    adf['low_b2']=adf['low'].shift(periods=2)
    adf['low_b1']=adf['low'].shift(periods=1)
    adf['high_b1']=adf['high'].shift(periods=1)
    adf['high_b2']=adf['high'].shift(periods=2)
    adf['close_b1']=adf['close'].shift(periods=1)

    pre_anno_setup=''
    dn_cnt = 0
    up_cnt = 0

    adf['Stage2_DN_DAYS_13']=0
    adf['Stage2_UP_DAYS_13']=0
    adf['bars_after_setup']=0
    setup_bar_index = 0
    up_13_dict = {}
    dn_13_dict = {}

    for index, row in adf.iterrows():
        code = row['code']
        date=row['date']
        close=row['close']
        low_b2=row['low_b2']
        low_b1=row['low_b1']
        high_b1=row['high_b1']
        high_b2=row['high_b2']
        low=row['low']
        high=row['high']
        close_b1=row['close_b1']
        anno_setup=row['anno_setup']

        if row['C_DN_DAYS_B4']==9:
            if pre_anno_setup == 'UP_D9_of_9':
                adf.at[index,'anno_stg2'] = 'UP_cancelled_by_new_DN'
            dn_cnt = 0
            up_cnt = 0
            up_13_dict = {}
            dn_13_dict = {}
            setup_bar_index = index
            pre_anno_setup = 'DN_D9_of_9'
            # continue

        if row['C_UP_DAYS_B4']==9:
            if pre_anno_setup == 'DN_D9_of_9':
                adf.at[index,'anno_stg2'] = 'DN_cancelled_by_new_UP'
            dn_cnt = 0
            up_cnt = 0
            up_13_dict = {}
            dn_13_dict = {}
            setup_bar_index = index
            pre_anno_setup = 'UP_D9_of_9'
            # continue

        adf.at[index, 'setup_bar_index'] = setup_bar_index

        bars_after_setup = index - setup_bar_index
        adf.at[index, 'bars_after_setup'] = bars_after_setup

        if setup_bar_index > 0 and bars_after_setup > 13*5 and pre_anno_setup !='':
            pre_anno_setup = ''
            # logging.info(f"code {code},{date} too far from setupbar, no trending present.{str(bars_after_setup)}  "
            #              + f" ann_setup {anno_setup} pre_anno_setup {pre_anno_setup}")

        if setup_bar_index > 0 and bars_after_setup == 4 and pre_anno_setup=='DN_D9_of_9':
            if close < adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_start_DN_13_CD'
                # logging.info(f"{code} {date} {close} {anno_bar10}.")
                adf.at[index, 'anno_bar10'] = anno_bar10
            elif close > adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_reverse_9_dn_to_up'
                # logging.info(f"{code} {date} {close} {anno_bar10}.")
                adf.at[index, 'anno_bar10'] = anno_bar10


        if setup_bar_index > 0 and bars_after_setup == 4 and pre_anno_setup=='UP_D9_of_9':
            if close < adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_start_reverse_9_up_to_dn'
                # logging.info(f"{code} {date} {close} {anno_bar10}.")
                adf.at[index, 'anno_bar10'] = anno_bar10
            elif close > adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_start_UP_13_CD'
                # logging.info(f"{code} {date} {close} {anno_bar10}.")
                adf.at[index, 'anno_bar10'] = anno_bar10

        if pre_anno_setup=='DN_D9_of_9':
            if min(low,close_b1) > row['last_completed_stg1_high'] and cancle_countdown:
                adf.at[index, 'anno_stg2'] = 'DN_cancelled_by_p_break_up_setup'
                pre_anno_setup=''
                dn_cnt=0
                continue

            if row['C_DN_DAYS_B4'] >= 18:
                adf.at[index, 'anno_stg2'] = 'DN_cancelled_by_setup_exceed_18D'
                pre_anno_setup = ''
                dn_cnt = 0
                continue

            if close <= low_b2:
                dn_cnt+=1
                adf.at[index,'anno_stg2']='DN_D'+ str(dn_cnt)+"_of_13"
                adf.at[index, 'Stage2_DN_DAYS_13'] = dn_cnt

                dn_13_dict[dn_cnt] = row

                if dn_cnt == 13:
                    # same as dn_13_dict[8]['close']
                    if low <= adf[adf['anno_stg2']=='DN_D8_of_13'].iloc[-1].close:
                        p_str=f"LONG CONDITION Meet! bars_after_setup {str(bars_after_setup)}"
                        adf.at[index, 'anno_stg2'] = p_str
                        logging.info(p_str)
                    else:
                        adf.at[index, 'anno_stg2'] ="Bar 13 is deferred, low > DN_D8_of_13 close"
            else:
                adf.at[index,'anno_stg2'] = 'DN_D'+str(dn_cnt)+'_of_13_pass'
                adf.at[index, 'Stage2_DN_DAYS_13'] = dn_cnt
                #

        if pre_anno_setup=='UP_D9_of_9' :
            if  max(high, close_b1) <= row['last_completed_stg1_low'] and cancle_countdown:
                adf.at[index, 'anno_stg2'] = 'UP_cancelled_by_p_break_dn_setup'
                pre_anno_setup=''
                up_cnt=0
                continue

            if row['C_UP_DAYS_B4'] >= 18:
                adf.at[index, 'anno_stg2'] = 'UP_cancelled_by_setup_exceed_18D'
                pre_anno_setup = ''
                up_cnt = 0
                continue

            if close >= high_b2:
                up_cnt+=1
                adf.at[index, 'anno_stg2']='UP_D'+str(up_cnt)+"_of_13"
                adf.at[index, 'Stage2_UP_DAYS_13']=up_cnt

                up_13_dict[dn_cnt] = row

                if up_cnt == 13:
                    if high >= adf[adf['anno_stg2']=='UP_D8_of_13'].iloc[-1].close:
                        p_str=f"SHORT CONDITION Meet! bars_after_setup {str(bars_after_setup)}"
                        adf.at[index, 'anno_stg2'] = p_str
                        logging.info(p_str)
                    else:
                        adf.at[index, 'anno_stg2'] ="Bar 13 short is deferred, high < DN_D8_of_13 close"


            else:
                adf.at[index, 'anno_stg2'] = 'UP_D'+str(dn_cnt)+'_of_13_pass'
                adf.at[index, 'Stage2_UP_DAYS_13'] = up_cnt


    # print(finlib.Finlib().pprint(adf))
    # adf[adf['Stage2_DN_DAYS_13']>=13]
    # adf[adf['Stage2_UP_DAYS_13']>=13]
    collist=['code', 'date', 'close', 'anno_setup', 'anno_stg2','anno_bar10',
            'last_completed_stg1_anno','last_completed_stg1_perfect', 'last_completed_stg1_end_date',
            'last_completed_stg1_close',
             'C_DN_DAYS_B4','C_UP_DAYS_B4',
             'Stage2_DN_DAYS_13','Stage2_UP_DAYS_13'
             ]

    # adf = finlib.Finlib().adjust_column(df=adf, col_name_list=collist)
    adf = adf[collist]
    return(adf)

def _td_oper(adf):
    # https://oxfordstrat.com/indicators/td-sequential-3/
    consective_op_cnt=1

    adf['anno_oper']=""

    if 'close_b4' not in adf.columns:
        adf['close_b4']=adf['close'].shift(periods=4)

    LONG_COND = False
    SHORT_COND = False

    oper_cnt_long =0
    oper_cnt_short =0
    for index, row in adf.iterrows():
        code = row['code']
        date=row['date']
        close=row['close']
        close_b4=row['close_b4']
        anno_stg2=row['anno_stg2']
        anno_setup=row['anno_setup']


        if anno_stg2.find("LONG CONDITION Meet")>-1 :
            LONG_COND=True
            SHORT_COND = False
            oper_cnt_long = 0
            # continue
        if anno_stg2.find("SHORT CONDITION Meet")>-1 :
            SHORT_COND=True
            LONG_COND=False
            oper_cnt_short = 0
            # continue

        if LONG_COND and close > close_b4 and oper_cnt_long < consective_op_cnt:
            # pstr=f'OPER_LONG {code} at {str(close)} on {date}, anno_setup {anno_setup}, anno_stg2 {anno_stg2}'
            pstr=f'OPER_LONG {code} at {str(close)} on {date}'
            adf.at[index, 'anno_oper'] = pstr
            print(pstr)
            oper_cnt_long +=1
            oper_cnt_short = 0


        if SHORT_COND and close < close_b4 and oper_cnt_short < consective_op_cnt:
            # pstr=f'OPER_SHORT {code} at {str(close)} on {date}, anno_setup {anno_setup}, anno_stg2 {anno_stg2}'
            pstr=f'OPER_SHORT {code} at {str(close)} on {date}'
            adf.at[index, 'anno_oper'] = pstr
            print(pstr)
            oper_cnt_short += 1
            oper_cnt_long = 0

    rtn_op_df = adf[(adf['anno_oper'].str.contains('OPER_LONG')) | (adf['anno_oper'].str.contains('OPER_SHORT')) ]
    rtn_op_df = rtn_op_df[['code', 'date', 'close','anno_oper','anno_stg2', 'anno_setup', 'last_completed_stg1_anno', 'last_completed_stg1_perfect', 'last_completed_stg1_end_date', 'last_completed_stg1_close', 'C_DN_DAYS_B4', 'C_UP_DAYS_B4', 'Stage2_DN_DAYS_13', 'Stage2_UP_DAYS_13']].reset_index().drop('index',axis=1)

    rtn_9_13_df = adf[(adf['anno_stg2'].str.contains('SHORT CONDITION Meet')) | (adf['anno_stg2'].str.contains('LONG CONDITION Meet')) ]
    rtn_9_13_df = rtn_9_13_df[['code', 'date', 'close', 'anno_oper', 'anno_stg2', 'anno_setup', 'last_completed_stg1_anno', 'last_completed_stg1_perfect', 'last_completed_stg1_end_date', 'last_completed_stg1_close', 'C_DN_DAYS_B4', 'C_UP_DAYS_B4', 'Stage2_DN_DAYS_13', 'Stage2_UP_DAYS_13']].reset_index().drop('index',axis=1)
    return(rtn_9_13_df, rtn_op_df)


def _td_setup_reverse(df_setup, debug=False):
    df_setup_u2d=pd.DataFrame()
    df_setup_d2u=pd.DataFrame()
    #
    # df_setup = df_setup[['code','name','date','close', 'anno_setup', 'last_completed_stg1_anno',
    #                      'last_completed_stg1_high', 'last_completed_stg1_low']]

    # UP to DOWN reverse
    for index, row in df_setup[df_setup['anno_setup']=='UP_D9_of_9'].iterrows():
        # if (row['last_completed_stg1_close']-row['last_completed_stg1_open'])/row['last_completed_stg1_open']<0.2:
        #     continue

        df_3_days = df_setup[index+1:index+4]
        df_3_days = df_3_days[df_3_days['close'] *0.7 <= row['last_completed_stg1_low']]
        df_3_days['bar_body'] = round(100 * (df_3_days['close'] - df_3_days['open']) / df_3_days['open'], 1)
        df_3_days = df_3_days[df_3_days['bar_body'] < -5]

        df_setup_u2d = df_setup_u2d.append(df_3_days)


    if debug and df_setup_u2d.__len__()>0:
        logging.info("TD setup up to down reverse:")
        logging.info(finlib.Finlib().pprint(df_setup_u2d))

    ### DOWN to UP reverse
    for index, row in df_setup[df_setup['anno_setup']=='DN_D9_of_9'].iterrows():
        # if (row['last_completed_stg1_close']-row['last_completed_stg1_open'])/row['last_completed_stg1_open']>-0.2:
        #     continue

        df_3_days = df_setup[index+1:index+4]
        df_3_days = df_3_days[df_3_days['close'] >= 0.7 * row['last_completed_stg1_high']]

        df_3_days['bar_body']= round(100*(df_3_days['close']-df_3_days['open'])/df_3_days['open'],1)
        df_3_days = df_3_days[df_3_days['bar_body'] > 5]

        df_setup_d2u = df_setup_d2u.append(df_3_days)

    if debug and df_setup_d2u.__len__()>0:
        logging.info("TD setup down to up reverse:")
        logging.info(finlib.Finlib().pprint(df_setup_d2u))

    return(df_setup_d2u, df_setup_u2d)


def td_indicator(df,pre_n_day,consec_day):
    df_setup = _td_setup_9_consecutive_close_4_day_lookup(df,pre_n_day,consec_day)
    df_setup = finlib.Finlib().add_stock_name_to_df(df_setup)

    df_setup_d2u, df_setup_u2d = _td_setup_reverse(df_setup)

    df_countdown = _td_countdown_13_day_lookup(df_setup,cancle_countdown=True)
    df_9_13, df_op = _td_oper(df_countdown)
    return(df_9_13, df_op, df_setup_d2u, df_setup_u2d , df_countdown.tail(1))


#shang zheng zong zhi
def TD_szzz_index(rst_dir,pre_n_day,consec_day):
    df_index=finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv')[-300:]
    df_9_13, df_op, df_setup_d2u, df_setup_u2d,df_today = td_indicator(df_index,pre_n_day,consec_day)
    df_9_13.to_csv(rst_dir+"/szzz_9_13.csv", encoding='UTF-8', index=False)

    df_setup_d2u.to_csv(rst_dir+"/szzz_d2u.csv", encoding='UTF-8', index=False)
    df_setup_u2d.to_csv(rst_dir+"/szzz_u2d.csv", encoding='UTF-8', index=False)

    df_op.to_csv(rst_dir+"/szzz_op.csv", encoding='UTF-8', index=False)
    df_today.to_csv(rst_dir+"/szzz_today.csv", encoding='UTF-8', index=False)


    logging.info("SH000001 INDEX 9_13: \n"+finlib.Finlib().pprint(df_9_13))
    logging.info("SH000001 INDEX Operation: \n"+finlib.Finlib().pprint(df_op))
    logging.info("SH000001 INDEX d2u: \n"+finlib.Finlib().pprint(df_setup_d2u))
    logging.info("SH000001 INDEX u2d: \n"+finlib.Finlib().pprint(df_setup_u2d))


def TD_stocks(rst_dir,pre_n_day,consec_day,stock_global=None, no_garbage=False):
    rtn_9_13 = pd.DataFrame()
    rtn_op = pd.DataFrame()
    rtn_today = pd.DataFrame()
    rtn_setup_d2u = pd.DataFrame()
    rtn_setup_u2d = pd.DataFrame()

    if stock_global is not None:
        rst_dir = rst_dir+"/"+str(stock_global)
        if not os.path.isdir(rst_dir):
            os.mkdir(rst_dir)

        df_hold = finlib.Finlib().remove_market_from_tscode(
            finlib.Finlib().get_stock_configuration(selected=True, stock_global=stock_global)['stock_list'])
        df_hold = finlib.Finlib().add_market_to_code(df=df_hold)

    td_csv_9_13 = rst_dir+"/"+"9_13.csv"
    td_csv_op = rst_dir+"/"+"op.csv"
    td_csv_today = rst_dir+"/"+"today.csv"
    td_csv_setup_d2u=rst_dir+"/"+"setup_d2u.csv"
    td_csv_setup_u2d=rst_dir+"/"+"setup_u2d.csv"

    if finlib.Finlib().is_cached(td_csv_9_13):
        logging.info("result csv has been updated in 1 days. "+td_csv_9_13)
        df_rtn = pd.read_csv(td_csv_9_13)
        return(df_rtn)

    df = finlib.Finlib().load_all_ag_qfq_data(days=300)

    if stock_global is not None:
        df = pd.merge(left=df, right=df_hold[['code']], on='code',how='inner').reset_index().drop('index', axis=1)

    if no_garbage:
        df = finlib.Finlib()._remove_garbage_must(df)

    for code in df['code'].unique():
        # logging.info(f"code {code}")
        adf = df[df['code']==code][['code','date','close','high', 'open', 'low']]
        df_9_13, df_op,df_setup_d2u, df_setup_u2d, df_today = td_indicator(adf,pre_n_day,consec_day)

        rtn_9_13 = rtn_9_13.append(df_9_13).reset_index().drop('index',axis=1)

        rtn_op = rtn_op.append(df_op).reset_index().drop('index',axis=1)
        rtn_today = rtn_today.append(df_today).reset_index().drop('index',axis=1)
        rtn_setup_d2u = rtn_setup_d2u.append(df_setup_d2u).reset_index().drop('index',axis=1)
        rtn_setup_u2d = rtn_setup_u2d.append(df_setup_u2d).reset_index().drop('index',axis=1)

    rtn_9_13 = finlib.Finlib().filter_days(df=rtn_9_13, date_col='date', within_days=5)
    rtn_op = finlib.Finlib().filter_days(df=rtn_op, date_col='date', within_days=5)
    rtn_today = finlib.Finlib().filter_days(df=rtn_today, date_col='date', within_days=5)
    rtn_setup_d2u = finlib.Finlib().filter_days(df=rtn_setup_d2u, date_col='date', within_days=5)
    rtn_setup_u2d = finlib.Finlib().filter_days(df=rtn_setup_u2d, date_col='date', within_days=5)


    finlib.Finlib().add_stock_name_to_df(df=rtn_9_13).to_csv(td_csv_9_13, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_op).to_csv(td_csv_op, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_today).to_csv(td_csv_today, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_setup_d2u).to_csv(td_csv_setup_d2u, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_setup_u2d).to_csv(td_csv_setup_u2d, encoding='UTF-8', index=False)

    print(f"result saved to \n{td_csv_today}\n{td_csv_op}\n{td_csv_9_13}\n{td_csv_setup_d2u}\n{td_csv_setup_u2d}")
    return(rtn_9_13)

def TD_indicator_start():
    rst_dir="/home/ryan/DATA/result/TD_Indicator"
    if not os.path.isdir(rst_dir):
        os.mkdir(rst_dir)

    pre_n_day = 4
    consec_day = 9
    logging.info(f"\n========== TD SZZS SZZZ SH000001 INDEX=============\n")
    TD_szzz_index(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day)

    logging.info(f"\n========== TD AG_HOLD=============\n")
    TD_stocks(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day, stock_global='AG_HOLD')

    logging.info(f"\n========== TD Stocks=============\n")
    df_rtn = TD_stocks(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day,no_garbage=False)
    return(df_rtn)


def bk_TD_stocks(rst_dir,pre_n_day,consec_day,stock_global=None, no_garbage=False):
    rtn_9_13 = pd.DataFrame()
    rtn_op = pd.DataFrame()
    rtn_today = pd.DataFrame()
    rtn_setup_d2u = pd.DataFrame()
    rtn_setup_u2d = pd.DataFrame()


    td_csv_9_13 = rst_dir+"/"+"9_13.csv"
    td_csv_op = rst_dir+"/"+"op.csv"
    td_csv_today = rst_dir+"/"+"today.csv"
    td_csv_setup_d2u=rst_dir+"/"+"setup_d2u.csv"
    td_csv_setup_u2d=rst_dir+"/"+"setup_u2d.csv"

    if finlib.Finlib().is_cached(td_csv_9_13):
        logging.info("result csv has been updated in 1 days. "+td_csv_9_13)
        df_rtn = pd.read_csv(td_csv_9_13)
        return(df_rtn)

    df = finlib.Finlib().load_all_bk_qfq_data(days=300)

    # for code in df['code'].unique()[:2]:#debug
    for code in df['code'].unique():
        # logging.info(f"code {code}")
        adf = df[df['code']==code][['code','date','close','high', 'open', 'low']]
        df_9_13, df_op,df_setup_d2u, df_setup_u2d, df_today = td_indicator(adf,pre_n_day,consec_day)

        rtn_9_13 = rtn_9_13.append(df_9_13).reset_index().drop('index',axis=1)

        rtn_op = rtn_op.append(df_op).reset_index().drop('index',axis=1)
        rtn_today = rtn_today.append(df_today).reset_index().drop('index',axis=1)
        rtn_setup_d2u = rtn_setup_d2u.append(df_setup_d2u).reset_index().drop('index',axis=1)
        rtn_setup_u2d = rtn_setup_u2d.append(df_setup_u2d).reset_index().drop('index',axis=1)

    rtn_9_13 = finlib.Finlib().filter_days(df=rtn_9_13, date_col='date', within_days=5)
    rtn_op = finlib.Finlib().filter_days(df=rtn_op, date_col='date', within_days=5)
    rtn_today = finlib.Finlib().filter_days(df=rtn_today, date_col='date', within_days=5)
    rtn_setup_d2u = finlib.Finlib().filter_days(df=rtn_setup_d2u, date_col='date', within_days=5)
    rtn_setup_u2d = finlib.Finlib().filter_days(df=rtn_setup_u2d, date_col='date', within_days=5)


    finlib.Finlib().add_stock_name_to_df(df=rtn_9_13).to_csv(td_csv_9_13, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_op).to_csv(td_csv_op, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_today).to_csv(td_csv_today, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_setup_d2u).to_csv(td_csv_setup_d2u, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_setup_u2d).to_csv(td_csv_setup_u2d, encoding='UTF-8', index=False)

    print(f"result saved to \n{td_csv_today}\n{td_csv_op}\n{td_csv_9_13}\n{td_csv_setup_d2u}\n{td_csv_setup_u2d}")
    return(rtn_9_13)

def bk_ma_across(csv_o_b, csv_o_s):
    df_rtn_jincha = pd.DataFrame()
    df_rtn_sicha = pd.DataFrame()

    if finlib.Finlib().is_cached(csv_o_b) and  finlib.Finlib().is_cached(csv_o_s)  :
        logging.info("result csv has been updated in 1 days. "+csv_o_b)
        logging.info("result csv has been updated in 1 days. "+csv_o_s)
        df_rtn_jincha = pd.read_csv(csv_o_b)
        df_rtn_sicha = pd.read_csv(csv_o_s)

        df_rtn_jincha = finlib.Finlib().filter_days(df_rtn_jincha, date_col='date', within_days=5)
        df_rtn_sicha = finlib.Finlib().filter_days(df_rtn_sicha, date_col='date', within_days=5)

        logging.info("=== BK JinCha ===\n"+finlib.Finlib().pprint(df_rtn_jincha.head(5)))
        logging.info("=== BK SiCha ===\n"+finlib.Finlib().pprint(df_rtn_sicha.head(5)))
        return(df_rtn_jincha,df_rtn_sicha)

    df = finlib.Finlib().load_all_bk_qfq_data(days=300)

    # for code in df['code'].unique()[:2]:#debug
    for code in df['code'].unique():
        logging.info(f"code {code}")
        adf = df[df['code']==code][['code','date','close']]
        adf = finlib_indicator.Finlib_indicator().add_ma_ema(df=adf,short=4,middle=27,long=60)
        adf = adf[['code','date','close','close_4_sma','close_27_sma']]

        adf, df_si_cha, df_jin_cha  = finlib_indicator.Finlib_indicator().slow_fast_across(df=adf,fast_col_name='close_4_sma',slow_col_name='close_27_sma')
        df_rtn_sicha = df_rtn_sicha.append(df_si_cha)
        df_rtn_jincha = df_rtn_jincha.append(df_jin_cha)


    df_rtn_sicha.to_csv(csv_o_s, encoding='UTF-8', index=False)
    df_rtn_jincha.to_csv(csv_o_b, encoding='UTF-8', index=False)
    print(f"result saved to {csv_o_b}, {csv_o_s}")
    return(df_rtn_jincha,df_rtn_sicha)

def bk_increase(csv_o,ndays=3,dayS=None, dayE=None):
    dayS, dayE, ndays = finlib.Finlib().get_dayS_dayE_ndays(ndays=ndays, dayS=dayS, dayE=dayE)

    # if start != None and ndays != None:
    #     csv_o = f"{csv_o}_{str(start)}_{str(ndays)}.csv"
    #
    # if start == None and ndays != None:
    #     csv_o = f"{csv_o}_last_{str(ndays)}.csv"

    csv_o = f"{csv_o}_{str(dayS)}_{str(dayE)}_{str(ndays)}.csv"


    df_rtn = pd.DataFrame()

    if finlib.Finlib().is_cached(csv_o) :
        logging.info("result csv has been updated in 1 days. "+csv_o)
        df_rtn = pd.read_csv(csv_o)

        most_decrease_df = df_rtn.sort_values(by='pct_change').head(10)
        most_increase_df = df_rtn.sort_values(by='pct_change').tail(10)
        most_amount_df = df_rtn.sort_values(by='amount').tail(30)
        most_vol_df = df_rtn.sort_values(by='vol').tail(10)
        most_swing_df = df_rtn.sort_values(by='swing').tail(10)

        logging.info("=== BK Most Decrease ===\n"+finlib.Finlib().pprint(most_decrease_df))
        logging.info("=== BK Most Increase ===\n"+finlib.Finlib().pprint(most_increase_df))
        logging.info("=== BK Most Amount ===\n"+finlib.Finlib().pprint(most_amount_df))
        logging.info("=== BK Most Vol ===\n"+finlib.Finlib().pprint(most_vol_df))
        logging.info("=== BK Most Swing ===\n"+finlib.Finlib().pprint(most_swing_df))
        return(df_rtn)



    df = finlib.Finlib().load_all_bk_qfq_data(days=300)


    # for code in df['code'].unique()[:2]:#debug
    for code in df['code'].unique():
        logging.info(f"code {code}")
        adf = df[df['code']==code][['code','date','close','open','high','low','vol','amount']]

        adf = adf[adf['date'] >= int(dayS)]
        adf = adf[adf['date'] <= int(dayE)]

        if adf.__len__()==0:
            logging.info(f"empty df, code {code}, {dayS}, {dayE}")
            continue

        s=adf.iloc[0]
        e=adf.iloc[-1]

        a = pd.DataFrame({
            'code':[code],
            'data_s':[s['date']],
            'data_e':[e['date']],
            'ndays':[ndays],
            'pct_change':[round(100*(e['close']-s['open'])/s['open'],1)],
            'swing':[round(100*(adf['high'].max()-adf['low'].min())/adf['low'].min(),1)],
            'vol':[adf['vol'].sum()],
            'amount':[adf['amount'].sum()],
        })

        df_rtn = df_rtn.append(a)


    df_rtn.to_csv(csv_o, encoding='UTF-8', index=False)
    logging.info(f"result saved to {csv_o}, len {str(df_rtn.__len__())}")
    return(df_rtn)

def bk_TD_indicator_start():
    rst_dir="/home/ryan/DATA/result/TD_Indicator_bk"
    if not os.path.isdir(rst_dir):
        os.mkdir(rst_dir)

    pre_n_day = 4
    consec_day = 9

    df_rtn = bk_TD_stocks(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day,no_garbage=False)
    return(df_rtn)



def not_work_da_v_zhui_zhang():
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv')[
         -300:]

    df['head'] = df['high'] - df['open']
    df['body'] = df['close'] - df['open']
    df['tail'] = df['close'] - df['low']

    df1 = df[df['body'] < 0]
    df1 = df1[df1['head'] < abs(df1['body'])]
    df1 = df1[abs(df1['body']) < df1['tail']]

    i = 0
    for index, row in df1.iterrows():
        # df_idx_1 = df1.iloc[i].name + 1
        # df_idx_2 = index + 1
        day1 = df.loc[index + 1]
        day2 = df.loc[index + 2]
        i += 1

        day_c = day1

        if day1['body'] < 0:
            # print("skip body<0 "+str(day1['body'] ))
            dayc = day2
            if day2['body'] < 0:
                continue

        if day_c['body'] < day_c['tail']:
            # print("skip body < tail " + str(day_c['tail']))
            # continue
            pass

        if day_c['close'] < row['close']:
            # print("skip close< lower than pre close " + str(row['close']))
            continue

        print(day_c['date'], day_c['close'])
        print('')


def lemon_766(csv_o):
    def _apply_func(tmp_df):
        logging.info(tmp_df.iloc[0]['name'])
        df1=copy.copy(tmp_df)
        df1 = finlib_indicator.Finlib_indicator().add_ma_ema(df=df1,short=20, middle=100, long=300)
        df1 = df1[['code', 'name', 'date', 'close','close_100_sma','close_300_sma']]
        df1 = df1.iloc[-1]
        return(df1)

    if finlib.Finlib().is_cached(csv_o,day=1):
        dfg = pd.read_csv(csv_o)
        logging.info(__file__ + " " + "loaded price mv from" + csv_o + " len " + str(dfg.__len__()))
    else:
        df = finlib.Finlib().load_all_ag_qfq_data(days=300)
        df = finlib.Finlib().add_stock_name_to_df(df=df)

        # df = df[df['code'].isin(['SZ000001','SH600519'])]
        dfg = df.groupby(by='code').apply(lambda _d: _apply_func(_d))


        dfg['c_20wk_diff']=round(100*(dfg['close'] - dfg['close_100_sma'])/dfg['close'],1)
        dfg['c_60wk_diff']=round(100*(dfg['close'] - dfg['close_300_sma'])/dfg['close'],1)
        dfg.to_csv(csv_o, encoding="UTF-8", index=False)
        logging.info(__file__ + " " + "saved price mv " + csv_o + " len " + str(dfg.__len__()))


    dfg_show = finlib.Finlib().add_stock_increase(df=dfg)

    dfg_show = dfg_show[dfg_show['c_60wk_diff']>0]
    dfg_show = dfg_show[dfg_show['c_20wk_diff']>0]
    dfg_show = dfg_show[dfg_show['c_60wk_diff']<5]
    dfg_show = dfg_show[dfg_show['inc360']<-5]

    logging.info(f"\n##### {sys._getframe().f_code.co_name} #####")
    logging.info(finlib.Finlib().pprint(dfg_show[['code','name','date','close','c_20wk_diff','c_60wk_diff', 'inc360']]))
    return(dfg)

def big_v(csv_o):
    def _apply_func(tmp_df):
        # logging.info(tmp_df.iloc[0]['name'])
        df=copy.copy(tmp_df)

        #positive
        df['all'] = round(abs(df['high'] - df['low']),2)
        df['body'] = round(abs(df['close'] - df['open']),2)

        df.at[df['close'] >= df['open'],'head'] = round(df['high']-df['close'],2)
        df.at[df['close'] < df['open'],'head'] = round(df['high']-df['open'],2)
        df['tail'] = round(df['all']-df['head']-df['body'],2)


        # logging.info(finlib.Finlib().pprint(df[['all','head','body','tail']].head(2)))
        df_big_v_yin=df[
            (df['close']<df['pre_close'])
            & (df['close']<df['open'])
            & (df['tail']>1.1*df['body'])
            & (df['body']>1.1*df['head'])
            & (df['high']>df['high'].shift(-1))
            & (df['low']<df['low'].shift(-1))
        ]

        # logging.info(finlib.Finlib().pprint(df_big_v_yin[['code','name','date','high','low']].tail(1)))

        return(df_big_v_yin)



    if finlib.Finlib().is_cached(csv_o,day=1):
        dfg = pd.read_csv(csv_o)
        logging.info(__file__ + " " + "loaded big v from" + csv_o + " len " + str(dfg.__len__()))
    else:
        df = finlib.Finlib().load_all_ag_qfq_data(days=300)
        df = finlib.Finlib().add_stock_name_to_df(df=df)

        # df = df[df['code'].isin(['SZ000001','SH600519'])]
        dfg = df.groupby(by='code').apply(lambda _d: _apply_func(_d))

        dfg.to_csv(csv_o, encoding="UTF-8", index=False)
        logging.info(__file__ + " " + "saved big v to " + csv_o + " len " + str(dfg.__len__()))


    # dfg = finlib.Finlib().add_stock_increase(df=dfg)
    dfg = finlib.Finlib().filter_days(df=dfg, date_col='date', within_days=5)
    logging.info(finlib.Finlib().pprint(dfg[['code','name','date','close']].head(5)))
    return(dfg)


#### MAIN #####

rst_dir= "/home/ryan/DATA/result"

from optparse import OptionParser
parser = OptionParser()

parser.add_option("-n", "--no_question", action="store_true", default=False, dest="no_question", help="run all without question.")

(options, args) = parser.parse_args()
no_question = options.no_question


if True or no_question or input("Run bar_support_resist_strategy? [N]")=="Y":
    df_strategy = bar_support_resist_strategy(csv_out = rst_dir+"/bar_support_resist_strategy.csv")
    # exit()
if no_question or input("Run lemon766? [N]")=="Y":
    df_lemon766 = lemon_766(csv_o = rst_dir+"/lemon_766.csv")
    # exit()

if no_question or input("Run bk_increase? [N]")=="Y":
    # df_bk_increase = bk_increase(csv_o = rst_dir+"/bk_increase.csv",start=20220427,ndays=3)
    # df_bk_increase = bk_increase(csv_o = rst_dir+"/bk_increase",start=20220101,ndays=200)
    # df_bk_increase = bk_increase(csv_o = rst_dir+"/bk_increase",dayS=20210715,ndays=30)

    df_bk_increase = bk_increase(csv_o = rst_dir+"/bk_increase.csv",ndays=3)
    # exit()

if no_question or input("Run BK ma_across? [N]")=="Y":
    df_ma_across = bk_ma_across(csv_o_b = rst_dir+"/bk_ma_cross_over.csv", csv_o_s = rst_dir+"/bk_ma_cross_down.csv")


if no_question or input("Run TD_indicator? [Y]")!="N":
    df_td = TD_indicator_start()
    logging.info("\n##### TD_indicator_main #####")
    logging.info(finlib.Finlib().pprint(df_td.head(5)))
    # exit()



if no_question or input("Run TD_indicator_BK? [Y]")!="N":
    df_td = bk_TD_indicator_start()
    logging.info("\n##### TD_indicator_bk_main #####")
    logging.info(finlib.Finlib().pprint(df_td.head(5)))
    # exit()

if no_question or input("Run Big V? [Y]")!="N":
    csv_o = "/home/ryan/DATA/result/big_v.csv"
    df_big_v = big_v(csv_o=csv_o)
    logging.info("\n##### big_v #####")
    logging.info(finlib.Finlib().pprint(df_big_v.tail(5)))
    # exit()

if no_question or input("Run Inner Merge TD_BigV? [N]") != "Y":
    df_rst = pd.merge(left=df_td, right=df_big_v,on='code',how='inner',suffixes=['_td','_bv'])
    logging.info("\n##### Inner Merge of TD and Big_V #####")
    logging.info(finlib.Finlib().pprint(df_rst[['code', 'name_td', 'date_td', 'date_bv']]))
    # exit()


if no_question or input("Run xiaohuxian? [N]") == "Y":
    csv_out = rst_dir+"/xiao_hu_xian.csv"
    df_xhx = xiao_hu_xian(csv_out)
    logging.info("\n##### xiao_hu_xian #####")
    logging.info(finlib.Finlib().pprint(df_xhx.head(5)))


if no_question or input("Run result_effort_ratio ? [Y]") != "N":
    csv_o = rst_dir+"/result_effort_ratio.csv"
    df_result_effort = result_effort_ratio(csv_o=csv_o)

    logging.info(f"\n##### df_result_effort #####")
    logging.info(finlib.Finlib().pprint(df_result_effort.tail(5)[['code','name','date','close','date_1','result_effort_ratio']]))


if no_question or input("Run ZD Tongji? [Y]") != "N":
    out_csv = rst_dir+"/daily_ZD_tongji.csv"
    df_zd = daily_UD_tongji(out_csv,ndays=5)
    logging.info("\n##### daily_UD_tongji #####")
    logging.info(finlib.Finlib().pprint(df_zd.tail(5)))
    # exit()



if no_question or input("Run price_amount_increase? [N]") == "Y":
     # startD and endD have to be trading day.
    df_increase = finlib_indicator.Finlib_indicator().price_amount_increase(startD=None, endD=None)
    # exit()


    # exit()
