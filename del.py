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

import akshare as ak
import glob
from scipy.stats import variation

########################### graham instrinsic value
def roe_pe():
    df_today = finlib.Finlib().get_today_stock_basic(date_exam_day='20210531')
    df_today = df_today[['code','industry','close','pe','pe_ttm','pb','ps','ps_ttm', 'dv_ratio', 'dv_ttm','list_date','on_market_days']]

    csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_20201231.csv'
    csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_20210331.csv'
    df_roe = finlib.Finlib().regular_read_csv_to_stdard_df(csv)
    df_roe = df_roe[['code','name','roe']]
    df_roe = pd.merge(df_roe, df_today, on='code')
    df_roe['roe_div_pe_ttm'] = df_roe['roe']/df_roe['pe_ttm']
    df_roe['roe_div_pe'] = df_roe['roe']/df_roe['pe']
    df_roe['roe_div_ps_ttm'] = df_roe['roe']/df_roe['ps_ttm']
    df_roe['roe_div_ps'] = df_roe['roe']/df_roe['ps']
    df_roe['roe_div_pb'] = df_roe['roe']/df_roe['pb']

    df_roe = finlib.Finlib().df_format_column(df=df_roe, precision='%.1e')

    df_roe_pe = df_roe[df_roe['pe']>0.1]
    df_roe_pe_ttm = df_roe[df_roe['pe_ttm']>0.1]
    df_roe_ps = df_roe[df_roe['ps']>0.1]
    df_roe_pb = df_roe[df_roe['pb']>0.1]

    print("==== ROE_DIV_PB ====")
    print(finlib.Finlib().pprint(df_roe_pb.sort_values(by='roe_div_pb', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','pb','roe_div_pb']].head(10)))

    print("==== ROE_DIV_PS ====")
    print(finlib.Finlib().pprint(df_roe_ps.sort_values(by='roe_div_ps', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','ps','roe_div_ps']].head(10)))

    print("==== ROE_DIV_PE_TTM ====")
    print(finlib.Finlib().pprint(df_roe_pe_ttm.sort_values(by='roe_div_pe_ttm', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','pe_ttm','roe_div_pe_ttm']].head(10)))

    print("==== ROE_DIV_PE ====")
    print(finlib.Finlib().pprint(df_roe_pe.sort_values(by='roe_div_pe', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','pe','roe_div_pe']].head(100)))

    print("haha")


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


def remove_garbage_by_fcf():
    df = finlib.Finlib()._remove_garbage_fcf_profit_act_n_years(n_year=1)
    df = finlib.Finlib()._remove_garbage_must(df=df)
    print(finlib.Finlib().pprint(df[['code','name']].head(100)))
    return(df)




def grep_garbage():
    #output saved to /home/ryan/DATA/result/garbage/*.csv
    df = finlib.Finlib().get_A_stock_instrment()
    df = finlib.Finlib().remove_garbage(df)

    finlib.Finlib()._remove_garbage_fcf_profit_act_n_years(n_year=1)

    #_remove_garbage_must
    finlib.Finlib()._remove_garbage_beneish_low_rate(df, m_score=-1)
    finlib.Finlib()._remove_garbage_change_named_stock(df, n_year=3)
    finlib.Finlib()._remove_garbage_high_pledge_ration(df, statistic_ratio_threshold=50, detail_ratio_sum_threshold=70)
    finlib.Finlib()._remove_garbage_low_roe_pe(df, market='AG', roe_pe_ratio_threshold=0.5)
    finlib.Finlib()._remove_garbage_by_fund_n_years(df, n_years=1)  #esp < bottom 0.1 etc
    finlib.Finlib()._remove_garbage_rpt_s1(df, code_field_name='code', code_format='C2D6')
    finlib.Finlib()._remove_garbage_by_profit_on_market_days_st(df)
    finlib.Finlib()._remove_garbage_none_standard_audit_statement(df, n_year=1)


########################### graham instrinsic value
def graham_intrinsic_value():
    df = finlib_indicator.Finlib_indicator().graham_intrinsic_value()
    logging.info("\n==== Graham instrinsic value ====")
    logging.info(finlib.Finlib().pprint(df.head(10)))
    return(df)


########################### Evaluate tr/pe ratio
def evaluate_tr_pe():
    df_daily = finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day())
    df_ts_all = finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years())

    df_ag_hold = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')['stock_list']
    tmp = finlib.Finlib().add_amount_mktcap(df=df_ag_hold)
    tmp = finlib.Finlib().add_tr_pe(df=tmp, df_daily=df_daily, df_ts_all=df_ts_all)
    tmp = finlib.Finlib().df_format_column(df=tmp, precision='%.1e')

    print(finlib.Finlib().pprint(tmp))

#########################################
def evaluate_grid(market='AG'):
    #markte in [AG_HOLD,AG, HK, US]
    logging.info("\n==== Evaluate_grid "+market+" ====")
    #
    # logging.info("========== Evaluate_grid 52 Week  ========")
    # logging.info("=============================")

    high_field='52 Week High'
    low_field='52 Week Low'

    # all_column = False
    all_column = True
    (df_52week,df_g_n4,df_g_n3_52week,df_g_n2,df_g_n1,df_g_p1_52week,df_g_p2,df_g_p3,df_g_p4) = finlib_indicator.Finlib_indicator().grid_market_overview(market=market,high_field=high_field, low_field=low_field, all_columns=all_column)

    cols = ['code','name', 'grid', 'eq_pos','roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)','atr_14','vola_month','volatility']
    # cols += ['cs_pos','grid_perc_resis_spt_dist',"l1","l2","l3","l4","l5","l6","l7" ,'description']
    cols += ['grid_support','grid_resistance','grid_perc_to_support','grid_perc_to_resistance']


    # print("==========  PositiveGrid 1 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_p1_52week[cols].head(2)))  #grid == 1

    # print("==========  NegtiveGrid 3 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_n3_52week[cols].head(2)))  #grid == -3


    # df_ag_hold = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')['stock_list']
    # df_ag = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG')['stock_list']

    df_mkt = finlib.Finlib().get_stock_configuration(selected=True, stock_global=market)['stock_list']

    # ddf_rtnf = finlib.Finlib().adjust_column(df=df, col_name_list=['code', 'tr_pe'])


    # df_hold_52week = pd.merge(df_52week, df_ag_hold, on=['code'], how='inner', suffixes=('', '_select'))[cols]
    df_52week = pd.merge(df_52week, df_mkt, on=['code'], how='inner', suffixes=('', '_select'))[cols]

    df_daily = finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day())
    df_ts_all = finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years())


    tmp = finlib.Finlib().add_amount_mktcap(df=df_52week)
    tmp = finlib.Finlib().add_tr_pe(df=tmp, df_daily=df_daily, df_ts_all=df_ts_all)
    tmp = finlib.Finlib().df_format_column(df=tmp, precision='%.1e')
    logging.info("\n==========  52 Week "+market+" ========")
    logging.info(finlib.Finlib().pprint(tmp))

    print("roe_ttm", "pe_ttm", 'Operating Margin (TTM)', 'Gross Margin (TTM)')
    _ = df_g_p4[['roe_ttm', 'pe_ttm', 'Operating Margin (TTM)', 'Gross Margin (TTM)']].describe().iloc[1];  print('p4: ', end='');  print(_['roe_ttm'], _['pe_ttm'], _['Operating Margin (TTM)'], _['Gross Margin (TTM)'])
    _ = df_g_p3[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p1_52week[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])

    _ = df_g_n1[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n3_52week[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])



    ###################

    logging.info("\n==========  3 Month "+market+" ========")
    logging.info("=============================")

    high_field='3-Month High'
    low_field ='3-Month Low'

    # all_column = False
    all_column = True
    (df_3month,df_g_n4,df_g_n3_3month,df_g_n2,df_g_n1,df_g_p1_3month,df_g_p2,df_g_p3,df_g_p4) = finlib_indicator.Finlib_indicator().grid_market_overview(market=market,high_field=high_field, low_field=low_field, all_columns=all_column)


    # print("========== 3 month  PositiveGrid 1 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_p1_3month[cols].head(2)))  #grid == 1

    # print("========== 3 month NegtiveGrid 3 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_n3_3month[cols].head(2)))  #grid == -3


    df = pd.merge(df_52week, df_3month, on='code', how='inner', suffixes=("", "_3M"))
    df_llrh = df[(df.grid==-3) & (df.grid_3M==1)][cols]  #long low, recent high
    logging.info("\n==== 52Week Low, 3Month High ====")
    print(finlib.Finlib().pprint(df_llrh.head(30)))

    df_lhrl = df[(df.grid==1) & (df.grid_3M==-3)][cols]  #long high, recent low
    logging.info("\n==== 52Week high, 3Month Lowe ====")
    print(finlib.Finlib().pprint(df_lhrl.head(30)))

    #
    print("roe_ttm", "pe_ttm",'Operating Margin (TTM)','Gross Margin (TTM)')
    _ = df_g_p4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p3[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p1_3month[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])

    _ = df_g_n1[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n3_3month[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])


    # df_g_p4.sort_values('grid_perc_resis_spt_dist', ascending=False, inplace=False).head(5)

    exit(0)

#########################################
def tv_filter():
    df_us_1D = finlib.Finlib().load_tv_fund(market='US', period='1D')
    df_us_1W = finlib.Finlib().load_tv_fund(market='US', period='1W')

    df=df_us_1D
    df = df[df['Total Debt (MRQ)']<=df['Total Revenue (FY)']]
    df = df[df['Basic EPS (TTM)']>0]
    df = df[df['roe_ttm']>5]
    df = df[df['pe_ttm']>5]
    df = df[df['ps']>5]

    cols=['code','name','Enterprise Value (MRQ)','EBITDA (TTM)','Enterprise Value/EBITDA (TTM)',
          'roe_ttm','ps','pe_ttm','Operating Margin (TTM)','vola_month',
          'Total Revenue (FY)', 'Total Debt (MRQ)',
          '52 Week High','close','52 Week Low',
          ]
    df[cols].describe()


    df_us_1D[df_us_1D['code'].isin(['BILI','FUTU','MSFT','DIS','TWLO','DELL'])][cols]


    print(1)



def ag_industry_selected():
    path = "/home/ryan/DATA/ag_selected_industry"
    allFiles = glob.glob(path + "/" + "*.BK*")
    df_rtn = pd.DataFrame()

    for f in allFiles:
        df = pd.read_csv(f, names=['code'], converters={'code': str}, header=None)
        df['industry'] = f.split("/")[-1].split("_")[0]
        df_rtn = df_rtn.append(df)
        logging.info("loading "+f+" appended len "+str(df.__len__())+" , all line "+str(df_rtn.__len__()))

    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn = finlib.Finlib().add_market_to_code(df=df_rtn)
    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)

    df_rtn = finlib.Finlib().remove_garbage(df=df_rtn)

    out_csv = path + "/" + "ag_industry_selected.csv"
    df_rtn.to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info("saved to "+out_csv)
    print(finlib.Finlib().pprint(df_rtn))
    return (df_rtn)


def bayes_start():
    df = finlib.Finlib().get_A_stock_instrment()
    # df = finlib.Finlib().remove_garbage(df)
    # df = finlib.Finlib().get_stock_configuration(select=False, stock_global='AG')
    sc = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')

    stock_list = sc['stock_list']
    csv_dir = sc['csv_dir']
    out_dir = sc['out_dir']

    for index, row in stock_list.iterrows():

        code = row['code']
        name = row['name']

        print(code+", "+name)

        _bayes_a_stock(code=code, name=name, csv_f=csv_dir+"/"+code+".csv")


    print(1)

def _bayes_a_stock(code,name,csv_f):
    # f = "/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv"
    # csv_f = "/home/ryan/DATA/DAY_Global/AG/SH603288.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(csv_f).tail(1000)

    # code = "SH000001"
    # code = "SH603288"
    # name = 'SH_INDEX'
    # name = '海天味业'

    short=4
    middle=10
    long=27

    df = finlib_indicator.Finlib_indicator().add_ma_ema(df=df,short=short, middle=middle, long=long)
    df['close_a5'] = df['close'].shift(-5)
    df['close_a2'] = df['close'].shift(-2)
    df['close_b1'] = df['close'].shift(1)
    df['open_b1'] = df['open'].shift(1)

    # df_up = df[df['close_a2'] > 1.01*df['close']]
    df_up = df[df['close_a5'] > 1.02*df['close']]


    df['sma_short_b1'] = df['sma_short_'+str(short)].shift(1)
    df['sma_middle_b1'] = df['sma_middle_'+str(middle)].shift(1)
    df['sma_long_b1'] = df['sma_long_'+str(long)].shift(1)


    #jincha
    df_jincha = df[(df['sma_short_b1'] < df['sma_long_b1']) & (df['sma_short_'+str(short)] > df['sma_long_'+str(long)])]

    #yunxian
    df_yunxian = df[(df['open'] < df['close']) & (df['open_b1'] > df['close_b1']) & (df['open_b1'] > df['close']) & (df['close_b1'] < df['open'])  ]

    _print_bayes_possibility(code=code,name=name, condition='jincha', df_all=df, df_up=df_up,df_con=df_jincha )
    _print_bayes_possibility(code=code,name=name, condition='yunxian', df_all=df, df_up=df_up,df_con=df_yunxian )

    return()



def _print_bayes_possibility(code, name, condition, df_all, df_up, df_con):
    #P(condition | up) : N(up & condition) / N(up)
    df_con_up =  pd.merge(df_con,df_up,on=['date'],how='inner',suffixes=('','_x'))

    p_con_up = df_con_up.__len__()/df_up.__len__()
    p_con = df_con.__len__() / df_all.__len__()
    p_up = df_up.__len__() / df_all.__len__()

    #P(up|condition)
    P_bayes = round((p_con_up*p_up)/p_con,2)  # it is actually equal df_con_up.__len__()/df_con.__len__()

    
    logging.info(str(code)+" "+str(name)+", Bayes Possibility, P(up|"+condition+"): "+str(P_bayes)
                 +" con_up: "+str(df_con_up.__len__() )
                 +" con: "+str(df_con.__len__() )
                 +" up: "+str(df_up.__len__() )
                 +" all: "+str(df_all.__len__() )
                 )


def result_effort_ratio():
    f = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=f)

    df = df.tail(200)

    df['inc'] = round(100*(df['close']-df['close'].shift(1))/df['close'].shift(1), 2)

    df['inc_1'] = df['inc'].shift(1)
    df['v_1'] = df['volume'].shift(1)

    df['result_effort_ratio'] = round((df['inc']/df['inc_1'])/(df['volume']/df['v_1']),2)
    print(finlib.Finlib().pprint(df))
    print(1)

def check_stop_loss_based_on_ma_across():
    file = "/home/ryan/DATA/result/price_let_mashort_equal_malong.csv"
    df = pd.read_csv(file)

    df_target = df[ (df['delta_perc'] < 10) & (df['delta_perc'] > -10)]

    df_target_no_gar = finlib.Finlib().remove_garbage(df=df_target)

    df_target_no_gar = df_target_no_gar.sort_values(by=['delta_perc'], ascending=[False]).reset_index().drop('index', axis=1)

    df_buy = df_target_no_gar[df_target_no_gar['action']=='buy'].sort_values(by=['delta_perc'],ascending=True).reset_index().drop('index', axis=1)
    df_sell = df_target_no_gar[df_target_no_gar['action']=='sell'].sort_values(by=['delta_perc'],ascending=False).reset_index().drop('index', axis=1)

    logging.info("\nBuy Target")
    logging.info(finlib.Finlib().pprint(df=df_buy))

    logging.info("\nSell Target")
    logging.info(finlib.Finlib().pprint(df=df_sell))


    print(1)


def plot_pivots(X, pivots):
    plt.xlim(0, len(X))
    plt.ylim(X.min()*0.99, X.max()*1.01)
    plt.plot(np.arange(len(X)), X, 'k:', alpha=0.5)
    plt.plot(np.arange(len(X))[pivots != 0], X[pivots != 0], 'k-')
    plt.scatter(np.arange(len(X))[pivots == 1], X[pivots == 1], color='g')
    plt.scatter(np.arange(len(X))[pivots == -1], X[pivots == -1], color='r')

def perf_review(df):
    print(1)



def stock_vs_index_perf_perc_chg():
    output_csv = "/home/ryan/DATA/result/df_profit_report_stock_vs_index_perf_pct_chg.csv"
    if finlib.Finlib().is_cached(output_csv, day=1):
        logging.info("using the result file "+output_csv)
        df_rtn = pd.read_csv(output_csv)
        return(df_rtn)


    csv_index = '/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv'
    df_index = finlib.Finlib().regular_read_csv_to_stdard_df(csv_index)

    df = finlib.Finlib().read_all_ag_qfq_data(days=300)
    df = finlib.Finlib().add_stock_name_to_df(df=df)
    # df = finlib.Finlib().remove_garbage(df=df)

    codes = df['code'].unique()
    codes.sort()
    df_profit_report = pd.DataFrame()

    for c in codes:
        df_sub = df[df['code']==c].reset_index().drop('index', axis=1)
        df_sub = pd.merge(left=df_sub, right=df_index, how='inner', on='date', suffixes=('',"_idx"))

        df_sub['pct_chg_vs_idx']=df_sub['pct_chg']-df_sub['pct_chg_idx']

        df_test = df_sub[['code','name','date','pct_chg_vs_idx']]
        df_test = df_test.rename(columns={'pct_chg_vs_idx':'close'})
        df_test = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_test, short=4, middle=27, long=60)
        (df_test,df_si_cha, df_jin_cha) = finlib_indicator.Finlib_indicator().slow_fast_across(df=df_test,fast_col_name='close_4_sma', slow_col_name='close_27_sma')

        df_jin_cha = pd.merge(df_jin_cha[['date']], df_sub, how="inner")
        df_si_cha = pd.merge(df_si_cha[['date']], df_sub, how="inner")

        if df_jin_cha.__len__()>0 and df_si_cha.__len__()>0:
            df_profit_details = finlib_indicator.Finlib_indicator()._calc_jin_cha_si_cha_profit(df_jin_cha=df_jin_cha, df_si_cha=df_si_cha)
            df_profit_details = finlib.Finlib().add_stock_name_to_df(df=df_profit_details)
            logging.info(finlib.Finlib().pprint(df_profit_details))
            df_profit_report  = df_profit_report.append(df_profit_details)

        continue

    logging.info("check_amount_x_individual, profit_overall describe ")
    logging.info(df_profit_report[["profit_overall"]].describe())
    df_profit_report.to_csv(output_csv)
    logging.info("result saved to "+output_csv)

def stock_vs_index_perf_amount():
    output_csv="/home/ryan/DATA/result/df_profit_report_stock_vs_index_perf_amount.csv"
    df_rtn = pd.DataFrame()

    if finlib.Finlib().is_cached(output_csv, day=1):
        logging.info("loading the result file "+output_csv)
        df_rtn = pd.read_csv(output_csv)
        return(df_rtn)

    csv_index = '/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv'
    df_index = finlib.Finlib().regular_read_csv_to_stdard_df(csv_index)

    df = finlib.Finlib().read_all_ag_qfq_data(days=300)
    df = finlib.Finlib().add_stock_name_to_df(df=df)
    # df = finlib.Finlib().remove_garbage(df=df)

    # df_today = df[df['date'] == df.date.max()]
    df_today = df[df['date'] == finlib.Finlib().get_last_trading_day()]

    q = df_today['amount'].quantile(0.7) #197355
    df_today = df_today[df_today['amount']>q].sort_values('amount', ascending=False, inplace=False)

    for c in df_today['code'].unique():
        df_sub = df[df['code']==c].reset_index().drop('index', axis=1)
        df_sub = pd.merge(left=df_sub, right=df_index, how='inner', on='date', suffixes=('',"_idx"))

        df_sub['amount_vs_idx']=df_sub['amount']*10000/df_sub['amount_idx']

        df_test = df_sub[['code','name','date','amount_vs_idx']]
        df_test = df_test.rename(columns={'amount_vs_idx':'close'})
        df_test = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_test, short=4, middle=27, long=60)
        (df_test,df_si_cha, df_jin_cha) = finlib_indicator.Finlib_indicator().slow_fast_across(df=df_test,fast_col_name='close_4_sma', slow_col_name='close_27_sma')

        df_jin_cha = pd.merge(df_jin_cha[['date']], df_sub, how="inner")
        df_si_cha = pd.merge(df_si_cha[['date']], df_sub, how="inner")

        bool_current_si_cha = False
        bool_current_jin_cha = False
        last_jin_cha_date = ''
        last_si_cha_date = ''

        if df_jin_cha.empty and (not df_si_cha.empty):
            bool_current_si_cha = True
            last_si_cha_date = df_si_cha.iloc[-1]['date']
        elif df_si_cha.empty and (not df_jin_cha.empty):
            bool_current_jin_cha = True
            last_jin_cha_date = df_jin_cha.iloc[-1]['date']
        elif (not df_si_cha.empty) and (not df_jin_cha.empty):
            if df_si_cha.iloc[-1]['date'] > df_jin_cha.iloc[-1]['date']:
                bool_current_si_cha = True
                last_si_cha_date = df_si_cha.iloc[-1]['date']
            else:
                bool_current_jin_cha = True
                last_jin_cha_date=df_jin_cha.iloc[-1]['date']

        if df_jin_cha.__len__()>0 and df_si_cha.__len__()>0:
            df_profit_details = finlib_indicator.Finlib_indicator()._calc_jin_cha_si_cha_profit(df_jin_cha=df_jin_cha, df_si_cha=df_si_cha)

            if df_profit_details.empty:
                continue

            df_profit_details = finlib.Finlib().add_stock_name_to_df(df=df_profit_details)
            logging.info(finlib.Finlib().pprint(df_profit_details.tail(1)))

            df_tmp = df_profit_details.tail(1)[['code',  'name', 'profit_overall']]
            df_tmp['transaction_count']=df_profit_details.__len__()
            df_tmp['current_si_cha']=bool_current_si_cha
            df_tmp['last_jin_cha_date']=last_jin_cha_date

            df_tmp['current_jin_cha']=bool_current_jin_cha
            df_tmp['last_si_cha_date']=last_si_cha_date

            df_rtn  = df_rtn.append(df_tmp)

        continue

    logging.info("check_amount_x_individual, profit_overall describe ")
    logging.info(df_rtn[["profit_overall"]].describe())

    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(output_csv)
    logging.info("result saved to "+output_csv)

    df_buy = df_rtn[df_rtn['profit_overall']>0]
    # df_buy = df_buy[df_buy['last_jin_cha_date']=='20211125']
    df_buy = df_buy[df_buy['last_jin_cha_date']==finlib.Finlib().get_last_trading_day()]
    df_buy = df_buy.sort_values(by='profit_overall', ascending=False)
    print(finlib.Finlib().pprint(df_buy.head(100)))

    logging.info("hhh")




def xiao_hu_xian(debug=False):
    logging.info("start of func xiao_hu_xian")

    csv_basic = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv"
    df_basic = finlib.Finlib().regular_read_csv_to_stdard_df(csv_basic)

    df_rtn = pd.DataFrame()

    ZT_P= 8 #ag_all_300_days.csv

    df = finlib.Finlib().read_all_ag_qfq_data(days=200)
    csv_f = "/home/ryan/DATA/DAY_Global/AG_qfq/ag_all_200_days.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)

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
        logging.info("no hit")
    else:
        df_rtn = finlib.Finlib().add_industry_to_df(df=df_rtn,source='wg')
        df_rtn = finlib.Finlib().add_amount_mktcap(df=df_rtn, mktcap_unit='100M')
        df_rtn = finlib.Finlib().add_tr_pe(df=df_rtn, df_daily=finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day()), df_ts_all=finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years()))
        df_rtn = finlib.Finlib().add_stock_increase(df=df_rtn)

        logging.info(finlib.Finlib().pprint(df_rtn))

    logging.info("end of the func xiao_hu_xian")
    sys.exit(0)

def fudu_daily_check():
    df_base = fudu_get_base_data(base_windows=5,slide_window=3)
    df_today=fudu_get_today_data(base_windows=5,slide_window=3)
    df_short = pd.merge(left=df_base, right=df_today[['code','drop_from_max','inc_from_min']], on='code', how='inner')
    df_short = finlib.Finlib().add_turnover_rate_f_sum_mean(df=df_short,ndays=3, dayE=finlib.Finlib().get_last_trading_day())

    df_base = fudu_get_base_data(base_windows=100,slide_window=90)
    df_today = fudu_get_today_data(base_windows=100,slide_window=90)
    df_long = pd.merge(left=df_base, right=df_today[['code','drop_from_max','inc_from_min']], on='code', how='inner')

    #######
    topN=5
    logging.info("\nRecent max turnover_sum stocks, top "+str(topN))
    logging.info(finlib.Finlib().pprint(df_short.sort_values(by='tv_sum').tail(topN)[['code','name','inc_cnt','inc_mean','tv_sum']]))
    # logging.info(finlib.Finlib().pprint(df_short.sort_values(by='tv_mean').tail(topN)[['code','name','inc_cnt','inc_mean','tv_mean']]))
    ###########
    logging.info("\nRecent max increase_mean stocks, top "+str(topN))
    logging.info(finlib.Finlib().pprint(df_short.sort_values(by='inc_mean').tail(topN)[['code','name','inc_cnt','inc_mean','dec_cnt']]))
    ###########
    # df_short_inc_max = df_short.sort_values(by='inc_mean').tail
    df=pd.merge(left=df_short, right=df_long, on=['code'], how='inner', suffixes=['_s', '_l'])

    #dfr: df short_inc_max, and long dec min
    dfr = df
    dfr = dfr[(dfr['inc_std_s'].rank(pct=True) > 0.2) & (dfr['inc_std_s'].rank(pct=True) <= 0.8)]
    dfr = dfr[(dfr['inc_mean_s'].rank(pct=True) > 0.9) & (dfr['inc_mean_s'].rank(pct=True) <= 0.95)].sort_values(by='inc_mean_s')

    dfr = dfr[(dfr['dec_std_l'].rank(pct=True) > 0) & (dfr['dec_std_l'].rank(pct=True) <= 0.9)]
    dfr = dfr[(dfr['dec_mean_l'].rank(pct=True) > 0) & (dfr['dec_mean_l'].rank(pct=True) <= 0.3)].sort_values(by='dec_mean_l',ascending=False)

    #############
    logging.info("\nmax average decrease in long period, top " + str(topN))
    logging.info("Hint: place Buy order -30% of current price. Reasonable to prepare -30% lost in 90 days.")
    logging.info(dfr.sort_values(by="dec_mean_l", ascending=False).tail(topN)[['code', 'name_l', 'dec_mean_l']])

    #############
    logging.info("\nmax average increase in long period, top " + str(topN))
    logging.info("Hint:Reasonable to prepare 30% increase in 90 days.")
    logging.info(dfr.sort_values(by="inc_mean_l").tail(topN)[['code', 'name_l', 'inc_mean_l']])



    #############
    logging.info("\nstocks have max drop in long period, top " + str(topN))
    _df = dfr.sort_values(by="drop_from_max_l", ascending=False).tail(topN) #not much meaning on drop_from_max_s
    logging.info(_df[['code', 'name_l', 'window_size_l', 'drop_from_max_l']])

    logging.info("\nstocks have max drop in long period, max inc_from_min_s in short period, top " + str(topN))
    _df = _df.sort_values(by="inc_from_min_s").tail(topN)
    logging.info(_df[['code', 'name_l', 'window_size_l','window_size_s','drop_from_max_l','inc_from_min_s']])

    logging.info("\nmax drop in long period, max inc_mean in short period, top " + str(topN))
    _df = _df.sort_values(by="inc_mean_s").tail(topN)
    logging.info(_df[['code', 'name_l','window_size_l','window_size_s', 'inc_mean_s', 'drop_from_max_l','inc_from_min_s']])


    #############
    logging.info("\nmax increase in short period, top " + str(topN))
    logging.info("Hint: burst stocks")
    df_short['inc_std_rank'] = df_short['inc_std'].rank(pct=True)
    # df_short= df_short[df_short['inc_std'] < 2]
    logging.info(df_short.sort_values(by='inc_from_min').tail(topN)[['code','name','inc_from_min','inc_std','inc_std_rank','tv_mean', 'tv_sum']])

    df_short_inc_max = df_short.sort_values(by='inc_from_min').tail(100)
    df_long_dec_max = df_long.sort_values(by='drop_from_max').head(100)

    df_long_drop_short_inc=pd.merge(left=df_long_dec_max, right=df_short_inc_max,on=['code'],how='inner',suffixes=['_l','_s'])
    df_long_drop_short_inc = df_long_drop_short_inc.sort_values(by='inc_from_min_s')
    df_long_drop_short_inc = df_long_drop_short_inc.sort_values(by='drop_from_max_l',ascending=False)


    df_long_drop_short_inc=pd.merge(left=df_long.sort_values(by='drop_from_max').head(100),
                                    right=df_short.sort_values(by='inc_from_min').tail(100),
                                    on=['code'],how='inner')
    logging.info("end of fudu_daily_check\n")
    return()



def fudu_get_base_data(base_windows=5, slide_window=3):
    csv_out = "/home/ryan/DATA/result/zhangfu_tongji_base_"+str(base_windows)+"_slide_"+str(slide_window)+".csv"

    if finlib.Finlib().is_cached(file_path=csv_out, day=1):
        df_rtn=pd.read_csv(csv_out)
        return(df_rtn)

    df_rtn = pd.DataFrame()

    df = finlib.Finlib().read_all_ag_qfq_data(days=base_windows)
    csv_f = "/home/ryan/DATA/DAY_Global/AG_qfq/ag_all_"+str(base_windows)+"_days.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)

    df =df[['code','date','high','low', 'close']]

    logging.info("fudu base data")

    for c in df.code.unique():
        # c = 'SZ300656'
        dfs=df[df['code']==c].tail(base_windows).reset_index().drop('index', axis=1)

        if dfs.__len__()<base_windows:
            continue

        h=dfs['high']
        l=dfs['low']

        dfs['win_max'] = h.rolling(slide_window).max()
        dfs['win_min'] = l.rolling(slide_window).min()

        dfs['win_max_idx'] = h.rolling(slide_window).apply(np.argmax)[slide_window-1:].astype(int)+np.arange(len(h)-slide_window+1)
        dfs['win_min_idx'] = l.rolling(slide_window).apply(np.argmin)[slide_window-1:].astype(int)+np.arange(len(l)-slide_window+1)

        dfs_inc = dfs[dfs['win_max_idx']>dfs['win_min_idx']]
        dfs_dec = dfs[dfs['win_max_idx']<dfs['win_min_idx']]

        dfs_inc = dfs_inc.drop_duplicates(subset=['win_max_idx','win_min_idx'], keep='last')
        dfs_dec = dfs_dec.drop_duplicates(subset=['win_max_idx','win_min_idx'], keep='last')

        dfs_inc['inc'] = round((dfs_inc['win_max'] - dfs_inc['win_min'])*100/dfs_inc['win_min'],0)
        # logging.info(c+" increase perc of window "+str(window_size)+" days,  description\n"+ str(dfs_inc['inc'].describe()))

        dfs_dec['dec'] = round((dfs_dec['win_min'] - dfs_dec['win_max'])*100/dfs_dec['win_max'],0)
        # logging.info(c+" decrease perc of window "+str(window_size)+" days,  description\n"+ str(dfs_dec['dec'].describe()))

        inc_desc=dfs_inc['inc'].describe()
        dec_desc=dfs_dec['dec'].describe()

        _df = pd.DataFrame({
            'code':[c],
            'window_size':[slide_window],
            'inc_cnt':[inc_desc['count']],
            'inc_mean':[round(inc_desc['mean'],2)],
            'inc_std':[round(inc_desc['std'],2)],
            'inc_max':[round(inc_desc['max'],2)],
            'inc_min':[round(inc_desc['min'],2)],

            'dec_cnt': [dec_desc['count']],
            'dec_mean': [round(dec_desc['mean'], 2)],
            'dec_std': [round(dec_desc['std'], 2)],
            'dec_max': [round(dec_desc['max'], 2)],
            'dec_min': [round(dec_desc['min'], 2)],
        })

        # logging.info(finlib.Finlib().pprint(_df))
        df_rtn = df_rtn.append(_df)

    df_rtn['cnt_ratio'] = round((df_rtn['inc_cnt']+1) / (df_rtn['dec_cnt']+1), 2)
    df_rtn['inc_ratio'] = round((df_rtn['inc_cnt']+1) * df_rtn['inc_mean'] / (df_rtn['dec_cnt']+1) / abs(df_rtn['dec_min']), 2)


    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)
    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
    logging.info("saved to " + csv_out + " len " + str(df_rtn.__len__()))
    return(df_rtn)

def fudu_get_today_data(base_windows=5,slide_window=3):
    csv_out = "/home/ryan/DATA/result/zhangfu_tongji_daily_check_base_"+str(base_windows)+"_slide_"+str(slide_window)+".csv"

    if finlib.Finlib().is_cached(file_path=csv_out, day=1):
        df_rtn=pd.read_csv(csv_out)
        return(df_rtn)

    df_rtn = pd.DataFrame()

    df = finlib.Finlib().read_all_ag_qfq_data(days=base_windows)
    csv_f = "/home/ryan/DATA/DAY_Global/AG_qfq/ag_all_"+str(base_windows)+"_days.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)
    df = finlib.Finlib()._remove_garbage_on_market_days(df=df,on_market_days=90)

    df =df[['code','date','high','low', 'close']]
    i=0
    all=df.code.unique().__len__()
    logging.info("fudu generating today data")
    for c in df.code.unique():
        # logging.info(str(i) + " of " + str(all))
        i+=1

        dfs=df[df['code']==c]

        dfs=dfs.tail(slide_window).reset_index().drop('index', axis=1)

        max=dfs['high'].max()
        min=dfs['low'].min()
        now=dfs['close'].iloc[-1]
        drop_from_max = round(-100 * (max-now)/max, 0)
        inc_from_min = round(100 * (now-min)/min, 0)

        _df = pd.DataFrame({
            'code':[c],
            'window_size':[slide_window],
            'drop_from_max':[drop_from_max],
            'inc_from_min':[inc_from_min],
        })
        df_rtn = df_rtn.append(_df)

        # logging.info(_df)

    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)
    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
    logging.info("saved to " + csv_out + " len " + str(df_rtn.__len__()))
    return(df_rtn)


def fetch_holder():
    df_holder = pd.DataFrame()

    out_csv = "/home/ryan/DATA/result/holdernumber.csv"

    if finlib.Finlib().is_cached(out_csv, day=5):
        logging.info("loading from " + out_csv)
        df_holder = pd.read_csv(out_csv)
    #
    pro = ts.pro_api(token="4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b", timeout=3)

    if df_holder.__len__() < 100:
        df_holder = pro.stk_holdernumber(end_date='20211231')
        df_holder = finlib.Finlib().ts_code_to_code(df_holder)

    i = 0
    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)
    stock_list = finlib.Finlib().add_ts_code_to_column(stock_list)
    for index, row in stock_list.iterrows():
        i += 1
        name, code, ts_code = row['name'], row['code'], row['ts_code']
        logging.info(str(i) + " of " + str(stock_list.__len__()) + " " + str(code) + " " + name)
        if code in df_holder['code'].to_list():
            # logging.info("code in return")
            continue
        else:
            # logging.info("not in return, fetch indidual")
            try:
                t = pro.stk_holdernumber(ts_code=ts_code)
            except Exception:
                logging.info("exception on pro.stk_holdernumber")
                continue

            try:
                t = finlib.Finlib().ts_code_to_code(t)
            except Exception:
                logging.info("exception on ts_code_to_code")
                continue

            time.sleep(1)

            # df_holder=df_holder.append(t.head(1))
            df_holder = df_holder.append(t)
            # print(str(df_holder.__len__()))
            df_holder.to_csv(out_csv, encoding='UTF-8', index=False)

    df_holder.reset_index().drop('index', axis=1).to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info("df_holder saved to "+out_csv+" , len "+str(df_holder.__len__()))
    return(df_holder)


def daily_UD_tongji():
    df_rtn = pd.DataFrame()

    out_csv = "/home/ryan/DATA/result/daily_ZD_tongji.csv"

    if finlib.Finlib().is_cached(out_csv, day=5):
        logging.info("loading from " + out_csv)
        df_rtn = pd.read_csv(out_csv)
    #
    pro = ts.pro_api(token="4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b", timeout=3)

    if df_rtn.__len__() < 100:
        df_rtn = pro.limit_list(trade_date='20220110')
        # df_rtn = pro.limit_list()
        df_rtn = finlib.Finlib().ts_code_to_code(df_rtn)

        df_D = df_rtn[df_rtn['limit']=='D']
        df_U = df_rtn[df_rtn['limit']=='U']

        logging.info("Down Limit, by fl_ratio\n"+finlib.Finlib().pprint(df_D.sort_values(by='fl_ratio').tail(10)))
        logging.info("UP Limit, by fl_ratio\n"+finlib.Finlib().pprint(df_U.sort_values(by='fl_ratio').tail(10)))
        # logging.info(finlib.Finlib().pprint(df_rtn[['code','name','fc_ratio','fl_ratio','first_time','last_time']]))
        logging.info("end of daily_UD_tongji\n\n")

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
            logging.info(f"code {code},{date} too far from setupbar, no trending present.{str(bars_after_setup)}  "
                         + f" ann_setup {anno_setup} pre_anno_setup {pre_anno_setup}")

        if setup_bar_index > 0 and bars_after_setup == 4 and pre_anno_setup=='DN_D9_of_9':
            if close < adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_start_DN_13_CD'
                print(f"{code} {date} {close} {anno_bar10}.")
                adf.at[index, 'anno_bar10'] = anno_bar10
            elif close > adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_reverse_9_dn_to_up'
                print(f"{code} {date} {close} {anno_bar10}.")
                adf.at[index, 'anno_bar10'] = anno_bar10


        if setup_bar_index > 0 and bars_after_setup == 4 and pre_anno_setup=='UP_D9_of_9':
            if close < adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_start_reverse_9_up_to_dn'
                print(f"{code} {date} {close} {anno_bar10}.")
                adf.at[index, 'anno_bar10'] = anno_bar10
            elif close > adf.at[setup_bar_index, 'close']:
                anno_bar10 = f'Bar10_start_UP_13_CD'
                print(f"{code} {date} {close} {anno_bar10}.")
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
                        print(p_str)
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
                        print(p_str)
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


def td_indicator(df,pre_n_day,consec_day):
    df_setup = _td_setup_9_consecutive_close_4_day_lookup(df,pre_n_day,consec_day)
    df_countdown = _td_countdown_13_day_lookup(df_setup,cancle_countdown=True)
    df_9_13, df_op = _td_oper(df_countdown)
    return(df_9_13, df_op, df_countdown.tail(1))


def TD_szsz_index(rst_dir,pre_n_day,consec_day):
    df_index=finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv')[-300:]
    df_9_13, df_op, df_today = td_indicator(df_index,pre_n_day,consec_day)
    df_9_13.to_csv(rst_dir+"/szzs_9_13.csv", encoding='UTF-8', index=False)
    df_op.to_csv(rst_dir+"/szzs_op.csv", encoding='UTF-8', index=False)
    df_today.to_csv(rst_dir+"/szzs_today.csv", encoding='UTF-8', index=False)
    print("SZZS INDEX 9_13: \n"+finlib.Finlib().pprint(df_9_13))
    print("SZZS INDEX Operation: \n"+finlib.Finlib().pprint(df_op))

def TD_debug(rst_dir,pre_n_day,consec_day):
    # df=finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/DAY_Global/AG_qfq/SH600519.csv')[-300:]
    df=finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/DAY_Global/AG_qfq/SH601918.csv')[-300:]
    df = df[df['date'] > '20211107'].reset_index().drop('index', axis=1)

    df_9_13, df_op, df_today = td_indicator(df,pre_n_day,consec_day)
    df_9_13.to_csv(rst_dir+"/debug_mt_9_13.csv", encoding='UTF-8', index=False)
    df_op.to_csv(rst_dir+"/debug_mt_op.csv", encoding='UTF-8', index=False)
    print("debug 9_13: \n"+finlib.Finlib().pprint(df_9_13))
    print("debug Operation: \n"+finlib.Finlib().pprint(df_op))

def TD_stocks(rst_dir,pre_n_day,consec_day,stock_global=None):
    rtn_9_13 = pd.DataFrame()
    rtn_op = pd.DataFrame()
    rtn_today = pd.DataFrame()

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

    df = finlib.Finlib().read_all_ag_qfq_data(days=300)

    if stock_global is not None:
        df = pd.merge(left=df, right=df_hold[['code']], on='code',how='inner').reset_index().drop('index', axis=1)

    for code in df['code'].unique():
        logging.info(f"code {code}")
        adf = df[df['code']==code][['code','date','close','high', 'open', 'low']]
        df_9_13, df_op, df_today = td_indicator(adf,pre_n_day,consec_day)

        rtn_9_13 = rtn_9_13.append(df_9_13).reset_index().drop('index',axis=1)
        rtn_op = rtn_op.append(df_op).reset_index().drop('index',axis=1)
        rtn_today = rtn_today.append(df_today).reset_index().drop('index',axis=1)

        rtn_9_13.to_csv(td_csv_9_13, encoding='UTF-8', index=False)
        rtn_op.to_csv(td_csv_op, encoding='UTF-8', index=False)
        rtn_today.to_csv(td_csv_today, encoding='UTF-8', index=False)

    finlib.Finlib().add_stock_name_to_df(df=rtn_9_13).to_csv(td_csv_9_13, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_op).to_csv(td_csv_op, encoding='UTF-8', index=False)
    finlib.Finlib().add_stock_name_to_df(df=rtn_today).to_csv(td_csv_today, encoding='UTF-8', index=False)

    print(f"result saved to \n{td_csv_today}\n{td_csv_op}\n{td_csv_9_13}")

def TD_indicator_main():
    rst_dir="/home/ryan/DATA/result/TD_Indicator"
    if not os.path.isdir(rst_dir):
        os.mkdir(rst_dir)

    pre_n_day = 4
    consec_day = 9

    TD_debug(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day)
    TD_szsz_index(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day)
    TD_stocks(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day, stock_global='AG_HOLD')
    TD_stocks(rst_dir=rst_dir,pre_n_day=pre_n_day,consec_day=consec_day)

    return()


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


#### MAIN #####
TD_indicator_main()


df = finlib.Finlib().read_all_ag_qfq_data(days=200)
df = finlib.Finlib()._remove_garbage_on_market_days(df)
df = finlib.Finlib().remove_garbage(df)

# a = daily_UD_tongji()
df_rtn = fudu_daily_check()
a = xiao_hu_xian()

df_holder = fetch_holder()
df_holder[df_holder['code']=='SZ300661']

#choose the latest df_holder
df_holder_latest = df_holder[df_holder['holder_num']>500]
df_holder_latest = df_holder_latest[~df_holder_latest['holder_num'].isna()]
df_holder_latest= df_holder_latest.drop_duplicates(subset=['code'],keep='first')

#merge
df_amt = finlib.Finlib().get_daily_amount_mktcap()
df=pd.merge(left=df_holder_latest, right=df_amt, on='code', how='inner')
df = df[['code','holder_num','total_mv','circ_mv','close','pe','turnover_rate']]
df['mv_per_holder']=round(df['circ_mv']/df['holder_num'],0)
df=finlib.Finlib().add_stock_name_to_df(df=df)


df = df.sort_values(by='mv_per_holder', ascending=False).reset_index().drop('index', axis=1)
print("top mv_per_holder")
print(finlib.Finlib().pprint(df.head(50)))

a = finlib.Finlib().remove_garbage(df=df)
print(finlib.Finlib().pprint(a.head(50)))

# a = stock_vs_index_perf_perc_chg()
a = stock_vs_index_perf_amount()


# f = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
f = "/home/ryan/DATA/DAY_Global/AG_qfq/BJ831445.csv"
df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=f)

df = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
df['date'] = df['date'].apply(lambda _d: datetime.datetime.strftime(_d, "%Y%m%d"))


grep_garbage() #save to files /home/ryan/DATA/result/garbage/*.csv
plt.show()
evaluate_grid(market='AG')
exit()


 # startD and endD have to be trading day.
df_increase = finlib_indicator.Finlib_indicator().price_amount_increase(startD=None, endD=None)
exit()

# bayes_start()
check_stop_loss_based_on_ma_across()

result_effort_ratio()

grep_garbage() #save to files /home/ryan/DATA/result/garbage/*.csv
exit()

df_all = finlib.Finlib().get_A_stock_instrment()
df_all = finlib.Finlib().add_industry_to_df(df=df_all)
print(1)


df_industry = ag_industry_selected()
exit()

df_fcf = remove_garbage_by_fcf() # update garbage, consider fcf and must

df_intrinsic_value = graham_intrinsic_value()

#########################################
df = finlib.Finlib().load_price_n_days(ndays=20)

df = finlib.Finlib().ts_code_to_code(df=df)
df_rtn = pd.DataFrame()

for code in df['code'].unique():
    df_1 = df[df['code']==code].reset_index().drop('index', axis=1)

    if df_1.__len__() < 5:
        continue

    df_1['vol_avg_5'] = df_1['vol'].rolling(window=5).mean()

    # df_1['price_change_in_day'] = round((df_1['close'] - df_1['open'])*100/df_1['open'], 2)
    df_1['price_change_across_day'] = round((df_1['close'] - df_1['close'].shift(1))*100/df_1['close'].shift(1), 2)
    df_1['vol_change_across_day'] = round((df_1['vol'] - df_1['vol'].shift(1))*100/df_1['vol'].shift(1), 2)
    df_1['vol_change_vs_avg_5'] = round((df_1['vol'] - df_1['vol_avg_5'].shift(1))*100/df_1['vol_avg_5'].shift(1), 2)
    df_1['vp_chg_ratio']= round(df_1['vol_change_across_day'] / df_1['price_change_across_day'],2)
    df_1['pct_chg_next_day']= df_1['pct_chg'].shift(-1)
    df_1['corr']=round(df_1['pct_chg_next_day'].corr(df_1['vp_chg_ratio']),2)
    df_1 = df_1.drop(columns=['open','high','low','pre_close','change'])

    df_rtn = df_rtn.append(df_1.tail(1))
    # print(str(code)+" pv_chg_ration / pct_chg_next_day cor " + str(df_1['corr'].iloc[-1]))

print(1)
df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)
df_rtn.sort_values(by='corr', ascending=False)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vp_chg_ratio', ascending=False).head(10)))
print(finlib.Finlib().pprint(df_rtn.head(10)))

#largest vol increase
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_across_day', ascending=False).head(10)))

#smallest vol increase (compare to vol_day-1)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_across_day', ascending=True).head(10)))

#smallest vol increase (compare to vol_ma5)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_vs_avg_5', ascending=True).head(10)))
#smallest vol increase (compare to vol_ma5)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_vs_avg_5', ascending=False).head(10)))


df_pin_bar_uppershadow = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_LONG_UPPER_SHADOW)
df_pin_bar_leg = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_LONG_LOWER_SHADOW)
df_pin_bar_guangtou = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_GUANG_TOU)
df_pin_bar_guangjiao = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_GUANG_TOU)

df_hammer= pd.merge(df_pin_bar_leg, df_pin_bar_guangtou, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
df_hammer_rev= pd.merge(df_pin_bar_uppershadow, df_pin_bar_guangjiao, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)

#########################################


csv = '/home/ryan/DATA/pickle/daily_update_source/ag_daily_20210319.csv'
df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv)

df = df.sort_values(by='volume')
df = finlib.Finlib().add_stock_name_to_df(df=df)

#########################################
df_tv = finlib.Finlib().load_tv_fund(market='AG', period="d")
df_tv = finlib.Finlib().add_stock_name_to_df(df_tv)
df_tv = finlib.Finlib().add_ts_code_to_column(df=df_tv)

_df = df_tv[['code','name','close','52 Week High']]
_df['ratio']=df_tv['close']/df_tv['52 Week High']
df_low_p = _df[_df['ratio']<0.5]
df = finlib.Finlib().remove_garbage(df=df_low_p)
print(finlib.Finlib().pprint(df))
exit(0)


# df_pv_db_buy_filter = finlib.Finlib().regular_read_csv_to_stdard_df('/home/ryan/DATA/result/today/talib_and_pv_db_buy_filtered_AG.csv')
df_hs300_candi = finlib.Finlib().regular_read_csv_to_stdard_df('/home/ryan/DATA/result/hs300_candidate_list.csv')
df_pv_db_buy_filter = finlib.Finlib()._remove_garbage_must(df=df_hs300_candi)
exit(0)


df_pv_db_buy_filter.drop_duplicates(inplace=True)

df_pv_db_buy_filter.sort_values('2mea', ascending=False, inplace=True)
# already sorted by Increase_2D in t_daily_pattern_Hit_Price_Volume.py
df_pv_db_buy_filter = df_pv_db_buy_filter.loc[df_pv_db_buy_filter['close_p'] != '0.0']
len_df_pv_db_buy_filter_0 = str(df_pv_db_buy_filter.__len__())
df_pv_db_buy_filter = finlib.Finlib().remove_garbage(df_pv_db_buy_filter, code_field_name='code', code_format='C2D6')
logging.info(__file__ + " " + "\t df_pv_db_buy_filter length " + str(df_pv_db_buy_filter.__len__()))



df = finlib.Finlib().load_tv_fund(market='US')

#ROE > 0

#PE rank < 0.85 perc

net_profit

_remove_garbage_high_pledge_ration

low_roe_pe

#Depeat fuzhai < zichan





# a = finlib.Finlib().is_on_market(ts_code="600519.SH", date="20161231", basic_df=None)
# df = finlib.Finlib().load_all_ts_pro()
df = finlib.Finlib().get_A_stock_instrment()
df_1 = finlib.Finlib().remove_garbage(df=df)
print(finlib.Finlib().pprint(df_1))

df_us = finlib.Finlib().get_roe_div_pe(market='US')
df_ag = finlib.Finlib().get_roe_div_pe(market='AG')

df = df_ag
df_gar_1 = df[df['pe_ttm']<1]
df_gar_2 = df[df['roe']<3]
df_gar_3 = df[df['roe_pe']<0.5]

# df = finlib.Finlib().load_tv_fund(market='us',period='d')





pro = ts.pro_api(token="4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b")
df = pro.stk_holdernumber(ts_code='300199.SZ', start_date='20160101', end_date='20181231')


#获取申万一级行业列表
df1 = pro.index_classify(level='L1', src='SW')

#获取申万二级行业列表
df2 = pro.index_classify(level='L2', src='SW')

#获取申万三级级行业列表
df3 = pro.index_classify(level='L3', src='SW')


df = pro.hk_daily(ts_code='00001.HK', start_date='20190101', end_date='20190904')
################
df = ak.stock_hk_daily(symbol="00700",adjust="qfq")
exit(0)



exit(0)


# finlib_indicator.Finlib_indicator().print_support_price_by_price_volume(code='SZ000568', market='US_INDEX')
# finlib_indicator.Finlib_indicator().print_support_price_by_price_volume(code='SZ000568', market='AG')


#from futu import *
#
# df1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.SZCZ_INDEX_BUY_CANDIDATE)
# df1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.MA55_NEAR_MA21)
# print(finlib.Finlib().pprint(df1[['code','date','reason']]))
#
# df1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.MA21_NEAR_MA55_N_DAYS)
# print(finlib.Finlib().pprint(df1[['code','date','reason']]))
#
# df2 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.PV2_VOLUME_RATIO_BOTTOM_10P)
# pass
# #####################
#
# #Year 2020
# # finlib.Finlib().get_last_n_days_stocks_amount(dayS='20191101', dayE='20201031', debug=True)
# # exit(0)
# # finlib.Finlib().get_last_n_days_stocks_amount(dayS='20200501', dayE='20201031')
#
# #Year 2021
# finlib.Finlib().get_last_n_days_stocks_amount(dayS='20200501', dayE='20210430', daily_update=True,
#                                               force_run=False, #ignore file existance, calculate everytime.
#                                               debug=True,  #only check head 3 rows
#                                               )  # HS300
# exit(0)
# finlib.Finlib().get_last_n_days_stocks_amount(dayS='20201101', dayE='20210430')  # SHEN_ZHEN
#
#
#
# exit(0)
#
#
#
#
# this_year = datetime.datetime.today().year
#
# this_year = 2020
# this_month = 2
#
#
# last_year = this_year - 1
# this_month = datetime.datetime.today().month
# ndays = 365
#
# if this_month <= 6:
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(last_year) + '0501', dayE=str(this_year) + '0430') # HS300
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(last_year) + '1101', dayE=str(this_year) + '0430') # SHEN_ZHEN
#
#     df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=ndays,period_end=None, debug=True,force_run=True) #output  /home/ryan/DATA/result/average_daily_amount_sorted.csv
#
#     a = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.MA5_UP_KOUDI_DISTANCE_GT_5)
#
#     exit(0)
#
# if this_month >6:
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(last_year) + '1101', dayE=str(this_year) + '1031') # HS300
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(this_year) + '0501', dayE=str(this_year) + '1031') # SHEN_ZHEN
#
#
# finlib.Finlib().get_last_n_days_stocks_amount(ndays=365)
#
#




points = [[1, 5], [2, 3], [4, 1], [8, 5]]
x = [1,2,4,8]
y = [5,3,1,5]
coefficients = np.polyfit(x=x, y=y, deg=6)
poly = np.poly1d(coefficients)
new_x = np.linspace(x[0], x[-1])

new_y = poly(new_x)
plt.plot(x, y, "o", new_x, new_y)
plt.show()
plt.xlim([x[0]-1, x[-1] + 1 ])
plt.savefig("line.jpg")



# df = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.DOUBLE_BOTTOM_AG_SELECTED, selected=True)
# print(finlib.Finlib().pprint(df))
#
#
# df = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.DOUBLE_BOTTOM_AG_SELECTED, selected=False)
# print(finlib.Finlib().pprint(df))


################ Filter price under 30 weeks MA,
################ Screen price growth steadly, e.g increase 2 point every days.
################  Looks duplicate with VCP.py at some level,
################  Consider if VCP.py output can be reused.
out_f = "/home/ryan/DATA/result/price_quality.csv"
stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # 603999.SH
stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')
stock_list = finlib.Finlib().add_ts_code_to_column(df=stock_list, code_col='code')


df = finlib.Finlib()._remove_garbage_high_pledge_ration(df=stock_list)


pro= ts.pro_api()
df = pro.pledge_detail(ts_code='000935.SZ')
print(finlib.Finlib().pprint(df))

csv_dir = "/home/ryan/DATA/DAY_Global/AG"
i = 0

for index, row in stock_list.iterrows():
    i += 1
    print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
    name, code = row['name'], row['code']

    csv_f = csv_dir + "/" + code + ".csv"


    if not os.path.exists(csv_f):
        print("csv_f not exist, " + csv_f)
        continue

    print("reading " + csv_f)
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)

    close_sma_2 = df['close'].rolling(window=2).mean()
    close_ema_2 = df['close'].ewm(span=2, min_periods=0, adjust=False, ignore_na=False).mean()

    finlib_indicator.Finlib_indicator().add_rsi()
    finlib_indicator.Finlib_indicator().add_ma_ema()


    pass



exit(0)

#END OF

#
# df1 = finlib.Finlib().evaluate_by_ps_pe_pb()
# print(df1.columns.values)
#
# df2 = finlib.Finlib().get_A_stock_instrment()
# df2 = df2[df2['code']=='600158']
# df3 = finlib.Finlib().remove_garbage(df2)
#

# (no_profit two years | PB


#on market < 2 years: PS

#on  2 < market  < 10 years: PE


#finlib.Finlib().get_today_stock_basic(date_exam_day='20200828')


n_days = 300
dir='/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source'
csv_f_top_list = dir + "/top_list_" + str(n_days) + "_days.csv"
csv_f_top_inst = dir + "/top_inst_" + str(n_days) + "_days.csv"


def fetch_top_lis_inst():
    pro = ts.pro_api()

    df_top_list = pd.DataFrame()
    df_top_inst = pd.DataFrame()


    for i in range(n_days):
        dateS = (datetime.datetime.today() - datetime.timedelta(days=i)).strftime("%Y%m%d")

        if finlib.Finlib().is_a_trading_day_ag(dateS):
            # 龙虎榜每日明细
            # output columns:
            # trade_date	str	Y	交易日期
            # ts_code	str	Y	TS代码
            # name	str	Y	名称
            # close	float	Y	收盘价
            # pct_change	float	Y	涨跌幅
            # turnover_rate	float	Y	换手率
            # amount	float	Y	总成交额
            # l_sell	float	Y	龙虎榜卖出额
            # l_buy	float	Y	龙虎榜买入额
            # l_amount	float	Y	龙虎榜成交额
            # net_amount	float	Y	龙虎榜净买入额
            # net_rate	float	Y	龙虎榜净买额占比
            # amount_rate	float	Y	龙虎榜成交额占比
            # float_values	float	Y	当日流通市值
            # reason	str	Y	上榜理由

            df1 = pro.query('top_list', trade_date=dateS)
            time.sleep(1) #60 ci /sec
            print("df_1 len "+str(df1.__len__()))
            df_top_list = df1.append(df_top_list)

            # 龙虎榜机构明细
            # trade_date	str	Y	交易日期
            # ts_code	str	Y	TS代码
            # exalter	str	Y	营业部名称
            # buy	float	Y	买入额（万）
            # buy_rate	float	Y	买入占总成交比例
            # sell	float	Y	卖出额（万）
            # sell_rate	float	Y	卖出占总成交比例
            # net_buy	float	Y	净成交额（万）
            df2 = pro.query('top_inst', trade_date=dateS)
            print("df_2 len "+str(df2.__len__()))
            df_top_inst = df2.append(df_top_inst)


    df_top_list.to_csv(csv_f_top_list, encoding='UTF-8', index=False)
    logging.info("top_list saved to "+csv_f_top_list+" , len "+ str(df_top_list.__len__()) )


    df_top_inst.to_csv(csv_f_top_inst, encoding='UTF-8', index=False)
    logging.info("top_inst saved to "+csv_f_top_inst+" , len "+str(df_top_inst.__len__()) )

    return()



# buy the stock in top_list at the next day(T+1) opening, sell at two days(T+2) closing.
# evaluated ATR, Profit
# pft_2, pft_3, pft_5
def merge_top_list_inst():
    df = pd.DataFrame(columns=["trade_date", "code", "e1", "e2", "e3", "e4", "e5"])

    csv_f_top_list = dir + "/top_list_" + str(n_days) + "_days.csv"
    csv_f_top_inst = dir + "/top_inst_" + str(n_days) + "_days.csv"

    df_top_list = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_list)
    df_top_inst = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_inst)

    n_days_before = (datetime.datetime.today() - datetime.timedelta(days=5)).strftime("%Y%m%d")

    #ryan debug
    #n_days_before = "20200806"

    df_top_list = df_top_list[df_top_list['trade_date'] <= n_days_before].reset_index().drop('index', axis=1)
    df_top_inst = df_top_inst[df_top_inst['trade_date'] <= n_days_before].reset_index().drop('index', axis=1)

    df_gar = df_top_list[(df_top_list['reason'] =='退市整理的证券') | (df_top_list['reason'] =='退市整理期') ]
    df_top_list = finlib.Finlib()._df_sub_by_code(df=df_top_list, df_sub=df_gar)
    df_top_inst = finlib.Finlib()._df_sub_by_code(df=df_top_inst, df_sub=df_gar)

    for i in range(df_top_list.__len__()):
        code_m = df_top_list.iloc[i]['code']
        on_date = df_top_list.iloc[i]['trade_date']

        d1 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=1)).strftime("%Y%m%d")
        d2 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=2)).strftime("%Y%m%d")
        d3 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=3)).strftime("%Y%m%d")
        d4 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=4)).strftime("%Y%m%d")
        d5 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=5)).strftime("%Y%m%d")

        p0 = finlib.Finlib().get_price(code_m=code_m, date=on_date)
        p1 = finlib.Finlib().get_price(code_m=code_m, date=d1)
        p2 = finlib.Finlib().get_price(code_m=code_m, date=d2)
        p3 = finlib.Finlib().get_price(code_m=code_m, date=d3)
        p4 = finlib.Finlib().get_price(code_m=code_m, date=d4)
        p5 = finlib.Finlib().get_price(code_m=code_m, date=d5)

        e1 = round(100* (p1 - p0)/p0, 0)
        e2 = round(100* (p2 - p0)/p0, 0)
        e3 = round(100* (p3 - p0)/p0, 0)
        e4 = round(100* (p4 - p0)/p0, 0)
        e5 = round(100* (p5 - p0)/p0, 0)

        df = df.append({
            "trade_date": on_date,
            "code": code_m,
            "e1": e1,
            "e2": e2,
            "e3": e3,
            "e4": e4,
            "e5": e5,
        }, ignore_index=True)

        print(1)
    df_top_list = pd.merge(left=df_top_list, right=df, on=['trade_date', "code"], how="inner") #adding e1-e5
    df_top_inst = pd.merge(left=df_top_inst, right=df_top_list, on=['trade_date', "code"], how="inner")


    # df_top_list[(df_top_list['reason'] =='无价格涨跌幅限制的证券') ].mean()
    # df_top_list[(df_top_list['reason'] =='异常期间价格涨幅偏离值累计达到15.39%') ].mean()

    df_top_list.to_csv(csv_f_top_list)
    logging.info("top_list with earn saved to "+csv_f_top_list+" , len "+ str(df_top_list.__len__()) )

    df_top_inst.to_csv(csv_f_top_inst)
    logging.info("top_inst with earn saved to "+csv_f_top_inst+" , len "+ str(df_top_inst.__len__()) )

    #
    # for r in df_top_list['reason'].drop_duplicates():
    #     print(r)
    #     print(df_top_list[(df_top_list['reason'] == r)]['e1'].mean())
    # #
    #0                                       日跌幅偏离值达到7%的前五只证券
    # 2                                       日涨幅偏离值达到7%的前五只证券
    # 6                                        日换手率达到20%的前五只证券
    # 9                               连续三个交易日内，涨幅偏离值累计达到20%的证券
    # 16                                       日振幅值达到15%的前五只证券
    # 28                      连续三个交易日内，涨幅偏离值累计达到12%的ST证券、*ST证券
    # 29                                                 退市整理期
    # 67                              连续三个交易日内，跌幅偏离值累计达到20%的证券
    # 89                                           无价格涨跌幅限制的证券
    # 90                                               退市整理的证券
    # 91                非ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券
    # 93                         有价格涨跌幅限制的日收盘价格跌幅偏离值达到7%的前三只证券
    # 95                         有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前三只证券
    # 100                            有价格涨跌幅限制的日价格振幅达到15%的前三只证券
    # 102                             有价格涨跌幅限制的日换手率达到20%的前三只证券
    # 114                             有价格涨跌幅限制的日换手率达到30%的前五只证券
    # 130             连续三个交易日内，跌幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券
    # 131             连续三个交易日内，涨幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券
    # 257    连续三个交易日内，日均换手率与前五个交易日的日均换手率的比值达到30倍，且换手率累计达20%的证券
    # 262                ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券
    # 304                              当日无价格涨跌幅限制的A股，出现异常波动停牌的
    # 337                                异常期间价格涨幅偏离值累计达到15.39%
    # 433               非ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券
    # 455                          有价格涨跌幅限制的日收盘价格涨幅达到15%的前五只证券


    return()



# PCA analysis, Line Regressino
def pca_analysis():

    csv_f_top_list = dir + "/top_list_" + str(n_days) + "_days.csv"
    csv_f_top_inst = dir + "/top_inst_" + str(n_days) + "_days.csv"

    df_top_list = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_list)
    df_top_inst = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_inst) #17905*26

    #df_top_inst['exalter'].unique().__len__() : 2676
    #top 100 frequence exalter
    top_freq_exalter_list = df_top_inst['exalter'].value_counts(sort=True).head(100).index.to_list()
    df_top_inst = df_top_inst[df_top_inst['exalter'].isin(top_freq_exalter_list)] #8272*26


    df_exalter = pd.get_dummies(data=df_top_inst['exalter'])    #8272*100
    df_top_inst = df_top_inst.join(df_exalter) #8272*126
    df_top_inst = df_top_inst.fillna(0)

    features = df_top_inst.columns.drop(['trade_date','code', 'name', 'close','exalter','e2','e3','e4','e5', 'reason']).to_list()

### LR

    df_win = df_top_inst[(df_top_inst['e1'] > 3)]
    df_win = df_win[features]
    df_lose = df_top_inst[(df_top_inst['e1'] < 3)]
    df_lose = df_lose[features]

    from sklearn.linear_model import LogisticRegression
    # all parameters not specified are set to their defaults
    # default solver is incredibly slow which is why it was changed to 'lbfgs'
    logisticRegr = LogisticRegression(solver='lbfgs')

    #split trainset
    import numpy as np
    from sklearn.model_selection import train_test_split
    df_win_train = train_test_split(df_win)

    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    # Fit on training set only.
    scaler.fit(df_win_train)


    logisticRegr.fit(train_img, train_lbl)



    ### PCA
    from sklearn.preprocessing import StandardScaler
    df = df_top_inst

    # Separating out the features
    x = df.loc[:, features].values
    # Separating out the target
    y = df.loc[:, ['e1']].values
    # Standardizing the features
    x = StandardScaler().fit_transform(x)
    print(1)


    from sklearn.decomposition import PCA
    pca = PCA(n_components=100)
    principalComponents = pca.fit_transform(x)
    print(pca.explained_variance_ratio_)
    principalDf = pd.DataFrame(data = principalComponents, columns = ['principal component 1', 'principal component 2'])

### MAIN ####
if __name__ == '__main__':
    fetch_top_list_inst() #step1
    merge_top_list_inst() #step2
    pca_analysis() #step3
