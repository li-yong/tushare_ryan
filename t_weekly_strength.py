# coding: utf-8

import sys, traceback, threading
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
from optparse import OptionParser
import sys
import os
import logging
import signal

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#from datetime import datetime, timedelta

# This script Run every week to get the fundamental info with tushare pro.
USD_DIV_CNY = 7

global debug_global
global force_run_global
global myToken
global fund_base
global fund_base_source
global fund_base_merged
global fund_base_report
global fund_base_tmp

global csv_income
global csv_balancesheet
global csv_cashflow
global csv_forecast
global csv_dividend
global csv_express
global csv_fina_indicator
global csv_fina_audit
global csv_fina_mainbz
global csv_fina_mainbz_sum
global csv_disclosure_date
global csv_basicmaai

global fund_base_latest

global csv_income_latest
global csv_balancesheet_latest
global csv_cashflow_latest
global csv_forecast_latest
global csv_express_latest
global csv_dividend_latest
global csv_fina_indicator_latest
global csv_fina_audit_latest
global csv_fina_mainbz_latest
global csv_fina_mainbz_sum_latest
global csv_disclosure_date_latest
global csv_disclosure_date_latest_notify
global csv_fina_mainbz_latest_percent

global col_list_income

global col_list_balancesheet

global col_list_cashflow
global col_list_forecast
global col_list_dividend
global col_list_express

global col_list_fina_indicator
global col_list_fina_audit
global col_list_fina_mainbz
global col_list_disclosure_date

global query_fields_income
global query_fields_balancesheet
global query_fields_cashflow
global query_fields_fina_indicator
global query_fields_forecast
global query_fields_dividend
global query_fields_express
global query_fields_fina_audit
global query_fields_fina_mainbz
global query_fields_disclosure_date

#pd.set_option('display.height', 1000)
#pd.set_option('display.max_rows', 500)
#pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1000)

global df_all_ts_pro
global df_all_jaqs
global big_memory_global


def set_global(debug=False, big_memory=False, force_run=False):
    global debug_global
    global force_run_global
    global myToken
 
    global df_all_ts_pro
    global df_all_jaqs
    global big_memory_global

    ### Global Variables ####

    myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'
    fund_base = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"


    debug_global = False
    force_run_global = False
    big_memory_global = False

    if force_run:
        force_run_global = True

    if debug:
        debug_global = True
        fund_base_source = fund_base + "/source.dev"
        fund_base_merged = fund_base + "/merged.dev"  # modify global fund_base_merged to dev
        fund_base_report = fund_base + "/report.dev"

    if big_memory:
        big_memory_global = True
        df_all_jaqs = finlib.Finlib().load_all_jaqs()
        df_all_ts_pro = finlib.Finlib().load_all_ts_pro(debug=debug, overwrite=True)

 
   
def get_beneish_element(ann_date):
    f = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + ann_date + ".csv"
    df = pd.read_csv(f, converters={'end_date': str})
    
    # Net Sales  净销售额  | c_fr_sale_sg, 销售商品、提供劳务收到的现金
    # Cost of Goods  货品的成本 |  total_cogs, 营业总成本 |  cogs_of_sales, 销售成本率
    # Net Receivables  净应收账款 | accounts_receiv, 应收账款| acc_receivable, 应收款项, payables, 	应付款项
    # Current Assets  流动资产|total_cur_assets, 流动资产合计
    # Property, Plant and Equipment 物业，厂房及设备 | fix_assets, 固定资产
    # Depreciation  折旧|daa,折旧与摊销, depr_fa_coga_dpba, 固定资产折旧、油气资产折耗、生产性生物资产折旧
    # Total Assets 总资产|total_assets, 资产总计
    # Selling, General and Administrative Expenses, 销售，一般和行政费用| sell_exp 减:销售费用  admin_exp	减:管理费用, fin_exp 减:财务费用, rd_exp 研发费用?   
    # Net Income 净收入 | n_income 净利润(含少数股东损益)    
    # Cash Flow from Operations 运营现金流| n_cashflow_act, 经营活动产生的现金流量净额    
    # Current Liabilities 流动负债 | total_cur_liab	 流动负债合计    
    # Long-Term Debt  长期债务 | total_ncl, 非流动负债合计    
    
    df['sga_exp'] = df['sell_exp']+df['admin_exp']+df['fin_exp'] 
    #df['sga_exp'] = df['sell_exp']+df['admin_exp']+df['fin_exp']+df['rd_exp']
    
    #cols = ['name','ts_code','end_date','c_fr_sale_sg','total_cogs','accounts_receiv','total_cur_assets','fix_assets','daa','depr_fa_coga_dpba','total_assets', 'sga_exp', 'sell_exp','admin_exp','fin_exp','rd_exp','n_income','n_cashflow_act','total_cur_liab','total_ncl']

    cols = ['name', 'ts_code','end_date','c_fr_sale_sg','total_cogs','accounts_receiv','total_cur_assets','fix_assets','depr_fa_coga_dpba','total_assets','sga_exp','sell_exp','admin_exp','fin_exp','n_income','n_cashflow_act','total_cur_liab','total_ncl']
    
    df = df[cols]

    return(df)

def beneish_calc(ts_code,name, ann_date, dt, dt_1):
    dict_rtn = {}
    #turn_days,营业周期


 #    dt_1 = {'c_fr_sale_sg': 93823,
 # 'total_cogs': 52155,
 # 'accounts_receiv': 1174,
 # 'total_cur_assets': 73717,
 # 'fix_assets': 2532,
 # 'daa': 1696,
 # 'total_assets': 86291,
 # 'sga_exp': 32426,
 # 'n_income': 5741,
 # 'n_cashflow_act': 8416,
 # 'total_cur_liab': 26297,
 # 'total_ncl': 1232}
 #
 #
 #    dt = {'c_fr_sale_sg': 93685,
 # 'total_cogs': 49193,
 # 'accounts_receiv': 1373,
 # 'total_cur_assets': 67991,
 # 'fix_assets': 2058,
 # 'daa': 1716,
 # 'total_assets': 84832,
 # 'sga_exp': 33013,
 # 'n_income': 9888,
 # 'n_cashflow_act': 2877,
 # 'total_cur_liab': 26275,
 # 'total_ncl': 1470}

    
    #DSRI Days Sales in Receivables Index, 应收账款周转指数
    # DSRI = (Net Receivablest / Salest) / Net Receivablest-1 / Salest-1)
    if (dt['accounts_receiv'] == 0.0 )   or\
            (dt_1['accounts_receiv'] == 0.0 )  or\
            (dt['c_fr_sale_sg'] == 0.0 ) or\
            (dt_1['c_fr_sale_sg'] == 0.0 ):
        DSRI = 0
    else:
        _1 = dt['accounts_receiv']/dt['c_fr_sale_sg']
        _2 = dt_1['accounts_receiv']/dt_1['c_fr_sale_sg']
        DSRI = _1/_2
    dict_rtn['DSRI'] = DSRI
    
    #GMI Gross Margin Index, 毛利率指数
    #GMI = [(Salest-1 - COGSt-1) / Salest-1] / [(Salest - COGSt) / Salest]    
    _1 = (dt['c_fr_sale_sg'] - dt['total_cogs'])/- dt['c_fr_sale_sg']
    _2 = (dt_1['c_fr_sale_sg'] - dt_1['total_cogs'])/- dt_1['c_fr_sale_sg']
    GMI = _1/_2   
    dict_rtn['GMI'] = GMI
    
    #AQI Asset Quality Index,  资产质量指数
    #AQI = [1 - (Current Assetst + PP&Et + Securitiest) / Total Assetst] / [1 - ((Current Assetst-1 + PP&Et-1 + Securitiest-1) / Total Assetst-1)]
    _1 = (dt['total_assets']-dt['fix_assets']-dt['total_cur_assets'])/dt['total_assets']
    _2 = (dt_1['total_assets']-dt_1['fix_assets']-dt_1['total_cur_assets'])/dt_1['total_assets']
    AQI = _1/_2    
    dict_rtn['AQI'] = AQI
    
    
    #SGI Sales Growth Index, 销售增长指数
    #SGI = Salest / Salest-1
    SGI = dt['c_fr_sale_sg']/dt_1['c_fr_sale_sg']
    dict_rtn['SGI'] = SGI
    
    #DEPI Depreciation Index, 折旧指数
    #DEPI = (Depreciationt-1/ (PP&Et-1 + Depreciationt-1)) / (Depreciationt / (PP&Et + Depreciationt))
    _1 = dt['daa']/(dt['daa']+dt['fix_assets'])    
    _2 = dt_1['daa']/(dt_1['daa']+dt_1['fix_assets'])
    DEPI = _1/_2 
    dict_rtn['DEPI'] = DEPI
    
    #SGAI Sales, General and Administrative Expenses Index,销售费用指数
    #SGAI = (SG&A Expenset / Salest) / (SG&A Expenset-1 / Salest-1)
    _1 = dt['sga_exp']/dt['c_fr_sale_sg']
    _2 = dt_1['sga_exp']/dt_1['c_fr_sale_sg']
    SGAI = _1/_2
    dict_rtn['SGAI'] = SGAI
    
    #TATA Total Accruals to Total Assets, 应计总数与总资产比率
    #TATA = (Income from Continuing Operationst - Cash Flows from Operationst) / Total Assetst
    TATA = (dt_1['n_income']-dt_1['n_cashflow_act'])/dt_1['total_assets']    
    dict_rtn['TATA'] = TATA
    
    #LVGI  Leverage Index,
    #LVGI = [(Current Liabilitiest + Total Long Term Debtt) / Total Assetst] / [(Current Liabilitiest-1 + Total Long Term Debtt-1) / Total Assetst-1]
    _1 = (dt['total_ncl']+dt['total_cur_liab'])/dt['total_assets']
    _2 = (dt_1['total_ncl']+dt_1['total_cur_liab'])/dt_1['total_assets']
    LVGI = _1/_2 
    dict_rtn['LVGI'] = LVGI
    
    
    logging.info("DSRI:"+str(round(DSRI,2)))
    logging.info("GMI:"+str(round(GMI,2)))
    logging.info("AQI:"+str(round(AQI,2)))
    logging.info("SGI:"+str(round(SGI,2)))
    logging.info("DEPI:"+str(round(DEPI,2)))
    logging.info("SGAI:"+str(round(SGAI,2)))
    logging.info("TATA:"+str(round(TATA,2)))
    logging.info("LVGI:"+str(round(LVGI,2)))
    logging.info("          ")
    
    M_5v= -6.065+ .823*DSRI + .906*GMI + .593*AQI + .717*SGI + .107*DEPI
    M_8v= -4.84 + .920* DSRI + .528* GMI + .404* AQI + .892* SGI + .115* DEPI -.172* SGAI  + 4.679*TATA - .327* LVGI
    dict_rtn['M_5v'] = M_5v
    dict_rtn['M_8v'] = M_8v
    
    logging.info(str(ts_code)+ " "+name+" "+str(ann_date)   +" M_5v:"+str(round(M_5v,2)))
    logging.info(str(ts_code)+ " "+name+" "+str(ann_date)   +" M_8v:"+str(round(M_8v,2)))
    
    if M_8v > 3 :
         logging.info("WARNNING: M_8v great than 3"+ str(ts_code) + " "+str(ann_date) + " "+str(round(M_8v,2)))
         
    if M_5v > 3 :
         logging.info("WARNNING: M_5v great than 3"+ str(ts_code) + " "+str(ann_date) + " "+str(round(M_5v,2))) 
         
    return(dict_rtn)
    
    
def main():
    ########################
    #
    #########################

    logging.info(__file__ + " " + "\n")
    logging.info(__file__ + " " + "SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("--big_memory", action="store_true", dest="big_memory_f", default=False, help="consumes 4G memory to load all the jaqs and tushare data to two df")

    parser.add_option("-u", "--debug", action="store_true", dest="debug_f", default=False, help="debug mode, using merge.dev, report.dev folder")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")
    
    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(AG)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/ ")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")

    #parser.add_option("-v", "--verify_fund_increase", action="store_true",
    #                  dest="verify_fund_increase_f", default=False,
    #                  help="verify quartly score and buy and increase to today")

    (options, args) = parser.parse_args()
    big_memory_f = options.big_memory_f
    debug_f = options.debug_f
    force_run_f = options.force_run_f
    
    selected = options.selected
    stock_global = options.stock_global

    #verify_fund_increase_f = options.verify_fund_increase_f

    logging.info(__file__ + " " + "big_memory_f: " + str(big_memory_f))
    logging.info(__file__ + " " + "debug_f: " + str(debug_f))
    logging.info(__file__ + " " + "force_run_f: " + str(force_run_f))
    logging.info(__file__ + " " + "selected: " + str(selected))
    logging.info(__file__ + " " + "stock_global: " + str(stock_global))

    set_global(debug=debug_f, big_memory=big_memory_f, force_run=force_run_f)


    ts.set_token(myToken)
    pro = ts.pro_api()


    ts_code='600519.SH'
    ts_code='000651.SZ'
    ann_date = '20191231'
    ann_date = finlib.Finlib().get_year_month_quarter()['ann_date_1y_before']
    field = 'accounts_receiv'
    big_memory = False
    df_all_ts_pro = None
    fund_base_merged = None

    df = pro.income(ts_code=ts_code, period=ann_date, fields='ts_code,ann_date,revenue')
    df = pro.fina_indicator(ts_code=ts_code, period=ann_date, fields='ts_code,ann_date,rd_exp,daa')
    #df = pro.fina_indicator(ts_code=ts_code, period=ann_date, fields='ts_code,ann_date,daa')
    print(1)

    #
    # df_1 = pro.index_basic(market='SSE')
    # df_1 = pro.index_basic(ts_code='000001.SH')
    # df_1 = pro.index_weight(index_code = '000001.SH',trade_date='20200731')
    # df_1 = df_1.sort_values(by='weight', ascending=False)
    # df_1 = pro.index_dailybasic(trade_date='20200807', fields='ts_code,trade_date,turnover_rate,pe')
    #
    # df0 = pro.index_weight(index_code='399300.SZ', start_date='20180901', end_date='20180930')
    # df1 = pro.index_weight(trade_date='20200807')


    
    #净利润
    net_profit =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date,
                                                         field='net_profit', big_memory=big_memory,
                                                         df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    #营业总收入
    revenue =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date,
                                                         field='total_revenue', big_memory=big_memory,
                                                         df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    #经营活动产生的现金流量净额
    n_cashflow_act =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date,
                                                         field='n_cashflow_act', big_memory=big_memory,
                                                         df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)

    #净利润/营业总收入, return percent number.  49.48 == 49.48%  == net_profit*100.0/n_cashflow_act
    profit_to_gr =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='profit_to_gr', big_memory=big_memory, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)



    ##### Benneish start
    rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
    out_dir = rst['out_dir']
    csv_dir = rst['csv_dir']
    stock_list = rst['stock_list']
    out_f = out_dir + "/" + stock_global.lower() + "_curve_shape.csv"  # /home/ryan/DATA/result/selected/us_index_fib.csv

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    df = get_beneish_element(ann_date=ann_date)
    current_date = datetime.datetime.strptime(ann_date, "%Y%m%d")
    df_1 = get_beneish_element(ann_date=str(current_date.year - 1) + '1231')

    i = 0
    stock_list = finlib.Finlib().add_market_to_code(df=stock_list, dot_f=True, tspro_format=True)

    for index, row in stock_list.iterrows():
        i += 1
        print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
        name, ts_code = row['name'], row['code']
        logging.info(str(ts_code)+" "+name)
        #ts_code: 600519.SH

        dt =  df[df['ts_code'] == ts_code].reset_index().drop('index', axis=1).iloc[0].to_dict()
        dt_1 =  df_1[df_1['ts_code'] == ts_code].reset_index().drop('index', axis=1).iloc[0].to_dict()

        dict_rtn = beneish_calc(ts_code=ts_code,name=name,ann_date=ann_date,dt=dt, dt_1=dt_1)


    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
