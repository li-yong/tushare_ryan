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
import akshare as ak
import tabulate
import finlib

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#from datetime import datetime, timedelta

# This script Run every week to get the fundamental info with tushare pro.
#pd.set_option('display.height', 1000)
#pd.set_option('display.max_rows', 500)
#pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1000)


def fetch_ak(api, note, parms):
    logging.info("\nfetching " + api + " " + note)

    base_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/akshare/source"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    csv_f = base_dir + "/" + api + "_" + note + ".csv"

    if finlib.Finlib().is_cached(csv_f, day=2):
            logging.info("file updated in 2 days, not fetch. "+csv_f)

    try:
        cmd = "ak."+api+"("+parms+")"
        df = eval(cmd)
        if df is not None:
            df.to_csv(csv_f, encoding='UTF-8')
            logging.info("fetched "+api+" "+note+", "+csv_f)
            logging.info(tabulate.tabulate(df.head(1), headers='keys', tablefmt='psql'))
        else:
            logging.warning("df is None")
    except e:
        logging.warning("Exception on "+ api + ", "+str(e))



def main():
    ########################
    #
    #################
    
    #企业社会责任
    fetch_ak(api = 'stock_zh_a_scr_report', note = '企业社会责任', parms='report_year=2019,page=1')

    #机构调研
    fetch_ak(api = 'stock_em_jgdy_tj', note = '机构调研-统计', parms='')
    fetch_ak(api = 'stock_em_jgdy_detail', note = '机构调研-详细', parms='')
    
    #股票质押
    fetch_ak(api = 'stock_em_gpzy_profile', note = '股权质押市场概况', parms='')
    fetch_ak(api = 'stock_em_gpzy_pledge_ratio', note = '上市公司质押比例', parms='')
    fetch_ak(api = 'stock_em_gpzy_pledge_ratio_detail', note = '重要股东股权质押明细', parms='')
    fetch_ak(api = 'stock_em_gpzy_distribute_statistics_company', note = '质押机构分布统计证券公司', parms='')
    fetch_ak(api = 'stock_em_gpzy_distribute_statistics_bank', note = '质押机构分布统计银行', parms='')
    fetch_ak(api = 'stock_em_gpzy_industry_data', note = '上市公司质押比例', parms='')
    
    #商誉专题
    fetch_ak(api = 'stock_em_sy_profile', note = 'A股商誉市场概况', parms='')
    fetch_ak(api = 'stock_em_sy_yq_list', note = '商誉减值预期明细', parms='')
    fetch_ak(api = 'stock_em_sy_jz_list', note = '个股商誉减值明细', parms='symbol="沪市主板", trade_date="2019-12-31"')
    fetch_ak(api = 'stock_em_sy_list', note = '个股商誉明细', parms='symbol="沪市主板", trade_date="2019-12-31"')
    fetch_ak(api = 'stock_em_sy_hy_list', note = '行业商誉明细', parms='trade_date="2019-12-31"')
    
    #分析师指数
    fetch_ak(api = 'stock_em_analyst_rank', note = '分析师指数最新排行', parms='')
    
    
    fetch_ak(api = 'stock_em_comment', note = '千股千评', parms='')
    
    #沪深港通持股
    fetch_ak(api = 'stock_em_hsgt_north_net_flow_in', note = '北向净流入', parms='indicator="北上"')
    fetch_ak(api = 'stock_em_hsgt_north_cash', note = '北向资金余额', parms='indicator="北上"')
    fetch_ak(api = 'stock_em_hsgt_north_acc_flow_in', note = '北向累计净流入', parms='indicator="北上"')
    fetch_ak(api = 'stock_em_hsgt_south_net_flow_in', note = '南向净流入', parms='indicator="南下"')
    fetch_ak(api = 'stock_em_hsgt_south_cash', note = '南向资金余额', parms='indicator="南下"')
    fetch_ak(api = 'stock_em_hsgt_south_acc_flow_in', note = '南向累计净流入', parms='indicator="南下"')
    fetch_ak(api = 'stock_em_hsgt_hold_stock', note = '个股排行', parms='market="北向", indicator="3日排行"')
    fetch_ak(api = 'stock_em_hsgt_stock_statistics', note = '每日个股统计', parms='market="北向持股"')
    
    #资金流向
    fetch_ak(api = 'stock_individual_fund_flow_rank', note = '个股资金流排名', parms='indicator="3日"')
    fetch_ak(api = 'stock_market_fund_flow', note = '大盘资金流', parms='')
    fetch_ak(api = 'stock_sector_fund_flow_rank', note = '板块资金流排名', parms='indicator="5日", sector_type="概念资金流"')
    
    
    
    fetch_ak(api = 'stock_history_dividend', note = '历史分红', parms='')
    fetch_ak(api = 'stock_sector_spot', note = '板块行情', parms='indicator="概念"')
    fetch_ak(api = 'stock_info_a_code_name', note = '股票列表-A股', parms='')
    fetch_ak(api = 'stock_info_sh_name_code', note = '股票列表-上证-主板A股', parms='indicator="主板A股"')
    fetch_ak(api = 'stock_info_sh_name_code', note = '股票列表-上证-主板B股', parms='indicator="主板B股"')
    fetch_ak(api = 'stock_info_sh_name_code', note = '股票列表-上证-科创板', parms='indicator="科创板"')
    
    
    fetch_ak(api = 'stock_info_sz_name_code', note = '股票列表-深证-A股列表', parms='indicator="A股列表"')
    fetch_ak(api = 'stock_info_sz_name_code', note = '股票列表-深证-B股列表', parms='indicator="B股列表"')
    fetch_ak(api = 'stock_info_sz_name_code', note = '股票列表-深证-中小企业板', parms='indicator="中小企业板"')
    fetch_ak(api = 'stock_info_sz_name_code', note = '股票列表-深证-创业板', parms='indicator="创业板"')
    fetch_ak(api = 'stock_info_sz_name_code', note = '股票列表-深证-上市公司列表', parms='indicator="上市公司列表"')

    fetch_ak(api = 'stock_institute_hold', note = '机构持股一览表', parms='quarter="20203"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-行业关注度', parms='indicator="行业关注度"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-机构关注度', parms='indicator="机构关注度"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-最新投资评级', parms='indicator="最新投资评级"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-上调评级股票', parms='indicator="上调评级股票"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-下调评级股票', parms='indicator="下调评级股票"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-股票综合评级', parms='indicator="股票综合评级"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-首次评级股票', parms='indicator="首次评级股票"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-目标涨幅排名', parms='indicator="目标涨幅排名"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-投资评级选股', parms='indicator="投资评级选股"')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-', parms='indicator=""')
    fetch_ak(api = 'stock_institute_recommend', note = '机构推荐池-', parms='indicator=""')
    
    
    
    fetch_ak(api = 'stock_js_price', note = '美港目标价', parms='category="us"')
    fetch_ak(api = 'stock_js_price', note = '美港目标价', parms='category="hk"')
    fetch_ak(api = 'stock_em_qsjy', note = '券商业绩月报-202006', parms='trade_date="2020-06-01"')
    fetch_ak(api = 'stock_em_qsjy', note = '券商业绩月报-202007', parms='trade_date="2020-07-01"')


    fetch_ak(api = 'stock_a_high_low_statistics', note = '创新高和新低的股票数量', parms='market="all"')
    fetch_ak(api = 'stock_a_below_net_asset_statistics', note = '破净股统计', parms='')
    
    
    fetch_ak(api = 'stock_report_fund_hold', note = '基金持股', parms='symbol="基金持仓", date="20200630"')
    fetch_ak(api = 'stock_sina_lhb_detail_daily', note = '龙虎榜-每日详情', parms='trade_date="20200730", symbol="涨幅偏离值达7%的证券"')
    fetch_ak(api = 'stock_sina_lhb_ggtj', note = '龙虎榜-个股上榜统计', parms='recent_day="5"')
    fetch_ak(api = 'stock_sina_lhb_yytj', note = '龙虎榜-营业上榜统计', parms='recent_day="5"')
    fetch_ak(api = 'stock_sina_lhb_jgzz', note = '龙虎榜-机构席位追踪', parms='recent_day="5"')
    fetch_ak(api = 'stock_sina_lhb_jgmx', note = '龙虎榜-机构席位成交明细', parms='')

    #fetch_ak(api = '', note = '', parms='')
   

    print(1)



    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
