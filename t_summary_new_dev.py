# coding: utf-8
# encoding= utf-8

import sys

#import imp #not compatible with python3
#imp.reload(sys) #not compatible with python3
#sys.setdefaultencoding('utf-8') #not compatible with python3

import pandas as pd
import re
import itertools
import os
import finlib
import tushare as ts
from sqlalchemy import create_engine
import mysql.connector

from datetime import datetime, timedelta
from optparse import OptionParser
import logging
from tabulate import tabulate
import constant

global stock_global

pd.set_option("display.max_rows", 9999)
pd.set_option("display.max_columns", 100)
pd.set_option('expand_frame_repr', False)


result = '/home/ryan/DATA/result'
result_today = result + '/today'
result_selected = result + '/selected'
result_wpls = result + '/wei_pan_la_sheng'

arr = []

selected_dict = {
    'AG': {
        'df_very_strong_down_trend_selected': {
            'file': result_selected + '/ag_junxian_barstyle_very_strong_down_trend.csv', 'column': '', 'kw': '',
            "term": "SHORT TERM", "price": "NA"},
        'df_very_strong_up_trend_selected': {'file': result_selected + '/ag_junxian_barstyle_very_strong_up_trend.csv',
                                             'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_jincha_major_selected': {'file': result_selected + '/ag_junxian_barstyle_jincha_major.csv', 'column': '',
                                     'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_jincha_minor_selected': {'file': result_selected + '/ag_junxian_barstyle_jincha_minor.csv', 'column': '',
                                     'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_duotou_pailie_selected': {'file': result_selected + '/ag_junxian_barstyle_duotou_pailie.csv', 'column': '',
                                      'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_yunxian_buy_selected': {'file': result_selected + '/ag_junxian_barstyle_yunxian_buy.csv', 'column': '',
                                    'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_yunxian_sell_selected': {'file': result_selected + '/ag_junxian_barstyle_yunxian_sell.csv', 'column': '',
                                     'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_fib_ag_selected': {'file': result_selected + '/key_points_ag_today_selected.csv', 'column': '', 'kw': '',
                               "term": "SHORT TERM", "price": "NA"},
        'df_pv_no_filter_ag_selected': {'file': result_selected + '/talib_and_pv_no_db_filter_ag.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_fib_ag_selected': {'file': result_selected + '/ag_fib.csv', 'column': '', 'kw': '', "term": "SHORT TERM",
                               "price": "NA"},
        'df_fib_ag_index_selected': {'file': result_selected + '/ag_index_fib.csv', 'column': '', 'kw': '',
                                     "term": "SHORT TERM", "price": "NA"},
        'df_moneyflow_top_amt_perc_selected': {'file': result_selected + '/mf_today_top5_large_amount.csv',
                                               'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
        'xxxx': {'file': result_selected + '/xxxx.csv', 'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
    },  # end of AG selected

    'US': {
        'df_jincha_minor_selected_us': {'file': result_selected + '/us_junxian_barstyle_jincha_minor.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_jincha_major_selected_us': {'file': result_selected + '/us_junxian_barstyle_jincha_major.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_very_strong_up_trend_selected_us': {
            'file': result_selected + '/us_junxian_barstyle_very_strong_up_trend.csv', 'column': '', 'kw': '',
            "term": "SHORT TERM", "price": "NA"},
        'df_very_strong_down_trend_selected_us': {
            'file': result_selected + '/us_junxian_barstyle_very_strong_down_trend.csv', 'column': '', 'kw': '',
            "term": "SHORT TERM", "price": "NA"},
        'df_duotou_pailie_selected_us': {'file': result_selected + '/us_junxian_barstyle_duotou_pailie.csv',
                                         'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_yunxian_buy_selected_us': {'file': result_selected + '/us_junxian_barstyle_yunxian_buy.csv', 'column': '',
                                       'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_yunxian_sell_selected_us': {'file': result_selected + '/us_junxian_barstyle_yunxian_sell.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_fib_us_index_selected': {'file': result_selected + '/us_index_fib.csv', 'column': '', 'kw': '',
                                     "term": "SHORT TERM", "price": "NA"},
        'df_fib_us_selected': {'file': result_selected + '/us_fib.csv', 'column': '', 'kw': '', "term": "SHORT TERM",
                               "price": "NA"},
        'df_key_points_us_selected': {'file': result_selected + '/key_points_us_today_selected.csv', 'column': '',
                                      'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_pv_no_filter_us_selected': {'file': result_selected + '/talib_and_pv_no_db_filter_us.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'xxxx': {'file': result_selected + '/xxxx.csv', 'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},

    },  # end of US select
    'HK': {
        'df_key_points_hk_selected': {'file': result_selected + '/key_points_hk_today_selected.csv', 'column': '',
                                      'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_pv_no_filter_hk_selected': {'file': result_selected + '/talib_and_pv_no_db_filter_hk.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_yunxian_sell_selected_hk': {'file': result_selected + '/hk_junxian_barstyle_yunxian_sell.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_yunxian_buy_selected_hk': {'file': result_selected + '/hk_junxian_barstyle_yunxian_buy.csv', 'column': '',
                                       'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_duotou_pailie_selected_hk': {'file': result_selected + '/hk_junxian_barstyle_duotou_pailie.csv',
                                         'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_jincha_minor_selected_hk': {'file': result_selected + '/hk_junxian_barstyle_jincha_minor.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_jincha_major_selected_hk': {'file': result_selected + '/hk_junxian_barstyle_jincha_major.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_very_strong_up_trend_selected_hk': {
            'file': result_selected + '/hk_junxian_barstyle_very_strong_up_trend.csv', 'column': '', 'kw': '',
            "term": "SHORT TERM", "price": "NA"},
        'df_very_strong_down_trend_selected_hk': {
            'file': result_selected + '/hk_junxian_barstyle_very_strong_down_trend.csv', 'column': '', 'kw': '',
            "term": "SHORT TERM", "price": "NA"},
        'df_key_points_hk_selected': {'file': result_selected + '/key_points_hk_today_selected.csv', 'column': '',
                                      'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_pv_no_filter_hk_selected': {'file': result_selected + '/talib_and_pv_no_db_filter_hk.csv', 'column': '',
                                        'kw': '', "term": "SHORT TERM", "price": "NA"},
        'df_fib_hk_selected': {'file': result_selected + '/hk_fib.csv', 'column': '', 'kw': '', "term": "SHORT TERM",
                               "price": "NA"},
        'xxxx': {'file': result_selected + '/xxxx.csv', 'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
    },  # end of HK select
}

not_selected_dict = {
    'AG': {
        # 'df_yunxian_sell':{'file':result+'/ag_junxian_barstyle_yunxian_sell.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_yunxian_buy':{'file':result+'/ag_junxian_barstyle_yunxian_buy.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_duotou_pailie':{'file':result+'/ag_junxian_barstyle_duotou_pailie.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_jincha_minor':{'file':result+'/ag_junxian_barstyle_jincha_minor.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_jincha_major':{'file':result+'/ag_junxian_barstyle_jincha_major.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_very_strong_up_trend':{'file':result+'/ag_junxian_barstyle_very_strong_up_trend.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_very_strong_down_trend':{'file':result+'/ag_junxian_barstyle_very_strong_down_trend.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_pv_no_filter':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_max_daily_increase':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'op_rsn','kw':'_max_daily_increase',"term": "SHORT TERM", "price": "NA" },
        # 'df_max_daily_decrease':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'op_rsn','kw':'_max_daily_decrease',"term": "SHORT TERM", "price": "NA" },
        # 'df_decrease_gap':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'op_rsn','kw':'_decrease_gap',"term": "SHORT TERM", "price": "NA" },
        # 'df_increase_gap':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'op_rsn','kw':'_increase_gap',"term": "SHORT TERM", "price": "NA" },
        # 'df_low_price_year':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'op_rsn','kw':'_pvbreak_lv_year',"term": "SHORT TERM", "price": "NA" },
        # 'df_low_vol_year':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'op_rsn','kw':'_max_daily_increase',"term": "SHORT TERM", "price": "NA" },
        # 'df_max_daily_increase':{'file':result_today + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv",'column':'op_rsn','kw':'_max_daily_increase',"term": "SHORT TERM", "price": "NA" },
        # 'df_macd_m':{'file':result+'/macd_selection_M.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_macd_w':{'file':result+'/macd_selection_W.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_macd_d':{'file':result+'/macd_selection_D.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        #
        #
        # 'df_kdj_m':{'file':result+'/kdj_selection_M.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_kdj_w':{'file':result+'/kdj_selection_W.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_kdj_d':{'file':result+'/kdj_selection_D.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_hsgt':{'file':result+'/hsgt_top_10_selected.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_moneyflow_top_amt_perc':{'file':result_today+'/mf_today_top5_large_amount.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_support_resist_line_today':{'file':result+"/key_points_" + str(stock_global).lower() + "_today_selected.csv",'column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_disclosure_date_notify':{'file':'/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date_notify.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_fib_ag':{'file':result+'/ag_fib.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        # 'df_fib_index':{'file':result+'/xxxx.csv','column':'','kw':'',"term": "SHORT TERM", "price": "NA" },
        'df_wei_pan_la_sheng': {'file': result_wpls + '/wei_pan_la_sheng_kcb_20201230_1445.csv', 'column': '', 'kw': '',
                                "term": "SHORT TERM", "price": "NA"},
        'df_hs300_add_candidate': {'file': result + '/' + 'hs300_candidate_list.csv', 'column': 'predict',
                                   'kw': constant.TO_BE_ADDED, "term": "SHORT TERM", "price": "NA"},
        'df_hs300_remove_candidate': {'file': result + '/' + 'hs300_candidate_list.csv', 'column': 'predict',
                                      'kw': constant.TO_BE_REMOVED, "term": "SHORT TERM", "price": "NA"},
        'df_sz100_add_candidate': {'file': result + '/' + 'sz100_candidate_list.csv', 'column': 'predict',
                                   'kw': constant.TO_BE_ADDED, "term": "SHORT TERM", "price": "NA"},
        'df_sz100_remove_candidate': {'file': result + '/' + 'sz100_candidate_list.csv', 'column': 'predict',
                                      'kw': constant.TO_BE_REMOVED, "term": "SHORT TERM", "price": "NA"},
        'df_zz100_add_candidate': {'file': result + '/' + 'zz100_candidate_list.csv', 'column': 'predict',
                                   'kw': constant.TO_BE_ADDED, "term": "SHORT TERM", "price": "NA"},
        'df_zz100_remove_candidate': {'file': result + '/' + 'zz100_candidate_list.csv', 'column': 'predict',
                                      'kw': constant.TO_BE_REMOVED, "term": "SHORT TERM", "price": "NA"},
        'df_szcz_add_candidate': {'file': result + '/' + 'szcz_candidate_list.csv', 'column': 'predict',
                                  'kw': constant.TO_BE_ADDED, "term": "SHORT TERM", "price": "NA"},
        'df_szcz_remove_candidate': {'file': result + '/' + 'szcz_candidate_list.csv', 'column': 'predict',
                                     'kw': constant.TO_BE_REMOVED, "term": "SHORT TERM", "price": "NA"},

        # 'xxxx': {'file': result + '/xxxx.csv', 'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},

    },  # end of AG

    'US': {
        'xxxx': {'file': result + '/xxxx.csv', 'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
    },  # end of US
    'HK': {
        'xxxx': {'file': result + '/xxxx.csv', 'column': '', 'kw': '', "term": "SHORT TERM", "price": "NA"},
    },  # end of HK
}


def refine_df(input_df, has_db_record=False, force_pass=False, force_pass_open_reason='', insert_buy_record_to_db=False, debug=False):
    #the function re-organize ptn hit result from code view, reduce the result from 1000+ to 10~100.
    #has_db_record: db has the ptn perf record.
    #force_pass: when inserting df_ann, that insert db even db doesn't have the ptn perf records.
    #force_pass_open_reason: must be specified if force_pass =True

    ####if a code has hit _pvbreak_ pattern,  then the code will be insert to db and in return df as one record.<<<removed
    #input_df = input_df.drop('index', axis=1).drop_duplicates().reset_index().drop('index', axis=1)

    if debug:
        return (input_df)

    if input_df.__len__() == 0:
        return (input_df)

    if 'index' in input_df.columns:
        input_df = input_df.drop('index', axis=1)

    if 'Unnamed: 0' in input_df.columns:
        input_df = input_df.drop('Unnamed: 0', axis=1)

    if 'level_0' in input_df.columns:
        input_df = input_df.drop('level_0', axis=1)

    input_df = input_df.drop_duplicates().reset_index().drop('index', axis=1)
    #print(input_df)

    #d = input_df['code'].drop_duplicates()
    #d = d.reset_index().drop('index', axis=1)
    #d = input_df

    logging.info(__file__+" "+"function refine_df, input df records " + str(input_df.__len__()) + " total stocks in the list: " + str(input_df.__len__()))

    #df_rtn = pd.DataFrame(columns=input_df.columns).rename(columns={'op': 'hit_ptn_cnt'}).drop('op_rsn', axis=1)
    df_rtn = pd.DataFrame(columns=input_df.columns).rename(columns={'op': 'hit_ptn_cnt'})

    engine = create_engine('mysql://root:admin888.@_@@127.0.0.1/ryan_stock_db?charset=utf8')
    mysql_host = '127.0.0.1'
    cnx = mysql.connector.connect(host=mysql_host, user='root', password='admin888.@_@', database="ryan_stock_db")
    cursor = cnx.cursor()  # setup db connection before for loop.

    for i in input_df['code'].unique():
        code = i
        #code = i[1][0]
        # print(code)

        # if code == 'SZ300607':
        #    pass

        c = input_df.loc[input_df['code'] == code]

        if c.__len__() > 0:
            #if c.__len__() > 0 and has_db_record:
            #if c.__len__() > 1:
            #    pass
            #print("run into debug")

            ptn_list = ''

            ptn_list = '\',\''.join(c['op_rsn'].tolist())
            #print("ptn_list_0: " + ptn_list)

            if re.match("^\'.*", ptn_list):
                #print("replacing head \'")
                ptn_list = re.sub("^\'", '', ptn_list)

            if re.match(".*\'$", ptn_list):
                #print("replacing tail \'")
                ptn_list = re.sub("\'$", '', ptn_list)

            ptn_list = '\'' + ptn_list + '\''

            #print("ptn_list: "+ptn_list)

            avg2 = avg5 = sum2uc = sum2dc = sum5uc = sum5dc = 0

            if has_db_record and insert_buy_record_to_db:  # AG only
                sql = "SELECT AVG(`2mea`) as avg2mea, AVG(`5mea`) as avg5mea," \
                      " sum(`2uc`) as sum2uc , sum(`2dc`) as sum2dc , sum(`5uc`) as sum5uc, sum(`5dc`) as sum5dc " \
                      " FROM `pattern_perf` WHERE `pattern` in ("+ptn_list+')'
                ptn_perf = pd.read_sql_query(sql, engine)

                avg2 = ptn_perf.iloc[0]['avg2mea']
                avg5 = ptn_perf.iloc[0]['avg5mea']
                sum2uc = ptn_perf.iloc[0]['sum2uc']
                sum2dc = ptn_perf.iloc[0]['sum2dc']
                sum5uc = ptn_perf.iloc[0]['sum5uc']
                sum5dc = ptn_perf.iloc[0]['sum5dc']

                if force_pass:
                    pass
                #elif ('_pvbreak_' in ptn_list ): ## REMOVED, HAS df_pv_break
                # pass #
                elif ('None' == str(avg2)) or ('None' == str(avg5)):
                    #if ('None' == str(avg2)) or ('None' == str(avg5)):
                    logging.info('None of the patterns in the DB')
                    continue
                elif (avg2 > 0.02) and (avg5 > 0.02) and (sum2uc > sum2dc * 3) and (sum5uc > sum5dc * 3):
                    pass  #Don't stop, continue to running into codes below.
                else:
                    logging.info(__file__+" "+"not qualified to buy")
                    continue

        c_code = str(c['code'].values[0])
        c_name = str(c['name'].values[0].encode("utf-8"))
        c_close_p = c['close_p'].values[0]

        if force_pass:
            c_op_rsn = force_pass_open_reason
        else:
            c_op_rsn = 'refine_df_func. ' + ','.join(c['op_rsn'].values.tolist())

        c_date = str(c['date'].values[0])
        c_status = 'OPEN'

        if pd.isnull(c_close_p) or float(c_close_p) == 0.0:
            logging.info(__file__+" "+"not processing as c_close_p is 0. " + c_code + " " + c_name + " " + c_date)
            continue

        #
        if insert_buy_record_to_db:
            sql = 'SELECT * FROM `order_tracking_stock` WHERE `code` =\'' + c_code + '\' AND `status`=\'OPEN\''
            open_stocks = pd.read_sql_query(sql, engine)
            db_tbl = 'order_tracking_stock'

            if open_stocks.__len__() > 0:
                logging.info(__file__+" "+"Have OPEN order on code " + c_code + ", +1 its buy_cnt")
                exist_buy_reason = open_stocks['buy_reason'].values[0]
                exist_buy_cnt = open_stocks['buy_cnt'].values[0]

                update_sql = ("UPDATE `" + db_tbl + "`  " "SET buy_cnt = %(buy_cnt)s, buy_reason = %(buy_reason)s " " WHERE `id` =  %(id)s ")
                data_sql = {}
                data_sql['buy_cnt'] = int(exist_buy_cnt + 1)
                bs = exist_buy_reason + "; " + c_date + ":" + c_op_rsn
                data_sql['buy_reason'] = bs[0:3199]  #var char 3200, length limit
                data_sql['id'] = int(open_stocks['id'].values[0])

                cursor.execute(update_sql, data_sql)
                cnx.commit()

                #continue
            else:
                logging.info('buy candidate,' + c_code + ' ' + c_name + ' ' + c_close_p + ' ' + c_date)

                update_sql = ("INSERT INTO `" + db_tbl + "`  " "SET code = %(code)s, name = %(name)s, buy_date = %(buy_date)s, " " buy_price = %(buy_price)s,  buy_cnt = %(buy_cnt)s, buy_reason = %(buy_reason)s, h_ptn_cnt = %(h_ptn_cnt)s, status = %(status)s ")

                data_sql = {}
                data_sql['code'] = c_code
                data_sql['name'] = c_name
                data_sql['buy_date'] = c_date
                data_sql['buy_price'] = c_close_p
                data_sql['buy_cnt'] = 1
                data_sql['buy_reason'] = c_op_rsn
                data_sql['status'] = c_status
                data_sql['h_ptn_cnt'] = c['op_rsn'].__len__()
                #print("update_sql "+update_sql)
                #print("name "+c_name)

                cursor.execute(update_sql, data_sql)
                cnx.commit()

        try:
            op_strength_group_sum = pd.to_numeric(c['op_strength']).sum()
            #print('buy candidate,' + c['code'].values[0] + ' ' + c['name'].values[0] + ', hit_cnt ' + str(
            #    c.__len__()) + ', avg2 ' + str(avg2) + ", avg5 " + str(avg5)+', op_strength '+str(op_strength_group_sum))

        except:
            #print("exception on op_strength " + c[0:1]['code'].values[0])
            op_strength_group_sum = 0
            #for i in c['op_strength'].iteritems():#python2
            for i in c['op_strength'].items():  #python3
                strength = i[1]
                if strength.find(',') >= -1:
                    strength = strength.split(',')[0]

                op_strength_group_sum += float(strength)
            logging.info(__file__+" "+"re-calc op_strength_group_sum to " + str(op_strength_group_sum) + ", " + c_code + " " + c_name)

        if has_db_record:
            df_rtn = df_rtn.append({
                "code": c[0:1]['code'].values[0],
                "name": c[0:1]['name'].values[0],
                "date": c[0:1]['date'].values[0],
                "close_p": c[0:1]['close_p'].values[0],
                "op_strength": op_strength_group_sum,
                "hit_ptn_cnt": c.__len__(),
                "op_rsn": ptn_list,
                "2mea": pd.to_numeric(c['2mea']).mean(),
                "5mea": pd.to_numeric(c['5mea']).mean(),
                "10mea": pd.to_numeric(c['10mea']).mean(),
                "20mea": pd.to_numeric(c['20mea']).mean(),
                "60mea": pd.to_numeric(c['60mea']).mean(),
                "120mea": pd.to_numeric(c['120mea']).mean(),
                "2uc": int(pd.to_numeric(c['2uc']).sum()),
                "2dc": int(pd.to_numeric(c['2dc']).sum()),
                "5uc": int(pd.to_numeric(c['5uc']).sum()),
                "5dc": int(pd.to_numeric(c['5dc']).sum()),
                "10uc": int(pd.to_numeric(c['10uc']).sum()),
                "10dc": int(pd.to_numeric(c['10dc']).sum()),
                "20uc": int(pd.to_numeric(c['20uc']).sum()),
                "20dc": int(pd.to_numeric(c['20dc']).sum()),
            }, ignore_index=True)
        else:
            df_rtn = df_rtn.append({
                "code": c[0:1]['code'].values[0],
                "name": c[0:1]['name'].values[0],
                "date": c[0:1]['date'].values[0],
                "close_p": c[0:1]['close_p'].values[0],
                "op_strength": op_strength_group_sum,
                "hit_ptn_cnt": c.__len__(),
                "op_rsn": ptn_list,
                "2mea": avg2,
                "5mea": avg5,
            }, ignore_index=True)

    engine.dispose()  #close db connection after for loop.
    cursor.close()
    cnx.close()
    # print(df_rtn)
    if 'index' in df_rtn.columns:
        df_rtn = df_rtn.drop('index', axis=1)

    if 'level_0' in df_rtn.columns:
        df_rtn = df_rtn.drop('level_0', axis=1)

    if 'Unnamed: 0' in df_rtn.columns:
        df_rtn = df_rtn.drop('Unnamed: 0', axis=1)

    df_rtn = df_rtn.sort_values(by=['hit_ptn_cnt', 'op_strength'], ascending=[False, False]).reset_index().drop('index', axis=1)
    return (df_rtn)


def my_sort(df, debug=False):

    if debug:
        return (df)

    if df.__len__() == 0:
        return df
    else:
        #logging.info(df.head(1))
        pass

    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1, inplace=False)

    finlib.Finlib().pprint(df)
    df = df.drop_duplicates().reset_index()

    if 'index' in df.columns:
        df = df.drop('index', axis=1)

    by_str = ''
    ascending_str = ''

    for k in ('hit_ptn_cnt', 'op_strength', '2mea'):
        if df.columns.tolist().__contains__(k):
            by_str += '\'' + k + '\','
            ascending_str += "False,"
    cmd = "df.sort_values(by=[" + by_str + "], ascending=[" + ascending_str + "], inplace=False)"

    if by_str.__len__() > 0 and df.__len__() > 0:
        df = eval(cmd)

    if 'code_x' in df.columns:
        df = df.rename(columns={"code_x": "code"}, inplace=False)

    if 'name_x' in df.columns:
        df = df.rename(columns={"name_x": "name"}, inplace=False)

    if 'close_p_x' in df.columns:
        df = df.rename(columns={"close_p_x": "close_p"}, inplace=False)

    if 'date_x' in df.columns:
        df = df.rename(columns={"date_x": "date"}, inplace=False)

    if 'index' in df.columns:
        df = df.drop('index', axis=1, inplace=False)

    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1, inplace=False)

    if 'name_y' in df.columns:
        df = df.drop('name_y', axis=1, inplace=False)

    if 'result_value_quarter_fundation_y' in df.columns:
        df = df.drop('result_value_quarter_fundation_y', axis=1, inplace=False)

    if 'esp_y' in df.columns:
        df = df.drop('esp_y', axis=1, inplace=False)

    if 'pe_y' in df.columns:
        df = df.drop('pe_y', axis=1, inplace=False)

    if 'profit_y' in df.columns:
        df = df.drop('profit_y', axis=1, inplace=False)

    #if 'hit_ptn_cnt_y' in df.columns: #KEEP THIS
    # df_rtn = df.drop('hit_ptn_cnt_y', axis=1)

    if '2mea_y' in df.columns:
        df = df.drop('2mea_y', axis=1, inplace=False)

    if '5mea_y' in df.columns:
        df = df.drop('5mea_y', axis=1, inplace=False)

    if 'close_p_y' in df.columns:
        df = df.drop('close_p_y', axis=1, inplace=False)

    if 'date_y' in df.columns:
        df = df.drop('date_y', axis=1, inplace=False)

    return df


def remove_unwant_columns(df):
    if 'index' in df.columns:
        df = df.drop('index', axis=1)
    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1)
    if 'Unnamed: 0' in df.columns:
        df = df.drop('Unnamed: 0', axis=1)
    return (df)


def combin_filter(df, post_combine=False, debug=False):

    if debug:
        return (df)

    df = remove_unwant_columns(df)

    cols = []
    keep_df_cols = [
        'code',
        'name',
        'date',
        'op_rsn',
        'op_strength',
        'close_p',
        'result_value_sum',
        'scoreA',
        'scoreAB',
        'score_over_years',
        'score_avg',
        'number_in_top_30',
        'result_value_today',
        'result_value_quarter_fundation',
        'score_sum',
        'result_value_2',
        'report_date',
        'roe',
        'year_quarter',
        'ps',
        'pe',
        'peg_1',
        'peg_4',
        'pe',
        'pb',
        'current_date',
        'current_close',
        'up_cnt',
        'down_cnt',
        'perc_to_up',
        'perc_to_down',
        'up_p',
        'down_p',
        'day_to_up',
        'day_to_down',
        'long_enter',
        'long_quit',
        'long_expect_ear_perct',
        'delta_to_long_enter',
        'pre_date',
        'ann_date',
        'actual_date',
        'long_take_profit_percent',
        'long_stop_lost_percent',
        'hit_sum',
        'h_cnt',
        'l_cnt',
        'o_cnt',
        'c_cnt',
        'p_max',
        'p_min',
    ]

    for col in df.columns:
        if post_combine:
            if not re.match('.*_y[0-9]+', col):  #remove duplicate name *_y0 from combined df
                cols.append(col)
        else:
            if col in keep_df_cols:
                cols.append(col)

    if 'date' in cols:
        cols.insert(0, cols.pop(cols.index('date')))

    if 'name' in cols:
        cols.insert(0, cols.pop(cols.index('name')))

    if 'code' in cols:
        cols.insert(0, cols.pop(cols.index('code')))

    df = df[cols]
    return df


def generate_result_csv(full_combination=False, select=True, debug=False):
    today_d = datetime.today().strftime('%d')

    exam_date = finlib.Finlib().get_last_trading_day()

    if select:
        rpt = "/home/ryan/DATA/result/report_" + exam_date + "_selected.txt"
    else:
        rpt = "/home/ryan/DATA/result/report_" + exam_date + "_" + stock_global + ".txt"
    rst = "\nGetting today's summary report " + exam_date + "\n\n"  #the string contains the report content.
    fh = open(rpt, "w")
    fh.write(rst)
    fh.close()

    logging.info(rst)


    ttdf_hs300_candidate_dict = finlib.Finlib().get_index_candidate('hs300')
    ttdf__candidate_dict = finlib.Finlib().get_index_candidate('hs300')

    # mkt_dict = not_selected_dict['AG']
    mkt_dict = not_selected_dict[stock_global]

    for k in mkt_dict.keys():
        df = pd.read_csv(mkt_dict[k]['file'])

        _column = mkt_dict[k]['column']
        _kw = mkt_dict[k]['kw']
        if not _column == '':
            df = df[df[_column]==_kw]
            df = df.reset_index().drop('index', axis=1)
            # logging.debug("applied filter on df column "+_column+" kw "+_kw)

        exec(k+" = df")
        logging.info("loaded "+k+" len "+str(df.__len__()))
        print(finlib.Finlib().pprint(df.head(1)))

        arr.append(k) #k is string



    ####################
    todayS_ymd = datetime.today().strftime('%Y%m%d')
    day_3_before_date = datetime.strptime(todayS_ymd, '%Y%m%d') - timedelta(5)
    day_3_before_date_ymd = day_3_before_date.strftime('%Y%m%d')

    #skip_sets = ('df_sz50', 'df_zz500', 'df_sme', 'df_hs300', 'df_gem')
    skip_sets = ('df_zz500', 'df_sme', 'df_hs300', 'df_gem', 'df_high_price_year')  #remove sz50 as 'df_sz50 df_sme' would make sense.
    buy_sets = ('df_fund', 'df_pv_break', 'df_low_price_year', 'df_low_vol_year')

    #ryan add 2018.08.26, reduce columns in df
    for d in arr:
        logging.info(d)
        tmp_df = combin_filter(eval(d), debug=debug)
        s = d + " = tmp_df"
        exec(s)



    ################################################
    # Df properties defination.
    ################################################

    dict_df = {}

    logging.info(__file__+" "+"------ Combination 1 -------")
    for a in arr:
        logging.info(__file__+" "+"=== " + a + " ====")
        if (a in ('df_zz500', 'df_sme', 'df_hs300', 'df_gem', 'sz50')):
            logging.info(__file__+" "+"skip list set: " + a)
            continue

        #cmd = a+".__len__()"
        #len= eval(cmd)
        #len=str(len)
        tmp = eval(a)
        tmp = combin_filter(tmp, post_combine=True, debug=debug)

        if tmp.__len__() == 0:
            logging.info(__file__+" "+"empty " + a)
            continue

        if ('date' in tmp.columns):
            tmp = tmp[tmp['date'] >= day_3_before_date_ymd]

        logging.info(__file__+" "+"sorting " + a)
        tmp = my_sort(tmp, debug=debug)
        len = str(tmp.__len__())

        #tmp_df=tmp_df.head(10)  #list all, as the final result should be keep for a long time. archive.

        long_term = 0
        middle_term = 0
        short_term = 0

        cheap_cnt = 0
        expensive_cnt = 0

        if mkt_dict[a]['term'] == "LONG TERM": # 'a' is a df name
            long_term += 1
        elif mkt_dict[a]['term'] == "MIDDLE TERM":
            middle_term += 1
        elif mkt_dict[a]['term'] == "SHORT TERM":
            short_term += 1

        if mkt_dict[a]['price'] == "CHEAP":
            cheap_cnt += 1
        elif mkt_dict[a]['price'] == "EXPENSIVE":
            expensive_cnt += 1

        dict_df[a] = tmp
        if tmp.__len__() > 0:
            rst = "\n==== " + str(cheap_cnt) + " cheap " + str(expensive_cnt) + " exp, " + str(short_term) + "s " + str(middle_term) + "m " + str(long_term) + "l " + "len " + len + ". " + a
            #rst += "\n"+str(tmp) + "\n"

            rst += "\n" + tabulate(tmp, headers='keys', tablefmt='psql') + "\n"

            fh = open(rpt, "a")
            fh.write(rst)
            fh.close()
            logging.info(rst)

    for combi in range(2, arr.__len__() + 1):
        #def _proc_combination(arr, combi, skip_sets, df_dict, day_3_before_date, debug, rpt):
        logging.info(__file__+" "+"------ Combination " + str(combi) + " -------")
        for subset in itertools.combinations(arr, combi):
            if ('df_low_price_year' in subset) and ('df_high_price_year' in subset):
                continue

            if ('df_low_vol_year' in subset) and ('df_high_vol_year' in subset):
                continue

            # every(all) subset are in skipset
            if subset.__len__() == set(subset).intersection(set(skip_sets)).__len__():
                logging.info(__file__+" "+"skip list set combination: " + " ".join(set(subset)))
                continue

            long_term = 0
            middle_term = 0
            short_term = 0

            cheap_cnt = 0
            expensive_cnt = 0

            for sub_item in subset:
                if mkt_dict[sub_item]['term'] == "LONG TERM":
                    long_term += 1
                elif mkt_dict[sub_item]['term'] == "MIDDLE TERM":
                    middle_term += 1
                elif mkt_dict[sub_item]['term'] == "SHORT TERM":
                    short_term += 1

                if mkt_dict[sub_item]['price'] == "CHEAP":
                    cheap_cnt += 1
                elif mkt_dict[sub_item]['price'] == "EXPENSIVE":
                    expensive_cnt += 1

            comb_df_name = subset[0]
            for _ in range(1, subset.__len__() - 1):
                comb_df_name += "_" + subset[_]
            # print(comb_df_name)

            if comb_df_name in dict_df.keys():
                tmp = dict_df[comb_df_name]
                #logging.info(("reuse saved combined df " + comb_df_name))
            else:
                #logging.info(__file__+" "+"combination no instance, " + comb_df_name)
                continue

            dup_suffix = "_y" + str(subset.__len__() - 2)
            tmp = finlib.Finlib().add_market_to_code(tmp) #debug
            tmp = eval("pd.merge(tmp," + subset[subset.__len__() - 1] + ", on='code',how='inner',suffixes=('','" + dup_suffix + "'))")
            tmp = combin_filter(tmp, post_combine=True, debug=debug)

            if 'date' in tmp.columns:
                tmp = tmp[tmp['date'] >= day_3_before_date_ymd]
            tmp = my_sort(tmp, debug=debug)

            comb_df_name = comb_df_name + "_" + subset[subset.__len__() - 1]

            if tmp.__len__() > 0:
                dict_df[comb_df_name] = tmp
                logging.info(("saved combined df " + comb_df_name))
                rst = "\n==== " + str(cheap_cnt) + " cheap " \
                      + str(expensive_cnt) + " exp, " \
                      + str(short_term) + "s " \
                      + str(middle_term) + "m " \
                      + str(long_term) + "l " \
                      + "len " + str(tmp.__len__()) + ". " + ", ".join(set(subset))

                rst += "\n" + tabulate(tmp, headers='keys', tablefmt='psql') + "\n"
                fh = open(rpt, "a")
                fh.write(rst)
                fh.close()
                logging.info(rst)

    logging.info(("result saved to " + rpt))
    logging.info(__file__+" "+"script completed")

    os._exit(0)


def ana_result():
    exam_date = finlib.Finlib().get_last_trading_day()
    input_f = "/home/ryan/DATA/result/report_" + exam_date + "_" + stock_global + ".txt"

    output_f = "/home/ryan/DATA/result/report_" + exam_date + "_" + stock_global + "_short.csv"
    df_rpt_short = pd.DataFrame(columns=['code', 'name', 'cnt'])
    dict = {}

    if not os.path.exists(input_f):
        logging.info(__file__+" "+"file not exist, " + input_f)
        return

    fh = open(input_f, "r")
    logging.info(__file__+" "+"loading " + input_f)

    skip_section = False

    for line in fh:
        #print(line)

        if re.match('.* code .*', line):
            continue

        if re.match("====", line):
            if re.match("====.* 0 exp", line):
                skip_section = False
            else:
                skip_section = True  #skip count expensive results

        if skip_section:
            continue

        lst = line.split('|')

        if lst.__len__() >= 4:
            code = lst[2]
            name = lst[3]

            #if code in dict.keys(): #python2
            if code in list(dict.keys()):  #python3
                dict[code]['cnt'] += 1
            else:
                dict[code] = {'cnt': 1, 'name': name}

        pass
    fh.close()
    df_rpt_short = pd.DataFrame.from_dict(dict).T.reset_index().rename(columns={"index": "code"})
    df_rpt_short = df_rpt_short.sort_values('cnt', ascending=False, inplace=False)
    df_rpt_short.to_csv(output_f, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "saved " + output_f + " . len " + str(df_rpt_short.__len__()))


def main():
    global stock_global

    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)
    # logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(AG)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

    parser.add_option("-f", "--full_combination", action="store_true", default=False, dest="full_combination", help="combine all the df from 2 to 12, Total df 24. Using with -x AG,  Report size could be 6GB ")

    parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="skip refine_df sub, ")
    parser.add_option("--select", action="store_true", default=False, dest="select", help="Analyze selected stocks only")

    parser.add_option("--action", dest="action", help="[generate_report|analyze_report] ")

    (options, args) = parser.parse_args()
    stock_global = options.stock_global
    full_combination = options.full_combination
    debug = options.debug
    action = options.action
    select = options.select

    if select:
        logging.info(__file__+" "+"select flag is set, stock_global flag will be ignored.")

    if (not select) and (stock_global is None):
        logging.info(__file__+" "+"-x --stock_global is None, check help for available options, program exit")
        exit(0)

    if (full_combination) and (stock_global != 'AG'):
        logging.info(__file__+" "+"--full_combination using with -x AG only")
        exit(0)

    if action == 'generate_report':
        generate_result_csv(full_combination=full_combination, select=select, debug=debug)
    elif action == 'analyze_report':
        ana_result()


### MAIN ####
if __name__ == '__main__':
    main()