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
    global csv_basic

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
    global csv_fina_mainbz_latest_percent
    global csv_disclosure_date_latest
    global csv_disclosure_date_latest_notify

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

    global df_all_ts_pro
    global df_all_jaqs
    global big_memory_global

    ### Global Variables ####

    myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'
    fund_base = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"


    fund_base_source = fund_base + "/source"
    fund_base_merged = fund_base + "/merged"
    fund_base_report = fund_base + "/report"
    fund_base_tmp = fund_base + "/tmp"
    fund_base_tmp = fund_base + "/tmp"

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

    csv_income = fund_base_source + "/income.csv"
    csv_balancesheet = fund_base_source + "/balancesheet.csv"
    csv_cashflow = fund_base_source + "/cashflow.csv"
    csv_forecast = fund_base_source + "/forecast.csv"
    csv_dividend = fund_base_source + "/dividend.csv"
    csv_express = fund_base_source + "/express.csv"
    csv_fina_indicator = fund_base_source + "/fina_indicator.csv"
    csv_fina_audit = fund_base_source + "/fina_audit.csv"
    csv_fina_mainbz = fund_base_source + "/fina_mainbz.csv"
    csv_fina_mainbz_sum = fund_base_source + "/fina_mainbz_sum.csv"
    csv_disclosure_date = fund_base_source + "/disclosure_date.csv"
    csv_basic = fund_base_source + "/basic.csv"

    fund_base_latest = fund_base_source + "/latest"

    csv_income_latest = fund_base_latest + "/income.csv"
    csv_balancesheet_latest = fund_base_latest + "/balancesheet.csv"
    csv_cashflow_latest = fund_base_latest + "/cashflow.csv"
    csv_forecast_latest = fund_base_latest + "/forecast.csv"
    csv_express_latest = fund_base_latest + "/express.csv"
    csv_dividend_latest = fund_base_latest + "/dividend.csv"
    csv_fina_indicator_latest = fund_base_latest + "/fina_indicator.csv"
    csv_fina_audit_latest = fund_base_latest + "/fina_audit.csv"
    csv_fina_mainbz_latest = fund_base_latest + "/fina_mainbz.csv"
    csv_fina_mainbz_sum_latest = fund_base_latest + "/fina_mainbz_sum.csv"
    csv_fina_mainbz_latest_percent = fund_base_latest + "/fina_mainbz_percent.csv"
    csv_disclosure_date_latest = fund_base_latest + "/disclosure_date.csv"
    csv_disclosure_date_latest_notify = fund_base_latest + "/disclosure_date_notify.csv"

    col_list_income = ['ts_code', 'name', 'end_date', 'basic_eps', 'total_revenue', 'revenue', 'oth_b_income', 'n_income_attr_p', 'distable_profit']
    col_list_balancesheet = ['ts_code', 'name', 'end_date', 'total_assets', 'total_liab', 'money_cap', 'undistr_porfit', 'invest_real_estate', 'fa_avail_for_sale', 'lt_borr', 'st_borr', 'cb_borr']
    col_list_cashflow = ['ts_code', 'name', 'end_date', 'net_profit']
    col_list_forecast = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_dividend = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_express = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_disclosure_date = ['ts_code', 'name', 'ann_date', 'end_date', 'pre_date', 'actual_date', 'modify_date']

    # rd_exp is not in the actual output
    col_list_fina_indicator = [
        'ts_code',
        'name',
        'end_date',
        'eps',
        'roe',
        'debt_to_assets',
        'rd_exp',
        'total_revenue_ps',
        'netprofit_margin',
    ]
    col_list_fina_audit = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_fina_mainbz = [
        'ts_code',
        'name',
        'end_date',
    ]

    ####start
    query_fields_income = 'ts_code,update_flag,end_date,basic_eps,total_revenue,revenue,oth_b_income,n_income_attr_p,distable_profit,admin_exp,ann_date,ass_invest_income,assets_impair_loss,biz_tax_surchg,comm_exp,comm_income,comp_type,compens_payout,compens_payout_refu,compr_inc_attr_m_s,compr_inc_attr_p,diluted_eps,div_payt,ebit,ebitda,f_ann_date,fin_exp,forex_gain,fv_value_chg_gain,income_tax,insur_reser_refu,insurance_exp,int_exp,int_income,invest_income,minority_gain,n_asset_mg_income,n_commis_income,n_income,n_oth_b_income,n_oth_income,n_sec_tb_income,n_sec_uw_income,nca_disploss,non_oper_exp,non_oper_income,oper_cost,oper_exp,operate_profit,oth_compr_income,other_bus_cost,out_prem,prem_earned,prem_income,prem_refund,reins_cost_refund,reins_exp,reins_income,report_type,reser_insur_liab,sell_exp,t_compr_income,total_cogs,total_profit,undist_profit,une_prem_reser'

    query_fields_balancesheet = 'ts_code,update_flag,end_date,total_assets,total_liab,money_cap,undistr_porfit,invest_real_estate,fa_avail_for_sale,lt_borr,st_borr,cb_borr,acc_exp,acc_receivable,accounts_receiv,acct_payable,acting_trading_sec,acting_uw_sec,adv_receipts,agency_bus_liab,amor_exp,ann_date,bond_payable,cap_rese,cash_reser_cb,cip,client_depos,client_prov,comm_payable,comp_type,const_materials,decr_in_disbur,defer_inc_non_cur_liab,defer_tax_assets,defer_tax_liab,deferred_inc,depos,depos_ib_deposits,depos_in_oth_bfi,depos_oth_bfi,depos_received,deriv_assets,deriv_liab,div_payable,div_receiv,estimated_liab,f_ann_date,fix_assets,fixed_assets_disp,forex_differ,goodwill,hfs_assets,hfs_sales,htm_invest,indem_payable,indep_acct_assets,indept_acc_liab,int_payable,int_receiv,intan_assets,inventories,invest_as_receiv,invest_loss_unconf,lending_funds,loan_oth_bank,loanto_oth_bank_fi,lt_amor_exp,lt_eqt_invest,lt_payable,lt_payroll_payable,lt_rec,minority_int,nca_within_1y,non_cur_liab_due_1y,notes_payable,notes_receiv,oil_and_gas_assets,ordin_risk_reser,oth_assets,oth_comp_income,oth_cur_assets,oth_cur_liab,oth_eqt_tools,oth_eqt_tools_p_shr,oth_liab,oth_nca,oth_ncl,oth_payable,oth_receiv,payable_to_reinsurer,payables,payroll_payable,ph_invest,ph_pledge_loans,pledge_borr,policy_div_payable,prec_metals,prem_receiv_adva,premium_receiv,prepayment,produc_bio_assets,pur_resale_fa,r_and_d,refund_cap_depos,refund_depos,reinsur_receiv,reinsur_res_receiv,report_type,reser_lins_liab,reser_lthins_liab,reser_outstd_claims,reser_une_prem,rr_reins_lins_liab,rr_reins_lthins_liab,rr_reins_outstd_cla,rr_reins_une_prem,rsrv_insur_cont,sett_rsrv,sold_for_repur_fa,special_rese,specific_payables,st_bonds_payable,st_fin_payable,surplus_rese,taxes_payable,time_deposits,total_cur_assets,total_cur_liab,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,total_nca,total_ncl,total_share,trad_asset,trading_fl,transac_seat_fee,treasury_share'

    query_fields_cashflow = 'ts_code,update_flag,end_date,net_profit,amort_intang_assets,ann_date,beg_bal_cash,beg_bal_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_disp_withdrwl_invest,c_fr_oth_operate_a,c_fr_sale_sg,c_inf_fr_operate_a,c_paid_for_taxes,c_paid_goods_s,c_paid_invest,c_paid_to_for_empl,c_pay_acq_const_fiolta,c_pay_claims_orig_inco,c_pay_dist_dpcp_int_exp,c_prepay_amt_borr,c_recp_borrow,c_recp_cap_contrib,c_recp_return_invest,comp_type,conv_copbonds_due_within_1y,conv_debt_into_cap,decr_def_inc_tax_assets,decr_deferred_exp,decr_inventories,decr_oper_payable,depr_fa_coga_dpba,eff_fx_flu_cash,end_bal_cash,end_bal_cash_equ,f_ann_date,fa_fnc_leases,finan_exp,free_cashflow,ifc_cash_incr,im_n_incr_cash_equ,im_net_cashflow_oper_act,incl_cash_rec_saims,incl_dvd_profit_paid_sc_ms,incr_acc_exp,incr_def_inc_tax_liab,incr_oper_payable,invest_loss,loss_disp_fiolta,loss_fv_chg,loss_scr_fa,lt_amort_deferred_exp,n_cap_incr_repur,n_cash_flows_fnc_act,n_cashflow_act,n_cashflow_inv_act,n_depos_incr_fi,n_disp_subs_oth_biz,n_inc_borr_oth_fi,n_incr_cash_cash_equ,n_incr_clt_loan_adv,n_incr_dep_cbob,n_incr_disp_faas,n_incr_disp_tfa,n_incr_insured_dep,n_incr_loans_cb,n_incr_loans_oth_bank,n_incr_pledge_loan,n_recp_disp_fiolta,n_recp_disp_sobu,n_reinsur_prem,oth_cash_pay_oper_act,oth_cash_recp_ral_fnc_act,oth_cashpay_ral_fnc_act,oth_pay_ral_inv_act,oth_recp_ral_inv_act,others,pay_comm_insur_plcy,pay_handling_chrg,prem_fr_orig_contr,proc_issue_bonds,prov_depr_assets,recp_tax_rends,report_type,st_cash_out_act,stot_cash_in_fnc_act,stot_cashout_fnc_act,stot_inflows_inv_act,stot_out_inv_act,uncon_invest_loss'

    query_fields_fina_indicator = 'ts_code,update_flag,end_date,eps,roe,debt_to_assets,total_revenue_ps,netprofit_margin,adminexp_of_gr,ann_date,ar_turn,assets_to_eqt,assets_turn,assets_yoy,basic_eps_yoy,bps,bps_yoy,ca_to_assets,ca_turn,capital_rese_ps,cash_ratio,cfps,cfps_yoy,cogs_of_sales,current_exint,current_ratio,currentdebt_to_debt,debt_to_eqt,diluted2_eps,dp_assets_to_eqt,dt_eps,dt_eps_yoy,dt_netprofit_yoy,ebit,ebit_of_gr,ebit_ps,ebitda,ebt_yoy,eqt_to_debt,eqt_to_interestdebt,eqt_to_talcapital,eqt_yoy,equity_yoy,expense_of_sales,extra_item,fa_turn,fcfe,fcfe_ps,fcff,fcff_ps,finaexp_of_gr,fixed_assets,gc_of_gr,gross_margin,grossprofit_margin,impai_ttm,int_to_talcap,interestdebt,invest_capital,longdeb_to_debt,nca_to_assets,netdebt,netprofit_yoy,networking_capital,noncurrent_exint,npta,ocf_to_debt,ocf_to_shortdebt,ocf_yoy,ocfps,op_income,op_of_gr,op_yoy,or_yoy,profit_dedt,profit_to_gr,profit_to_op,q_dt_roe,q_gc_to_gr,q_npta,q_ocf_to_sales,q_op_qoq,q_roe,q_saleexp_to_gr,q_sales_yoy,quick_ratio,retained_earnings,retainedps,revenue_ps,roa,roa2_yearly,roa_dp,roa_yearly,roe_dt,roe_waa,roe_yearly,roe_yoy,roic,saleexp_to_gr,surplus_rese_ps,tangasset_to_intdebt,tangible_asset,tangibleasset_to_debt,tangibleasset_to_netdebt,tbassets_to_totalassets,tr_yoy,turn_days,undist_profit_ps,working_capital, rd_exp'

    query_fields_forecast = 'ts_code,end_date,ann_date,change_reason,first_ann_date,last_parent_net,net_profit_max,net_profit_min,p_change_max,p_change_min,summary,type'

    query_fields_dividend = 'ts_code,end_date,ann_date,cash_div,cash_div_tax,div_listdate,div_proc,ex_date,imp_ann_date,pay_date,record_date,stk_bo_rate,stk_co_rate,stk_div'

    query_fields_express = 'ts_code,end_date,ann_date,bps,diluted_eps,diluted_roe,eps_last_year,growth_assets,growth_bps,is_audit,n_income,np_last_year,op_last_year,open_bps,open_net_assets,operate_profit,or_last_year,perf_summary,remark,revenue,total_assets,total_hldr_eqy_exc_min_int,total_profit,tp_last_year,yoy_dedu_np,yoy_eps,yoy_equity,yoy_net_profit,yoy_op,yoy_roe,yoy_sales,yoy_tp'

    query_fields_fina_audit = 'ts_code,end_date,ann_date,audit_agency,audit_result,audit_sign'

    query_fields_fina_mainbz = 'ts_code,update_flag,end_date,bz_cost,bz_item,bz_profit,bz_sales,curr_type'

    query_fields_disclosure_date = 'ts_code,ann_date,end_date,pre_date,actual_date,modify_date'
    ###end

def get_beneish_element(ts_code, ann_date, df_all_ts_pro):
    dict = {}
    
    #The following data is needed.
    # Net Sales  净销售额  | c_fr_sale_sg, 销售商品、提供劳务收到的现金
    c_fr_sale_sg =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='c_fr_sale_sg', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['c_fr_sale_sg']=c_fr_sale_sg

    # Cost of Goods  货品的成本 |  total_cogs, 营业总成本 |  cogs_of_sales, 销售成本率
    total_cogs =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='total_cogs', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['total_cogs']=total_cogs

    # Net Receivables  净应收账款 | accounts_receiv, 应收账款| acc_receivable, 应收款项, payables, 	应付款项
    accounts_receiv =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='accounts_receiv', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    acc_receivable =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='acc_receivable', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    payables =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='payables', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['accounts_receiv']=accounts_receiv
    
    # Current Assets  流动资产|total_cur_assets, 流动资产合计
    total_cur_assets =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='total_cur_assets', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['total_cur_assets']=total_cur_assets
    
    # Property, Plant and Equipment 物业，厂房及设备 | fix_assets, 固定资产
    fix_assets =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='fix_assets', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['fix_assets']=fix_assets
    
    # Depreciation  折旧|daa,折旧与摊销, depr_fa_coga_dpba, 固定资产折旧、油气资产折耗、生产性生物资产折旧
    depr_fa_coga_dpba =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='depr_fa_coga_dpba', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['depr_fa_coga_dpba']=depr_fa_coga_dpba
    
    # Total Assets 总资产|total_assets, 资产总计
    total_assets =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='total_assets', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['total_assets']=total_assets
    
    # Selling, General and Administrative Expenses, 销售，一般和行政费用| sell_exp 减:销售费用  admin_exp	减:管理费用, fin_exp 减:财务费用, rd_exp 研发费用?
    sell_exp =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='sell_exp', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    admin_exp =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='admin_exp', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    fin_exp =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='fin_exp', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    rd_exp =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='rd_exp', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)

    if rd_exp is None:
        rd_exp = 0

    sga_exp = sell_exp + admin_exp + fin_exp + rd_exp
    dict['sga_exp']=sga_exp

    # Net Income 净收入 | n_income 净利润(含少数股东损益)
    n_income =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='n_income', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['n_income']=n_income   
    
    # Cash Flow from Operations 运营现金流| n_cashflow_act, 经营活动产生的现金流量净额
    n_cashflow_act =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='n_cashflow_act', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['n_cashflow_act']=n_cashflow_act
    
    # Current Liabilities 流动负债 | total_cur_liab	 流动负债合计
    total_cur_liab =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='total_cur_liab', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['total_cur_liab']=total_cur_liab
    
    # Long-Term Debt  长期债务 | total_ncl, 非流动负债合计
    total_ncl =  finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date, field='total_ncl', big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro,fund_base_merged=fund_base_merged)
    dict['total_ncl']=total_ncl
    
    return(dict)


def main():
    ########################
    #
    #########################

    logging.info(__file__ + " " + "\n")
    logging.info(__file__ + " " + "SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-f", "--fetch_data_all", action="store_true", dest="fetch_all_f", default=False, help="fetch all the quarterly fundatation history data")

    parser.add_option("--fetch_pro_basic", action="store_true", dest="fetch_pro_basic_f", default=False, help="")

    parser.add_option("--fetch_stk_holdertrade", action="store_true", dest="fetch_stk_holdertrade_f", default=False, help="")
    parser.add_option("--fetch", action="store_true", dest="fetch_f", default=False, help="fetch pro income,balance,cashflow,mainbz,dividend,indicator,audit,forecast,express,disclosure ")
    parser.add_option("--fetch_basic_quarterly", action="store_true", dest="fetch_basic_quarterly_f", default=False, help="")
    parser.add_option("--fetch_basic_daily", action="store_true", dest="fetch_basic_daily_f", default=False, help="")
    parser.add_option("--fetch_pro_concept", action="store_true", dest="fetch_pro_concept_f", default=False, help="")
    parser.add_option("--fetch_pro_repurchase", action="store_true", dest="fetch_pro_repurchase_f", default=False, help="")
    parser.add_option("--fetch_cctv_news", action="store_true", dest="fetch_cctv_news_f", default=False, help="")

    parser.add_option("-e", "--extract_latest", action="store_true", dest="extract_latest_f", default=False, help="extract latest quarter data")

    parser.add_option("-m", "--merge_quarterly", action="store_true", dest="merge_quarterly_f", default=False, help="merge date by quarter")

    parser.add_option("-a", "--analyze", action="store_true", dest="analyze_f", default=False, help="analyze based on the merged quarterly csv")

    parser.add_option("--concept_top", action="store_true", dest="concept_top_f", default=False, help="analyze top 3 stocks in each concept")

    parser.add_option("--overwrite", action="store_true", dest="overwrite_f", default=False, help="overwrite existing analyse output csv, using with -a")

    parser.add_option("--fully_a", action="store_true", dest="fully_a_f", default=False, help="run all the analyze steps, using with -a")

    parser.add_option("-d", "--daily_a", action="store_true", dest="daily_a_f", default=False, help="only run analyze_step_6, using with -a")

    parser.add_option("-s", "--sum_mainbz", action="store_true", dest="sum_mainbz_f", default=False, help="sum_mainbz, output fina_mainbz_sum.csv")

    parser.add_option("--percent_mainbz_f", action="store_true", dest="percent_mainbz_f", default=False, help="calc each item percent in the mainbz, output source/latest/fina_mainbz_percent.csv")

    parser.add_option("-c", "--fast_fetch", action="store_true", dest="fast_fetch_f", default=False, help="only fetch stocks whose high score >70.")

    parser.add_option("--wh_hencow_fcf", action="store_true", dest="white_horse_hencow_fcf_f", default=False, help="extract white horse, hen, cow, freecashflow.")

    parser.add_option("--merge_individual", action="store_true", dest="merge_individual_f", default=False, help="consolidate indiviaul from each period.")

    parser.add_option("--merge_local", action="store_true", dest="merge_local_f", default=False, help="consolidate individual csv to source/[*].csv")
    parser.add_option("--merge_local_basic", action="store_true", dest="merge_local_basic_f", default=False, help="consolidate daily basic to source/basic.csv")

    parser.add_option("--big_memory", action="store_true", dest="big_memory_f", default=False, help="consumes 4G memory to load all the jaqs and tushare data to two df")

    parser.add_option("-u", "--debug", action="store_true", dest="debug_f", default=False, help="debug mode, using merge.dev, report.dev folder")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("--express_notify", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("--disclosure_date_notify_day", type="int", dest="disclosure_date_notify_day_f", default=None, help="generate stock list that will be disclosured in the give days.")

    #parser.add_option("-v", "--verify_fund_increase", action="store_true",
    #                  dest="verify_fund_increase_f", default=False,
    #                  help="verify quartly score and buy and increase to today")

    (options, args) = parser.parse_args()
    fetch_all_f = options.fetch_all_f
    extract_latest_f = options.extract_latest_f
    merge_quarterly_f = options.merge_quarterly_f
    analyze_f = options.analyze_f
    concept_top_f = options.concept_top_f
    overwrite_f = options.overwrite_f
    fully_a_f = options.fully_a_f
    daily_a_f = options.daily_a_f
    sum_mainbz_f = options.sum_mainbz_f
    percent_mainbz_f = options.percent_mainbz_f
    fast_fetch_f = options.fast_fetch_f
    white_horse_hencow_fcf_f = options.white_horse_hencow_fcf_f
    merge_individual_f = options.merge_individual_f
    merge_local_f = options.merge_local_f
    merge_local_basic_f = options.merge_local_basic_f
    big_memory_f = options.big_memory_f
    debug_f = options.debug_f
    force_run_f = options.force_run_f
    disclosure_date_notify_day_f = options.disclosure_date_notify_day_f

    #verify_fund_increase_f = options.verify_fund_increase_f

    logging.info(__file__ + " " + "fetch_all_f: " + str(fetch_all_f))
    logging.info(__file__ + " " + "extract_latest_f: " + str(extract_latest_f))
    logging.info(__file__ + " " + "merge_quarterly_f: " + str(merge_quarterly_f))
    logging.info(__file__ + " " + "analyze_f: " + str(analyze_f))
    logging.info(__file__ + " " + "concept_top_f: " + str(concept_top_f))
    logging.info(__file__ + " " + "overwrite_f: " + str(overwrite_f))
    logging.info(__file__ + " " + "fully_a_f: " + str(fully_a_f))
    logging.info(__file__ + " " + "daily_a_f: " + str(daily_a_f))
    logging.info(__file__ + " " + "sum_mainbz_f: " + str(sum_mainbz_f))
    logging.info(__file__ + " " + "percent_mainbz_f: " + str(percent_mainbz_f))
    logging.info(__file__ + " " + "fast_fetch_f: " + str(fast_fetch_f))
    logging.info(__file__ + " " + "white_horse_hencow_fcf_f: " + str(white_horse_hencow_fcf_f))
    logging.info(__file__ + " " + "merge_individual_f: " + str(merge_individual_f))
    logging.info(__file__ + " " + "merge_local_f: " + str(merge_local_f))
    logging.info(__file__ + " " + "merge_local_basic_f: " + str(merge_local_basic_f))
    logging.info(__file__ + " " + "big_memory_f: " + str(big_memory_f))
    logging.info(__file__ + " " + "debug_f: " + str(debug_f))
    logging.info(__file__ + " " + "force_run_f: " + str(force_run_f))
    logging.info(__file__ + " " + "disclosure_date_notify_day_f: " + str(disclosure_date_notify_day_f))
    #logging.info(__file__+" "+"disclosure_date_notify_day_f: " + str(disclosure_date_notify_day_f))

    set_global(debug=debug_f, big_memory=big_memory_f, force_run=force_run_f)


    ts.set_token(myToken)
    pro = ts.pro_api()


    ts_code='600519.SH'
    ann_date = '20191231'
    field = 'accounts_receiv'
    big_memory = False
    df_all_ts_pro = None
    fund_base_merged = None

    df = pro.income(ts_code=ts_code, period=ann_date, fields='ts_code,ann_date,revenue')
    df = pro.fina_indicator(ts_code=ts_code, period=ann_date, fields='ts_code,ann_date,rd_exp')
    pass

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



    
    #M = -6.065 + 0.823 DSRI + 0.906 GMI + 0.593 AQI + 0.717 SGI + 0.107 DEPI
    
    #M = -4.84 + 0.92 DSRI + 0.528 GMI + 0.404 AQI + 0.892 SGI + 0.115 DEPI – 0.172 SGAI + 4.679 TATA – 0.327 LVGI
    
    
    
    #turn_days,营业周期
    dict = get_beneish_element(ts_code=ts_code, ann_date=ann_date,df_all_ts_pro=df_all_ts_pro)
    pass
    
    
    #DSRI Days Sales in Receivables Index, 应收账款周转指数
    # DSRI = (Net Receivablest / Salest) / Net Receivablest-1 / Salest-1)
       
    
    #GMI Gross Margin Index, 毛利率指数
    #GMI = [(Salest-1 - COGSt-1) / Salest-1] / [(Salest - COGSt) / Salest]    
    
    #AQI Asset Quality Index,  资产质量指数
    #AQI = [1 - (Current Assetst + PP&Et + Securitiest) / Total Assetst] / [1 - ((Current Assetst-1 + PP&Et-1 + Securitiest-1) / Total Assetst-1)]
    
    #SGI Sales Growth Index, 销售增长指数
    #SGI = Salest / Salest-1
    
    #DEPI Depreciation Index, 折旧指数
    #DEPI = (Depreciationt-1/ (PP&Et-1 + Depreciationt-1)) / (Depreciationt / (PP&Et + Depreciationt))
    
    #SGAI Sales, General and Administrative Expenses Index,销售费用指数
    #SGAI = (SG&A Expenset / Salest) / (SG&A Expenset-1 / Salest-1)
    
    
    #TATA Total Accruals to Total Assets, 应计总数与总资产比率
    #TATA = (Income from Continuing Operationst - Cash Flows from Operationst) / Total Assetst
    
    #LVGI  Leverage Index,
    #LVGI = [(Current Liabilitiest + Total Long Term Debtt) / Total Assetst] / [(Current Liabilitiest-1 + Total Long Term Debtt-1) / Total Assetst-1]
    
    

    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
