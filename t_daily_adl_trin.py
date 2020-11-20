import pandas as pd
import finlib

import finlib_indicator

import tushare as ts
import datetime

import talib
from optparse import OptionParser

import logging
import time
import os
import matplotlib.pyplot as plt

logging.getLogger('matplotlib.font_manager').disabled = True


def prepare_data(days=30):
    ######### For TRIN, Advance/Decline LINE #######\
    dir = "/home/ryan/DATA/DAY_Global/AG"
    csv = dir+"/ag_all.csv"
    csv = '/home/ryan/DATA/DAY_Global/AG/ag_all.csv'

    if not finlib.Finlib().is_cached(file_path=csv, day=1):

        print("generating csv from source.")

        cmd1 = "head -1 "+dir+"/SH600519.csv > "+csv
        cmd2 = "for i in `ls "+dir+"/SH*.csv`; do tail -"+str(days)+" $i >> "+dir+"/ag_all.csv; done"
        cmd3 = "for i in `ls "+dir+"/SZ*.csv`; do tail -"+str(days)+" $i >> "+dir+"/ag_all.csv; done"

        print(cmd1)
        print(cmd2) # for i in `ls SH*.csv`; do tail -300 $i >> ag_all.csv;done
        print(cmd3) # for i in `ls SZ*.csv`; do tail -300 $i >> ag_all.csv;done

        os.system(cmd1)
        os.system(cmd2)
        os.system(cmd3)
        print("generated "+csv)
    else:
        print("re-using csv as it generated in 1 days. "+csv)


    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv)
    return(df)

def market_adl_trin(df_in, df_out, days):
    ###----------- AG OverAll ------------
    a = df_in['date'].unique()
    a.sort()
    dates = a[-1*days:]

    ADL = 0
    previous_adv = 0
    previous_adv_perc = 0
    previous_vol_perc = 0
    previous_amt_perc = 0
    previous_net_amt = 0

    for d in dates:
        _df = df_in[df_in['date']==d]
        _df_adv = _df[_df['close'] > _df['open']]
        _df_dec = _df[_df['close'] < _df['open']]
        _adv_stocks = _df_adv.__len__()
        _dec_stocks = _df_dec.__len__()
        net_adv = _adv_stocks - _dec_stocks
        net_adv_perc = round((_adv_stocks - _dec_stocks)/_df.__len__(),4)

        net_vol_perc = (_df_adv['volume'].sum() - _df_dec['volume'].sum())/(_df_adv['volume'].sum() + _df_dec['volume'].sum())
        net_amt = _df_adv['amount'].sum() - _df_dec['amount'].sum()
        # print("Debug: net_amt "+str(net_amt))
        amt_sum = _df_adv['amount'].sum() + _df_dec['amount'].sum()
        if net_amt != 0 and amt_sum != 0:
            amt_perc = net_amt/amt_sum
        else:
            amt_perc = 0

        # print("Debug: amt_perc "+str(amt_perc))


        ADL = net_adv + previous_adv
        ADL_perc = round(net_adv_perc + previous_adv_perc,4)
        vol_perc = round(net_vol_perc + previous_vol_perc,4)
        net_amt = round(net_amt + previous_net_amt,4)
        print("Debug: net_amt_acc "+str(net_amt))

        amt_perc = round(amt_perc + previous_amt_perc,4)
        # print("Debug: amt_perc_acc "+str(amt_perc))

        logging.debug("date "+d+", code SH000001 (AG_overall)"
              +", net_adv "+str(net_adv)
              +", PA "+str(previous_adv)
              +", ADL "+str(ADL)
              +", ADL_perc "+str(ADL_perc)
              +", vol_perc "+str(vol_perc)
              +", amt_perc "+str(amt_perc)
              +", amt "+str(net_amt)
              )



        ### TRIN (Arms Index)
        adv_dec_cnt_ratio = round(_adv_stocks / _dec_stocks,2)
        adv_dec_vol_ratio = round(_df_adv['volume'].sum()/_df_dec['volume'].sum(),2)
        adv_dec_amt_ratio = round(_df_adv['amount'].sum()/_df_dec['amount'].sum(),2)

        TRIN = round(adv_dec_amt_ratio/adv_dec_vol_ratio,2)

        logging.debug("date "+d+", code SH000001, ad_cnt_ratio "+str(adv_dec_cnt_ratio)+", ad_vol_ratio "+str(adv_dec_vol_ratio)+", ad_amt_ratio "+str(adv_dec_amt_ratio) +
              ", TRIN "+str(TRIN))

        df_out = df_out.append({'date':d, 'code':'SH000001',
                               # 'net_adv_perc':net_adv_perc,
                                'ADL':ADL,
                                'ADL_perc':ADL_perc,
                                'vol_perc':vol_perc,
                                'amt_perc':amt_perc,
                                'net_amt':net_amt,
                                'TRIN':TRIN}, ignore_index=True)

        previous_adv = ADL
        previous_adv_perc = ADL_perc
        previous_vol_perc = vol_perc
        previous_amt_perc = amt_perc
        previous_net_amt = net_amt

    return(df_out)
    logging.info("Dataframe AG Market ADL, TRIN is generated")

def individual_adl_trin(df_in, df_out):
    ###----------- Individual ------------
    df = finlib.Finlib().remove_garbage(df_in, code_field_name='code', code_format='C2D6')

    codes = df['code'].unique()
    codes.sort()

    for c in codes:
        previous_adv = 0
        ADL = 0

        _df = df[df['code']==c]  #_df already has number of 'days' rows.

        if _df.__len__() < days-1:
            print("ignore code "+c+" has no enough bars. expected "+str(days)+", actual "+str(_df.__len__()))
            continue

        _last_date = _df[['date']].iloc[-1].values[0]
        _df_adv = _df[df['close'] > df['open']]
        _df_dec = _df[df['close'] < df['open']]
        _adv_days = _df_adv.__len__()
        _dec_days = _df_dec.__len__()
        net_adv = _adv_days - _dec_days
        net_adv_perc = round(net_adv/_df.__len__(),4)

        ADL = net_adv
        ADL_perc = round(net_adv/_df.__len__(), 4)
        logging.debug("date "+_last_date+", code "+c+", net_adv_perc "+str(net_adv_perc)+", PA "+str(previous_adv)+", ADL "+str(ADL)+", ADL_perc "+str(ADL_perc))


        ### TRIN (Arms Index)
        if _dec_days == 0: # no decling days, very strong, set TRIN=0
            adv_dec_cnt_ratio = 0
            adv_dec_vol_ratio = 0
            adv_dec_amt_ratio = 0
            TRIN = 0
        else:
            adv_dec_cnt_ratio = round(_adv_days / _dec_days,2)
            adv_dec_vol_ratio = round(_df_adv['volume'].sum()/_df_dec['volume'].sum(),2)
            adv_dec_amt_ratio = round(_df_adv['amount'].sum()/_df_dec['amount'].sum(),2)

            TRIN = round(adv_dec_amt_ratio/adv_dec_vol_ratio,2)

        logging.debug("date "+_last_date+", code "+c+", ad_cnt_ratio "+str(adv_dec_cnt_ratio)+", ad_vol_ratio "+str(adv_dec_vol_ratio)+", ad_amt_ratio "+str(adv_dec_amt_ratio) +
              ", TRIN "+str(TRIN))



        # df_out = df_out.append({'date':d, 'code':'SH000001',
        #                        # 'net_adv_perc':net_adv_perc,
        #                         'ADL':ADL,
        #                         'ADL_perc':ADL_perc,
        #                         'vol_perc':vol_perc,
        #                         'amt_perc':amt_perc,
        #                         'net_amt':net_amt,
        #                         'TRIN':TRIN}, ignore_index=True)


        df_out = df_out.append({'date':_last_date,
                                'code':c,
                                #'net_adv_perc':net_adv_perc,
                                'ADL':ADL,
                                'ADL_perc':ADL_perc,
                                'vol_perc': round((_df_adv['volume'].sum() - _df_dec['volume'].sum())/(_df_adv['volume'].sum() + _df_dec['volume'].sum()),4),
                                'amt_perc': round((_df_adv['amount'].sum() - _df_dec['amount'].sum())/(_df_adv['amount'].sum() + _df_dec['amount'].sum()),4),
                                'net_amt':_df_adv['amount'].sum() - _df_dec['amount'].sum(),
                                'TRIN':TRIN}, ignore_index=True)

    return(df_out)

def show_result(csv_out,root_dir,show_plot=False):

    df_index = finlib.Finlib().regular_read_csv_to_stdard_df(
        data_csv='/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv')
    df_index = df_index[['date', 'close']]




    df_out = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_out)
    logging.info("loading df from " + csv_out)
    df_ag = df_out[df_out['code'] == 'SH000001'].reset_index().drop('index', axis=1)
    df_ag = pd.merge(left=df_ag, right=df_index, how='inner', left_on='date', right_on='date', suffixes=('', '_x'))
    df_ag = df_ag[['code', 'name', 'date', 'close', 'net_adv_perc', 'ADL', 'ADL_perc', 'vol_perc',
       'amt_perc', 'net_amt', 'TRIN']]

    df_individual = df_out[df_out['code'] != 'SH000001'].reset_index().drop('index', axis=1)

    logging.info("last 5 days market")
    finlib.Finlib().pprint(df_ag[-5:])
    csv_tmp = root_dir+"/ag_index_adl.csv"
    df_ag.to_csv(csv_tmp, encoding='UTF-8', index=False)
    logging.info("saved to "+csv_tmp)


    logging.info("\ntop 5 ADL_perc stocks")
    df_tmp = df_individual.sort_values('ADL_perc', ascending=False, inplace=False)[0:5]
    finlib.Finlib().pprint(df_tmp)
    csv_tmp = root_dir+"/adl_perc_top_5_stocks.csv"
    df_tmp.to_csv(csv_tmp, encoding='UTF-8', index=False)
    logging.info("saved to "+csv_tmp)

    logging.info("\ntop 5 vol_perc stocks")
    df_tmp = df_individual.sort_values('vol_perc', ascending=False, inplace=False)[0:5]
    finlib.Finlib().pprint(df_tmp)
    csv_tmp = root_dir+"/vol_perc_top_5_stocks.csv"
    df_tmp.to_csv(csv_tmp, encoding='UTF-8', index=False)
    logging.info("saved to "+csv_tmp)



    logging.info("\ntop 5 amt_perc stocks")
    df_tmp = df_individual.sort_values('amt_perc', ascending=False, inplace=False)[0:5]
    finlib.Finlib().pprint(df_tmp)
    csv_tmp = root_dir+"/amt_perc_top_5_stocks.csv"
    df_tmp.to_csv(csv_tmp, encoding='UTF-8', index=False)
    logging.info("saved to "+csv_tmp)


    # df = df_merged[['date','ADL_perc','vol_perc','amt_perc','close','net_amt']]
    df = df_ag[['date', 'ADL_perc', 'vol_perc', 'amt_perc', 'close']]
    df['date'] = pd.to_datetime(df_ag['date'].values, format='%Y-%m-%d')
    df = df.set_index('date')

    # plt.tight_layout()
    if show_plot:
        ax = df.plot(subplots=True, sharex=True, grid=False)
        #
        #
        # fn = root_dir+"/adl_trin_plot_ag_index_plot"
        # ax.get_figure().savefig(fn)
        # logging.info("figure saved to " + fn + "\n")



### MAIN ####
if __name__ == '__main__':

    parser = OptionParser()

    parser.add_option("-n", "--exam_days", type="int", dest="exam_days", default=14,
                      help="the number of the latest N days to be checked.")


    parser.add_option("-s","--show_plot", action="store_true", dest="show_plot", default=False,
                       help="show market index plot")

    (options, args) = parser.parse_args()


    days =options.exam_days
    show_plot =options.show_plot
    root_dir = "/home/ryan/DATA/result/adl"
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    csv_out = root_dir+'/ag_adl_trin_'+str(days)+'d.csv'

    if not finlib.Finlib().is_cached(csv_out,1):
        df_in = prepare_data(days=days)
        df_out = pd.DataFrame( columns=['date','code','net_adv_perc','ADL','ADL_perc','vol_perc', 'amt_perc','net_amt','TRIN'])

        df_out = market_adl_trin(df_in=df_in, df_out=df_out, days=days)
        df_out.to_csv(csv_out, encoding='UTF-8', index=False)
        logging.info("AG Market ADL, TRIN saved to "+csv_out)

        df_out = individual_adl_trin(df_in = df_in, df_out=df_out) #comment the line if don't need run on each invididual stocks
        df_out = finlib.Finlib().add_stock_name_to_df(df=df_out, ts_pro_format=False)
        df_out.to_csv(csv_out, encoding='UTF-8', index=False)
        logging.info("Individual stock appended to "+csv_out)

    show_result(csv_out, root_dir, show_plot=show_plot)

    exit(0)
