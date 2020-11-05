import pandas as pd
import finlib

import finlib_indicator

import tushare as ts
import datetime

import talib 
import logging
import time
import os


def prepare_data():
    ######### For TRIN, Advance/Decline LINE #######\
    days=30
    dir = "/home/ryan/DATA/DAY_Global/AG"
    cmd1 = "head -1 "+dir+"/SH600519.csv > "+dir+"/ag_all.csv"


    cmd2 = "for i in `ls "+dir+"/SH*.csv`; do tail -"+str(days)+" $i >> "+dir+"/ag_all.csv; done"
    cmd3 = "for i in `ls "+dir+"/SZ*.csv`; do tail -"+str(days)+" $i >> "+dir+"/ag_all.csv; done"

    # print(cmd1)
    # print(cmd2) # for i in `ls SH*.csv`; do tail -300 $i >> ag_all.csv;done
    # print(cmd3) # for i in `ls SZ*.csv`; do tail -300 $i >> ag_all.csv;done


    os.system(cmd1)
    os.system(cmd2)
    os.system(cmd3)


    csv = '/home/ryan/DATA/DAY_Global/AG/ag_all.csv'
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv)
    print("generated "+dir+"/ag_all.csv, len "+str(df.__len__()))

    return(df)





def market_adl_trin(df_in, df_out, days=21):
    ###----------- AG OverAll ------------
    a = df_in['date'].unique()
    a.sort()
    dates = a[-1*days:]

    previous_adv = 0
    previous_adv_perc = 0
    ADL = 0




    for d in dates:
        _df = df_in[df_in['date']==d]
        _df_adv = _df[_df['close'] > _df['open']]
        _df_dec = _df[_df['close'] < _df['open']]
        _adv_stocks = _df_adv.__len__()
        _dec_stocks = _df_dec.__len__()
        net_adv = _adv_stocks - _dec_stocks
        net_adv_perc = round((_adv_stocks - _dec_stocks)/_df.__len__(),4)

        ADL = net_adv + previous_adv
        ADL_perc = round(net_adv_perc + previous_adv_perc,4)

        print("date "+d+", code SH000001 (AG_overall)"+" , net_adv "+str(net_adv)+", PA "+str(previous_adv)+", ADL "+str(ADL)+", ADL_perc "+str(ADL_perc))

        previous_adv = ADL
        previous_adv_perc = ADL_perc

        ### TRIN (Arms Index)
        adv_dec_cnt_ratio = round(_adv_stocks / _dec_stocks,2)
        adv_dec_vol_ratio = round(_df_adv['volume'].sum()/_df_dec['volume'].sum(),2)
        adv_dec_amt_ratio = round(_df_adv['amount'].sum()/_df_dec['amount'].sum(),2)

        TRIN = round(adv_dec_amt_ratio/adv_dec_vol_ratio,2)

        print("date "+d+", code SH00001, ad_cnt_ratio "+str(adv_dec_cnt_ratio)+", ad_vol_ratio "+str(adv_dec_vol_ratio)+", ad_amt_ratio "+str(adv_dec_amt_ratio) +
              ", TRIN "+str(TRIN))

        df_out = df_out.append({'date':d, 'code':'SH000001', 'net_adv_perc':net_adv_perc, 'ADL':ADL,'ADL_perc':ADL_perc, 'TRIN':TRIN}, ignore_index=True)

    return(df_out)
    print("Dataframe AG Market ADL, TRIN is generated")


def individual_adl_trin(df_in, df_out):
    ###----------- Individual ------------
    df = finlib.Finlib().remove_garbage(df_in, code_field_name='code', code_format='C2D6')


    codes = df['code'].unique()
    codes.sort()

    for c in codes:
        previous_adv = 0
        ADL = 0

        _df = df[df['code']==c]  #_df already has number of 'days' rows.
        _last_date = _df[['date']].iloc[-1].values[0]
        _df_adv = _df[df['close'] > df['open']]
        _df_dec = _df[df['close'] < df['open']]
        _adv_days = _df_adv.__len__()
        _dec_days = _df_dec.__len__()
        net_adv = _adv_days - _dec_days
        net_adv_perc = round(net_adv/_df.__len__(),4)

        ADL = net_adv
        ADL_perc = round(net_adv/_df.__len__(), 4)
        print("date "+_last_date+", code "+c+", net_adv_perc "+str(net_adv_perc)+", PA "+str(previous_adv)+", ADL "+str(ADL)+", ADL_perc "+str(ADL_perc))


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

        print("date "+_last_date+", code "+c+", ad_cnt_ratio "+str(adv_dec_cnt_ratio)+", ad_vol_ratio "+str(adv_dec_vol_ratio)+", ad_amt_ratio "+str(adv_dec_amt_ratio) +
              ", TRIN "+str(TRIN))

        df_out = df_out.append({'date':_last_date, 'code':c, 'net_adv_perc':net_adv_perc, 'ADL':ADL,'ADL_perc':ADL_perc, 'TRIN':TRIN}, ignore_index=True)

    return df_out









### MAIN ####
if __name__ == '__main__':
    csv_out = 'ag_adl_trin.csv'

    df_in = prepare_data()
    df_out = pd.DataFrame( columns=['date','code','net_adv_perc','ADL','ADL_perc','TRIN'])

    df_out = market_adl_trin(df_in=df_in, df_out=df_out)
    df_out.to_csv(csv_out, encoding='UTF-8', index=False)
    print("AG Market ADL, TRIN saved to "+csv_out)

    df_out = individual_adl_trin(df_in = df_in, df_out=df_out)
    df_out = finlib.Finlib().add_stock_name_to_df(df=df_out, ts_pro_format=False)
    print("Individual stock appended to "+csv_out)



    exit(0)
