import pandas as pd
import finlib
import finlib_indicator


def verify_a_stock():
    csv_in = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
    csv_in = "/home/ryan/DATA/DAY_Global/AG/SZ000651.csv"
    #csv_in = "/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv"
    csv_out = "/home/ryan/DATA/result/tmp/SZ000651_del.csv"

    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_in)

    df = df.iloc[-300:].reset_index().drop('index', axis=1)

    ###################################
    # Prepare
    ###################################
    df = finlib_indicator.Finlib_indicator().add_ma_ema(df=df, short=5, middle=10, long=20)
    df = finlib_indicator.Finlib_indicator().add_tr_atr(df=df, short=5, middle=10, long=20)



    ######################################################
    #'code', 'date', 'close', 'short_period', 'middle_period', 'long_period', 'jincha_minor', 'jincha_minor_strength', 'sicha_minor', 'sicha_minor_strength', 'jincha_major
    ######################################################


    ######################################################
    # Bar Style
    #         date      code  open  ...  long_lower_shadow  yunxian_buy  yunxian_sell
    #299  20200619  SZ000651  58.5  ...              False        False         False
    ######################################################
    df_bar_style = finlib_indicator.Finlib_indicator().upper_body_lower_shadow(df)
    df_today_bar_style = df_bar_style[-1:].reset_index().drop('index', axis=1)    #<<<<<< TODAY BAR_STYLE



    ######################################################
    # Junxian Style. Only One Row
    #
    #      code      date  close  ...  ema_short  ema_middle   ema_long
    #0  SZ000651  20200619  58.84  ...  58.433884    58.61309  58.362801
    ######################################################
    df_today_junxian_style = finlib_indicator.Finlib_indicator().sma_jincha_sicha_duotou_koutou(df,5,10,20)    #<<<<<< TODAY JUNXIAN


    ######################################################
    #H1, price 59.0, freq perc in 300 bars 96.4 freq 10.2
    #L1, price 58.0, freq perc in 300 bars 100.0 freq 12.0
    ######################################################
    analyzer_price_dict = finlib_indicator.Finlib_indicator().price_counter(df)



    price_dict = {'price_occurence_sum':[analyzer_price_dict['price_freq_dict'][list(analyzer_price_dict['price_freq_dict'].keys())[0]]['sum']]}

    for level in ['h1','l1','h2','l2','h3','l3','h4','l4','h5','l5']:
        if level in list(analyzer_price_dict.keys()):
            price_dict[level+"_price"]=[analyzer_price_dict[level]['price']]
            price_dict[level+"_frequency_percent"]=[analyzer_price_dict[level]['frequency_percent']]
            price_dict[level+"_occurrence_percent"]=[analyzer_price_dict[level]['occurrence_percent']]
            price_dict[level+"_occurrence"]=[analyzer_price_dict[level]['occurrence']]
        else:
            price_dict[level+"_price"]=None
            price_dict[level+"_frequency_percent"]=None
            price_dict[level+"_occurrence_percent"]=None
            price_dict[level+"_occurrence"]=None

    df_today_price_dict = pd.DataFrame.from_dict(price_dict)


    ######################################################
    # Get a stock report
    ######################################################
    df_a_stock_report = df_today_bar_style.merge(df_today_junxian_style, left_index=True, right_index=True).merge(df_today_price_dict, left_index=True, right_index=True)

    ######################################################
    #115 columns. Adjust column sequence
    ######################################################
    sp = str(df_a_stock_report.iloc[0]['short_period'])
    mp = str(df_a_stock_report.iloc[0]['middle_period'])
    lp = str(df_a_stock_report.iloc[0]['long_period'])

    col = ['code', 'date', 'open', 'low', 'high',  'close', 'volume', 'short_period', 'middle_period', 'long_period']
    col.append(['very_strong_down_trend', 'very_strong_up_trend'])

    #####################  junxian
    col.append(['trend_short', 'trend_short_strength'])
    col.append(['trend_middle', 'trend_middle_strength'])
    col.append(['trend_long'])
    col.append(['duotou_pailie',   'duotou_pailie_last_bars', 'last_kongtou_pailie_date', 'last_kongtou_pailie_n_days_before'])
    col.append(['kongtou_pailie',  'kongtou_pailie_last_bars','last_duotou_pailie_date',  'last_duotou_pailie_n_days_before'])
    col.append(['jincha_minor', 'jincha_minor_strength'])
    col.append(['jincha_major', 'jincha_major_strength'])
    col.append(['sicha_minor', 'sicha_minor_strength'])
    col.append(['sicha_major', 'sicha_major_strength'])


    ##################### Bar
    col.append(['yunxian_buy','yunxian_sell'])
    col.append(['guangtou','guangjiao','small_body','cross_star'])
    col.append(['long_upper_shadow','long_lower_shadow' ])
    col.append(['upper_shadow_len','body_len','lower_shadow_len'])

    ##################### SMA, EMA, tr, etc
    col.append(['sma_short_'+sp,'sma_middle_'+mp,'sma_long_'+lp,'ema_short_'+sp,'ema_middle_'+mp,'ema_long_'+lp])
    col.append(['tr', 'atr_short_'+sp, 'atr_middle_'+mp, 'atr_long_'+lp])

    ##################### price frequence
    col.append(['price_occurence_sum'])
    col.append(['l5_price'])
    col.append(['l4_price'])
    col.append(['l3_price'])
    col.append(['l2_price'])
    col.append(['l1_price'])
    col.append(['h1_price'])
    col.append(['h2_price'])
    col.append(['h3_price'])
    col.append(['h4_price'])
    col.append(['h5_price'])

    col.append(['l5_frequency_percent'])
    col.append(['l4_frequency_percent'])
    col.append(['l3_frequency_percent'])
    col.append(['l2_frequency_percent'])
    col.append(['l1_frequency_percent'])
    col.append(['h1_frequency_percent'])
    col.append(['h2_frequency_percent'])
    col.append(['h3_frequency_percent'])
    col.append(['h4_frequency_percent'])
    col.append(['h5_frequency_percent'])

    col.append(['l5_occurrence'])
    col.append(['l4_occurrence'])
    col.append(['l3_occurrence'])
    col.append(['l2_occurrence'])
    col.append(['l1_occurrence'])
    col.append(['h1_occurrence'])
    col.append(['h2_occurrence'])
    col.append(['h3_occurrence'])
    col.append(['h4_occurrence'])
    col.append(['h5_occurrence'])

    col.append(['l5_occurrence_percent'])
    col.append(['l4_occurrence_percent'])
    col.append(['l3_occurrence_percent'])
    col.append(['l2_occurrence_percent'])
    col.append(['l1_occurrence_percent'])
    col.append(['h1_occurrence_percent'])
    col.append(['h2_occurrence_percent'])
    col.append(['h3_occurrence_percent'])
    col.append(['h4_occurrence_percent'])
    col.append(['h5_occurrence_percent'])


    df_a_stock_report = finlib.Finlib().adjust_column(df=df_a_stock_report, col_name_list=col)
    return(df_a_stock_report)


def main():
    


### MAIN ####
if __name__ == '__main__':
    main()
