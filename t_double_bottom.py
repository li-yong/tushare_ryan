# ___library_import_statements___
import pandas as pd

# for pandas_datareader, otherwise it might have issues, sometimes there is some version mismatch
pd.core.common.is_list_like = pd.api.types.is_list_like

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import time
import finlib
import re
import logging
import tabulate
from optparse import OptionParser
import os

logging.getLogger('matplotlib.font_manager').disabled = True
plt.rcParams['font.family'] = ['AaTEST (Non-Commercial Use)']


def draw_a_stock(df, code, name, show_fig_f=False, save_fig_f=False, min_sample=500):
    rtn_dict = {}
    rtn_dict['hit']=False

    if df.__len__() < min_sample:
        return (rtn_dict)

    df = df.tail(min_sample)  #.head(70) #ryan_debug
    mean_window = 1  #ryan_debug
    predict_ext_win = 1  ## of days to predict

    df = df[(df['low'] > 0) & (df['high'] > 0) & (df['open'] > 0) & (df['close'] > 0)]
    df = df.reset_index().drop('index', axis=1)
    # use numerical integer index instead of date
    print(tabulate.tabulate(df.tail(2), headers='keys', tablefmt='psql'))

    if df.__len__() < min_sample:
        return (rtn_dict)


    df['date'] = df['date'].apply(lambda _d: datetime.datetime.strptime(str(_d), '%Y%m%d'))

    y_data = df['close'].rolling(window=mean_window).mean().dropna()
    data_len = y_data.__len__()

    x_date = df['date'][mean_window - 1:].to_list()
    x_date_ext = df['date'][mean_window - 1:].to_list()
    x_data = list(range(data_len))  # [0..209]

    the_day = x_date[-1].strftime("%Y%m%d")

    for i in range(predict_ext_win):
        x_date_ext.append(x_date[-1] + datetime.timedelta(days=i + 1))

    # polynomial fit of degree xx
    pol = np.polyfit(x_data, y_data, 17)
    y_pol = np.polyval(pol, np.linspace(0, data_len - 1, data_len))
    y_pol_ext = np.polyval(pol, np.linspace(0, data_len - 1 + predict_ext_win, data_len + predict_ext_win))
    rtn_dict['y_pol']=round(y_pol[-1],2)
    rtn_dict['y_data'] = round(y_data.iloc[-1], 2)

    #___ plotting ___
    plt.figure(figsize=(25, 10), facecolor='w', edgecolor='k')
    #plt.figure(figsize=(150, 10), dpi=120, facecolor='w', edgecolor='k')
    legend_list = []
    #plt.xticks(rotation=90)

    # plot stock data
    plt.plot_date(x_date, y_data, 'o', markersize=1.5, color='grey', alpha=0.7)
    legend_list.append('stock data')
    # plot polynomial fit
    plt.plot_date(x_date_ext, y_pol_ext, '-', markersize=1.0, color='black', alpha=0.9)
    legend_list.append('polynomial fit')

    #plt.show()

    ########################################
    # ___ detection of local minimums and maximums ___
    ########################################
    min_max = np.diff(np.sign(np.diff(y_pol))).nonzero()[0] + 1  # local min & max
    l_min = (np.diff(np.sign(np.diff(y_pol))) > 0).nonzero()[0] + 1  # local min
    l_max = (np.diff(np.sign(np.diff(y_pol))) < 0).nonzero()[0] + 1  # local max
    # +1 due to the fact that diff reduces the original index number

    # plot
    x_min_list = []
    y_min_list = []
    y_min_pol_list = []

    x_max_list = []
    y_max_list = []
    y_max_pol_list = []

    for i in l_min:
        x_min_list.append(x_date[i])
        y_min_list.append(y_data[i])
        y_min_pol_list.append(y_pol[i])
        plt.annotate(x_date[i].strftime("%m-%d") + " " + str(round(y_pol[i], 2)), (x_date[i], y_pol[i]), label="min", color='r')

    for i in l_max:
        x_max_list.append(x_date[i])
        y_max_list.append(y_data[i])
        y_max_pol_list.append(y_pol[i])
        plt.annotate(x_date[i].strftime("%m-%d") + " " + str(round(y_pol[i])), (x_date[i], y_pol[i]), label="min", color='b')

    #fit the chart left minimal
    pol_min_left = np.polyfit(l_min[:2], y_min_pol_list[:2], 1)
    y_pol_min_left = np.polyval(pol_min_left, np.linspace(0, data_len - 1 + predict_ext_win, data_len + predict_ext_win))
    rtn_dict['pol_min_left_2'] = round(y_pol_min_left[-1],2)
    plt.plot_date(x_date_ext, y_pol_min_left, '-', color='blue')

    pol_max_left = np.polyfit(l_max[:2], y_max_pol_list[:2], 1)
    y_pol_max_left = np.polyval(pol_max_left, np.linspace(0, data_len - 1 + predict_ext_win, data_len + predict_ext_win))
    plt.plot_date(x_date_ext, y_pol_max_left, '-', color='blue')

    #fit the chart right minimal
    pol_min_right = np.polyfit(l_min[-2:], y_min_pol_list[-2:], 1)
    y_pol_min_right = np.polyval(pol_min_right, np.linspace(0, data_len - 1 + predict_ext_win, data_len + predict_ext_win))
    plt.plot_date(x_date_ext, y_pol_min_right, '-', color='green', markersize=0.5)

    pol_max_right = np.polyfit(l_max[-2:], y_max_pol_list[-2:], 1)
    y_pol_max_right = np.polyval(pol_max_right, np.linspace(0, data_len - 1 + predict_ext_win, data_len + predict_ext_win))
    plt.plot_date(x_date_ext, y_pol_max_right, '-', color='green', markersize=0.5)

    pol_min_right_3 = np.polyfit(l_min[:3], y_min_pol_list[:3], 1)
    slop_3_degree_min = np.arctan(pol_min_right_3[0]) * 180 / np.pi
    plt.annotate("min 3p deg: "+str(round(slop_3_degree_min, 2)),xy=(10, 20), xycoords='figure points')
    if (slop_3_degree_min > 30):
        print("a very good right min slop degree " + str(round(slop_3_degree_min, 2)))
        rtn_dict['hit'] = True
        rtn_dict['min_slop_degree_3'] = round(slop_3_degree_min, 2)

    pol_max_right_3 = np.polyfit(l_max[:3], y_max_pol_list[:3], 1)
    slop_3_degree_max = np.arctan(pol_max_right_3[0]) * 180 / np.pi
    plt.annotate("max 3p deg: " + str(round(slop_3_degree_max, 2)),xy=(10, 10), xycoords='figure points')
    if (slop_3_degree_max > 30):
        rtn_dict['hit'] = True
        print("a very good right max slop degree " + str(round(slop_3_degree_max, 2)))
        rtn_dict['max_slop_degree_3'] = round(slop_3_degree_max, 2)

    # print('corresponding LOW values for suspected indeces: ')
    #print(df.low.iloc[l_min])

    #plt.figure(figsize=(150, 2), dpi= 120, facecolor='w', edgecolor='k')
    #plt.plot_date(x_date, y_pol, color='grey')
    #legend_list.append('polyfit_2')

    #plt.plot_date(x_min_list, y_pol[l_min], "o", label="min", color='r')        # minima
    #plt.plot_date( y_pol[l_min], "o", label="min", color='r')        # minima
    #legend_list.append('min_p')
    #plt.plot_date(x_max_list, y_pol[l_max], "o", label="max", color='b')        # maxima
    #plt.plot_date( y_pol[l_max], "o", label="max", color='b')        # maxima
    #legend_list.append('max_p')

    plt.title(code + " " + name + " " + the_day + " " + str(round(y_data.iloc[-1], 2)))

    # I choose using fitted ploynomial number as breakthough vaule

    #plt.show()

    ########################################
    # Extend range
    ########################################
    # extend the suspected x range:
    delta = 10  # how many ticks to the left and to the right from local minimum on x axis

    dict_i = dict()
    dict_x = dict()

    df_len = len(df.index)  # number of rows in dataset

    for element in l_min:  # x coordinates of suspected minimums
        l_bound = element - delta  # lower bound (left)
        u_bound = element + delta  # upper bound (right)
        x_range = range(l_bound, u_bound + 1)  # range of x positions where we SUSPECT to find a low
        dict_x[element] = x_range  # just helpful dictionary that holds suspected x ranges for further visualization strips

        # print('x_range: ', x_range)

        y_loc_list = list()
        for x_element in x_range:
            # print('-----------------')
            if x_element > 0 and x_element < df_len:  # need to stay within the dataframe
                # y_loc_list.append(df.low.iloc[x_element])   # list of suspected y values that can be a minimum
                y_loc_list.append(df.low.iloc[x_element])
                # print(y_loc_list)
                # print('df.low.iloc[x_element]', df.low.iloc[x_element])
        dict_i[element] = y_loc_list  # key in element is suspected x position of minimum
        # to each suspected minimums we append the price values around that x position
        # so 40: [53.70000076293945, 53.93000030517578, 52.84000015258789, 53.290000915527344]
        # x position: [ 40$, 39$, 41$, 45$]
    # print('DICTIONARY for l_min: ', dict_i)

    y_delta = 0.12  # percentage distance between average lows
    threshold = min(df['low']) * 1.15  # setting threshold higher than the global low

    y_dict = dict()
    mini = list()
    suspected_bottoms = list()
    #   BUG somewhere here
    for key in dict_i.keys():  # for suspected minimum x position
        mn = sum(dict_i[key]) / len(dict_i[key])  # this is averaging out the price around that suspected minimum
        # if the range of days is too high the average will not make much sense

        price_min = min(dict_i[key])
        mini.append(price_min)  # lowest value for price around suspected

        l_y = mn * (1.0 - y_delta)  # these values are trying to get an U shape, but it is kinda useless
        u_y = mn * (1.0 + y_delta)
        y_dict[key] = [l_y, u_y, mn, price_min]

    # print('y_dict: ')
    # print(y_dict)

    # print('SCREENING FOR DOUBLE BOTTOM:')

    for key_i in y_dict.keys():
        for key_j in y_dict.keys():
            if (key_i != key_j) and (y_dict[key_i][3] < threshold):
                suspected_bottoms.append(key_i)

    # ___ plotting ___
    #plt.figure(figsize=(20, 10), dpi=120, facecolor='w', edgecolor='k')

    # plot stock data
    #plt.plot_date(x_date, y_data, 'o', markersize=1.5, color='magenta', alpha=0.7)
    #legend_list.append('stock_data_2')

    # we can plot also all the other prices to get a price range for given day just for information
    #plt.plot_date(x_date, df['high'], 'o', markersize=1.5, color='blue', alpha=0.7)
    #plt.plot_date(x_date, df['open'], 'o', markersize=1.5, color='grey', alpha=0.7)
    #plt.plot_date(x_date, df['close'], 'o', markersize=1.5, color='red', alpha=0.7)  # Adj Close should be more accurate indication (accounts for dividends and stock splits)

    # plot polynomial fit
    #plt.plot_date(x_date, y_pol, '-', markersize=1.0, color='black', alpha=0.9)
    #legend_list.append('poly_fit_3')
    plt.legend(legend_list)

    for position in suspected_bottoms:
        #print("vline:"+str(x_date[position]))
        plt.axvline(x=x_date[position], linestyle='-.', color='r')

    plt.axhline(threshold, linestyle='--', color='b')

    for key in dict_x.keys():
        # print('dict key value: ', dict_i[key])
        for value in dict_x[key]:
            if value in range(x_date.__len__()):
                plt.axvline(x=x_date[value], linestyle='-', color='lightblue', alpha=0.2)

    if save_fig_f:
        if rtn_dict['hit']:
            fn = "/home/ryan/DATA/result/curv_plot/" + code + "_" + name + "_" + the_day + "_hitted.png"
        else:
            fn = "/home/ryan/DATA/result/curv_plot/" + code + "_" + name + "_" + the_day + ".png"

        plt.savefig(fn, bbox_inches='tight')
        print("figure saved to " + fn + "\n")

    if show_fig_f:
        plt.show()

    plt.close('all')
    plt.clf()

    # print('dict_x: ', dict_x)   # this dictionary is holding the values of the suspected low price
    # print('y_dict:', y_dict)

    rtn_dict['code'] = code
    rtn_dict['name'] = name
    rtn_dict['date'] = the_day
    '''
    rtn_dict['cur_price'] = r['pri_cur']
    rtn_dict['p_max'] = r['p_max']
    rtn_dict['p_min'] = r['p_min']
    rtn_dict['percent'] = r['per_cur']
    rtn_dict['long_take_profit_percent'] = r['long_take_profit_percent']
    rtn_dict['long_stop_lost_percent'] = r['long_stop_lost_percent']
    rtn_dict['long_in_p'] = r['long_enter_price']
    rtn_dict['long_tp_p'] = r['long_take_profit_price']
    rtn_dict['long_sl_p'] = r['long_stop_lost_price']

    rtn_dict['hit_sum'] = r['current_hit_cnt']['sum_cnt']
    rtn_dict['h_cnt'] = r['current_hit_cnt']['h_cnt']
    rtn_dict['l_cnt'] = r['current_hit_cnt']['l_cnt']
    rtn_dict['o_cnt'] = r['current_hit_cnt']['o_cnt']
    rtn_dict['c_cnt'] = r['current_hit_cnt']['c_cnt']
    '''

    return (rtn_dict)


def main():
    ########################
    #
    #########################

    parser = OptionParser()

    parser.add_option("-b", "--begin_date", type="string", action="store", dest="begin_date_f", default="2018-01-01", help="begin date to use Fibo")

    parser.add_option("--min_sample", type="int", action="store", dest="min_sample_f", default=500, help="minimal samples number of input to analysis")

    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug ")

    parser.add_option("--save_fig", action="store_true", dest="save_fig_f", default=False, help="save the matplot figure ")

    parser.add_option("--show_fig", action="store_true", dest="show_fig_f", default=False, help="display the matplot figure ")

    parser.add_option("--log_price", action="store_true", dest="log_price_f", default=False, help="log for y-axis price ")

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(AG)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")

    # df_rtn = pd.DataFrame()
    df_rtn = pd.DataFrame(columns=["code", "name"])

    (options, args) = parser.parse_args()
    debug_f = options.debug_f
    begin_date_f = options.begin_date_f
    show_fig_f = options.show_fig_f
    save_fig_f = options.save_fig_f
    min_sample_f = options.min_sample_f
    log_price_f = options.log_price_f
    selected = options.selected
    stock_global = options.stock_global

    rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
    out_dir = rst['out_dir']
    csv_dir = rst['csv_dir']
    stock_list = rst['stock_list']
    out_f = out_dir + "/" + stock_global.lower() + "_curve_shape.csv"  # /home/ryan/DATA/result/selected/us_index_fib.csv

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    i = 0

    for index, row in stock_list.iterrows():
        i += 1
        print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
        name, code = row['name'], row['code']

        csv_f = csv_dir + "/" + code + ".csv"
        logging.info(csv_f)

        if not os.path.isfile(csv_f):
            logging.warning(__file__ + " " + "file not exist. " + csv_f)
            continue

        df = finlib.Finlib().regular_read_csv_to_stdard_df(csv_f, add_market=False)

        if (df.__len__() < min_sample_f):
            continue

        code_name_map = stock_list

        code = df.iloc[0]['code']
        name = code_name_map[code_name_map['code'] == code].iloc[0]['name']

        if log_price_f:
            df['open'] = df['open'].apply(lambda _d: np.log(_d))
            df['close'] = df['close'].apply(lambda _d: np.log(_d))
            df['high'] = df['high'].apply(lambda _d: np.log(_d))
            df['low'] = df['low'].apply(lambda _d: np.log(_d))

        #show_fig_f=False, =False, =500
        rtn_dict_t = draw_a_stock(df=df, code=code, name=name, show_fig_f=show_fig_f, save_fig_f=save_fig_f, min_sample=min_sample_f)
        df_t = pd.DataFrame(data=rtn_dict_t, index=[0])
        # print(df_t)
        if not df_t.empty:
            df_rtn = pd.concat([df_rtn, df_t], sort=False).reset_index().drop('index', axis=1)

    df_rtn.to_csv(out_f, encoding='UTF-8', index=False)
    print(df_rtn)
    print("output saved to " + out_f)

    exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
    '''
    
    stock_list_pri = finlib.Finlib().prime_stock_list()
    stock_list_all = finlib.Finlib().get_A_stock_instrment()
    stock_list_all = finlib.Finlib().add_market_to_code(df=stock_list_all)

    #stock_list_pri = stock_list_pri[stock_list_pri['code']=="SZ002032"]

    for i in range(stock_list_pri.__len__()):
        code = stock_list_pri.iloc[i]['code']
        name = stock_list_pri.iloc[i]['name']
        csv = '/home/ryan/DATA/DAY_Global/AG/' + code + ".csv"
        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv, add_market=False)
        draw_a_stock()
        pass

    # for i  in range(stock_list_all.__len__()):
    #    code = stock_list_all.iloc[i]['code']
    #    name = stock_list_all.iloc[i]['name']
    #    csv= '/home/ryan/DATA/DAY_Global/AG/'+code+".csv"
    #    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv, add_market=True)
    #    pass



'''
