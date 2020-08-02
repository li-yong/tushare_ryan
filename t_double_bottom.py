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

logging.getLogger('matplotlib.font_manager').disabled = True
plt.rcParams['font.family'] = ['AaTEST (Non-Commercial Use)']


def draw_a_stock(df,code,name):
  #  data_csv = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
 #   data_csv = "/home/ryan/DATA/DAY_Global/AG/SZ000651.csv"
#    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=data_csv)
    df = df.tail(90)#.head(70) #ryan_debug
    mean_window = 1 #ryan_debug
    predict_ext_win = 1 ## of days to predict

    df = df[ (df['low'] > 0) & (df['high'] > 0) & (df['open'] > 0) & (df['close'] > 0) ]
    df = df.reset_index().drop('index', axis=1)
    # use numerical integer index instead of date
    print(tabulate.tabulate(df.tail(5), headers='keys', tablefmt='psql'))

    df['date'] = df['date'].apply(lambda _d: datetime.datetime.strptime(str(_d), '%Y%m%d'))



    y_data = df['close'].rolling(window=mean_window).mean().dropna()
    data_len = y_data.__len__()

    x_date = df['date'][mean_window-1:].to_list()
    x_date_ext = df['date'][mean_window-1:].to_list()
    x_data = list(range(data_len))      # [0..209]


    for i in range(predict_ext_win):
        x_date_ext.append(x_date[-1] + datetime.timedelta(days=i+1))



    # polynomial fit of degree xx
    pol = np.polyfit(x_data, y_data, 17)
    y_pol = np.polyval(pol, np.linspace(0, data_len-1, data_len))
    y_pol_ext = np.polyval(pol, np.linspace(0, data_len-1+predict_ext_win, data_len+predict_ext_win))

    #___ plotting ___
    plt.figure(figsize=(150, 10), dpi= 120, facecolor='w', edgecolor='k')
    legend_list=[]
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
    min_max = np.diff(np.sign(np.diff(y_pol))).nonzero()[0] + 1          # local min & max
    l_min = (np.diff(np.sign(np.diff(y_pol))) > 0).nonzero()[0] + 1      # local min
    l_max = (np.diff(np.sign(np.diff(y_pol))) < 0).nonzero()[0] + 1      # local max
    # +1 due to the fact that diff reduces the original index number


    # plot
    x_min_list=[]
    y_min_list=[]
    y_min_pol_list=[]

    x_max_list=[]
    y_max_list=[]
    y_max_pol_list=[]


    for i in l_min:
        x_min_list.append(x_date[i])
        y_min_list.append(y_data[i])
        y_min_pol_list.append(y_pol[i])
        plt.annotate(x_date[i].strftime("%m-%d")+" "+str(round(y_pol[i],2)), (x_date[i], y_pol[i]),  label="min", color='r')

    for i in l_max:
        x_max_list.append(x_date[i])
        y_max_list.append(y_data[i])
        y_max_pol_list.append(y_pol[i])
        plt.annotate(x_date[i].strftime("%m-%d")+" "+str(round(y_pol[i])), (x_date[i], y_pol[i]), label="min", color='b')



    #fit the chart left minimal
    pol_min = np.polyfit(l_min[:2], y_min_pol_list[:2], 1)
    y_pol_min = np.polyval(pol_min, np.linspace(0, data_len-1+predict_ext_win, data_len+predict_ext_win))
    plt.plot_date(x_date_ext, y_pol_min, '-', color='blue')

    pol_max = np.polyfit(l_max[:2], y_max_pol_list[:2], 1)
    y_pol_max = np.polyval(pol_max, np.linspace(0, data_len-1+predict_ext_win, data_len+predict_ext_win))
    plt.plot_date(x_date_ext, y_pol_max, '-', color='blue')


    #fit the chart right minimal
    pol_min = np.polyfit(l_min[-2:], y_min_pol_list[-2:], 1)
    y_pol_min = np.polyval(pol_min, np.linspace(0, data_len-1+predict_ext_win, data_len+predict_ext_win))
    plt.plot_date(x_date_ext, y_pol_min, '-', color='green', markersize=0.5)

    pol_max = np.polyfit(l_max[-2:], y_max_pol_list[-2:], 1)
    y_pol_max = np.polyval(pol_max, np.linspace(0, data_len-1+predict_ext_win, data_len+predict_ext_win))
    plt.plot_date(x_date_ext, y_pol_max, '-', color='green', markersize=0.5)



    # print('corresponding LOW values for suspected indeces: ')
    print(df.low.iloc[l_min])


    #plt.figure(figsize=(150, 2), dpi= 120, facecolor='w', edgecolor='k')
    #plt.plot_date(x_date, y_pol, color='grey')
    #legend_list.append('polyfit_2')

    #plt.plot_date(x_min_list, y_pol[l_min], "o", label="min", color='r')        # minima
    #plt.plot_date( y_pol[l_min], "o", label="min", color='r')        # minima
    #legend_list.append('min_p')
    #plt.plot_date(x_max_list, y_pol[l_max], "o", label="max", color='b')        # maxima
    #plt.plot_date( y_pol[l_max], "o", label="max", color='b')        # maxima
    #legend_list.append('max_p')

    plt.title(code+" "+name+" ")

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
        print("vline:"+str(x_date[position]))
        plt.axvline(x=x_date[position], linestyle='-.', color='r')

    plt.axhline(threshold, linestyle='--', color='b')

    for key in dict_x.keys():
        # print('dict key value: ', dict_i[key])
        for value in dict_x[key]:
            if value in range(x_date.__len__()):
                plt.axvline(x=x_date[value], linestyle='-', color='lightblue', alpha=0.2)

    plt.show()

    # print('dict_x: ', dict_x)   # this dictionary is holding the values of the suspected low price
    # print('y_dict:', y_dict)

    pass
    #end_of_the_function

def main():
    stock_list_pri = finlib.Finlib().prime_stock_list()
    stock_list_all = finlib.Finlib().get_A_stock_instrment()
    stock_list_all = finlib.Finlib().add_market_to_code(df=stock_list_all)

    for i  in range(stock_list_pri.__len__()):
        code = stock_list_pri.iloc[i]['code']
        name = stock_list_pri.iloc[i]['name']
        csv= '/home/ryan/DATA/DAY_Global/AG/'+code+".csv"
        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv, add_market=False)
        draw_a_stock(df=df,code=code,name=name)
        pass

    # for i  in range(stock_list_all.__len__()):
    #    code = stock_list_all.iloc[i]['code']
    #    name = stock_list_all.iloc[i]['name']
    #    csv= '/home/ryan/DATA/DAY_Global/AG/'+code+".csv"
    #    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv, add_market=True)
    #    pass


### MAIN ####
if __name__ == '__main__':
    main()