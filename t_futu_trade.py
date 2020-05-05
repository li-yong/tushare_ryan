# coding: utf-8
#from futuquant import *
#import futuquant as ft

#from futu import *
import futu as ft
import sys
import re
import pandas as pd
import time

#Depends on futu demon
#Step1: /home/ryan/FutuOpenD_1.03_Ubuntu16.04/FutuOpenD &
#Step2:  python3a $0
# /usr/bin/python3a: symbolic link to /hdd2/anaconda3/bin/python3


def buy_limit(quote_ctx,
              trd_ctx,
              df_stock_info,
              code,
              drop_threshold=0.19,
              pwd_unlock='123456',
              trd_env=ft.TrdEnv.SIMULATE,
              time_sleep=4):

    ###
    #ret, data = quote_ctx.get_market_snapshot(code)
    #df = df_market_snapshot[df_market_snapshot['code']==code]

    #if ret == ft.RET_OK:
    lot_size = df_stock_info.iloc[0]['lot_size']
    last_price = df_stock_info.iloc[0]['last_price']
    prev_close_price = df_stock_info.iloc[0]['prev_close_price']

    price_to_order = last_price * (1 - drop_threshold)
    price_to_order = round(price_to_order, 2)
    #else:
    #    lot_size = 0
    #    price_to_order = 0

    sys.stdout.write("Placing buying limit order, " + code + ", price: " +
                     str(price_to_order) + ", lot: " + str(lot_size) +
                     ", env: " + str(trd_env))

    ret, order_table = trd_ctx.place_order(price=price_to_order,
                                           qty=lot_size,
                                           code=code,
                                           trd_side=ft.TrdSide.BUY,
                                           trd_env=trd_env,
                                           order_type=order_type)

    if ret == ft.RET_OK:
        print(". Done")
    else:
        print(". Failed: " + order_table)
    time.sleep(time_sleep)


if __name__ == '__main__':

    #init
    ip = '127.0.0.1'
    port = 11111
    pwd_unlock = '731024'
    #code = "US.AAPL"
    #code = "HK.00700"
    #code = "HK.03337"
    drop_threshold = 0.19  #buy at 19% drop
    order_type = ft.OrderType.NORMAL
    trd_env = ft.TrdEnv.SIMULATE

    #prepare
    quote_ctx = ft.OpenQuoteContext(host=ip, port=port)
    trd_ctx_hk = ft.OpenHKTradeContext(host=ip, port=port)
    trd_ctx_us = ft.OpenUSTradeContext(host=ip, port=port)

    f_dow = '/home/ryan/DATA/pickle/INDEX_US_HK/dow.csv'
    f_hkhs = '/home/ryan/DATA/pickle/INDEX_US_HK/hkhs.csv'
    f_nasdqa100 = '/home/ryan/DATA/pickle/INDEX_US_HK/nasdqa100.csv'

    df_dow = pd.read_csv(f_dow, converters={'code': str})
    df_hkhs = pd.read_csv(f_hkhs, converters={'code': str})
    df_nasdqa100 = pd.read_csv(f_nasdqa100, converters={'code': str})

    df_input = pd.DataFrame(columns=['code', 'name'])

    for index, row in df_dow.iterrows():
        code = 'US.' + row['code']
        name = row['name']
        new_df = pd.DataFrame([[code, name]], columns=['code', 'name'])
        df_input = df_input.append(new_df, ignore_index=True)

    for index, row in df_nasdqa100.iterrows():
        code = 'US.' + row['code']
        name = row['name']
        new_df = pd.DataFrame([[code, name]], columns=['code', 'name'])
        df_input = df_input.append(new_df, ignore_index=True)

    for index, row in df_hkhs.iterrows():
        code = 'HK.' + row['code']
        name = row['name']
        new_df = pd.DataFrame([[code, name]], columns=['code', 'name'])
        df_input = df_input.append(new_df, ignore_index=True)

    ### Buy according to df_input
    ret, df_market_snapshot = quote_ctx.get_market_snapshot(
        df_input['code'].tolist())
    if ret != ft.RET_OK:
        raise Exception('Failed to get_market_snapshot')

    ret, data = trd_ctx_hk.unlock_trade(pwd_unlock)
    if ret != ft.RET_OK:
        raise Exception('Failed to unlock trade, HK')

    ret, data = trd_ctx_us.unlock_trade(pwd_unlock)
    if ret != ft.RET_OK:
        raise Exception('Failed to unlock trade, US')

    #checking account
    ret, df_accinfo_hk = trd_ctx_hk.accinfo_query(trd_env=trd_env)
    if ret != ft.RET_OK:
        raise Exception("Cannot get account info, HK")

    ret, df_accinfo_us = trd_ctx_us.accinfo_query(trd_env=trd_env)
    if ret != ft.RET_OK:
        raise Exception("Cannot get account info, US")
    '''
    power_us =df_accinfo_hk.iloc[0]['power'] #99804.33
    total_assets=data.iloc[0]['total_assets'] #100000.00
    cash=data.iloc[0]['cash'] #100000.00
    market_val=data.iloc[0]['market_val'] # 0.00
    frozen_cash=data.iloc[0]['frozen_cash'] #195.67
    avl_withdrawal_cash=data.iloc[0]['avl_withdrawal_cash'] # 0.00



    power = data.iloc[0]['power']  # 99804.33
    total_assets = data.iloc[0]['total_assets']  # 100000.00
    cash = data.iloc[0]['cash']  # 100000.00
    market_val = data.iloc[0]['market_val']  # 0.00
    frozen_cash = data.iloc[0]['frozen_cash']  # 195.67
    avl_withdrawal_cash = data.iloc[0]['avl_withdrawal_cash']  # 0.00
    '''

    #checking orders(in queue)
    ret, df_order_list_hk = trd_ctx_hk.order_list_query(trd_env=trd_env)
    if ret != ft.RET_OK:
        raise Exception("Cannot get order info, HK")

    ret, df_order_list_us = trd_ctx_us.order_list_query(trd_env=trd_env)
    if ret != ft.RET_OK:
        raise Exception("Cannot get order info, US")

    #checking postion
    ret, df_position_list_hk = trd_ctx_hk.position_list_query(trd_env=trd_env)
    if ret != ft.RET_OK:
        raise Exception("Failed to get position, HK")

    ret, df_position_list_us = trd_ctx_us.position_list_query(trd_env=trd_env)
    if ret != ft.RET_OK:
        raise Exception("Failed to get position, US")

    i_cnt = 1
    length = df_input.__len__()

    for index, row in df_input.iterrows():
        sys.stdout.write(str(i_cnt) + " of " + str(length) + ". ")
        i_cnt = i_cnt + 1
        code = row['code']
        df_stock_info = df_market_snapshot[df_market_snapshot['code'] == code]

        if re.match('.*HK.*', code):
            trd_ctx = trd_ctx_hk
            df_position = df_position_list_hk
            df_order_list = df_order_list_hk

        if re.match('.*US.*', code):
            trd_ctx = trd_ctx_us
            df_position = df_position_list_us
            df_order_list = df_order_list_us

        #order check
        df = df_order_list[df_order_list['code'] == code].reset_index().drop(
            'index', axis=1)

        if df.__len__() > 0:
            print("already have the open order for " + code +
                  " . Not making new order")
            for i in range(df.__len__()):
                stock_name = df.iloc[i]['stock_name']  # 安东油田服务
                trd_side = df.iloc[i]['trd_side']  # BUY
                order_status = df.iloc[i]['order_status']  # SUBMITTED
                price = df.iloc[i]['price']  # 1.11
                qty = df.iloc[i]['qty']  # 2000
                create_time = df.iloc[i]['create_time']  # 2018-09-16 06:53:10
                updated_time = df.iloc[i][
                    'updated_time']  # 2018-09-16 06:53:10
                dealt_qty = df.iloc[i]['dealt_qty']  # 0
                dealt_avg_price = df.iloc[i]['dealt_avg_price']  # 0
                print("\t" + stock_name + " " + str(qty) + " " +
                      str(trd_side) + " " + str(price))
            continue

        #postion check
        df = df_position[df_position['code'] == code].reset_index().drop(
            'index', axis=1)

        if df.__len__() > 0:
            print("already have the open position for " + code +
                  " . Not making new order")
            for i in range(df.__len__()):
                stock_name = df.iloc[i]['stock_name']  # 腾讯控股
                qty = df.iloc[i]['qty']  # 100
                can_sell_qty = df.iloc[i]['can_sell_qty']  # 100
                cost_price = df.iloc[i]['cost_price']  # 331.4
                nominal_price = df.iloc[i]['nominal_price']  # 330
                position_side = df.iloc[i]['position_side']  # LONG
                pl_ratio = df.iloc[i]['pl_ratio']  # -0.42245
                print("\t" + stock_name + " " + str(qty) + " " +
                      str(position_side) + " " + str(pl_ratio))
            continue

        #place order
        buy_limit(quote_ctx=quote_ctx,
                  trd_ctx=trd_ctx,
                  df_stock_info=df_stock_info,
                  code=code,
                  drop_threshold=drop_threshold,
                  pwd_unlock=pwd_unlock,
                  trd_env=trd_env,
                  time_sleep=4)

    #### clean up
    quote_ctx.close()
    trd_ctx_hk.close()
    trd_ctx_us.close()
