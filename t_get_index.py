# coding: utf-8

import os
import tushare as ts
myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'
ts.set_token(myToken)
pro = ts.pro_api()
df = pro.index_daily(ts_code='000001.SH')

df.to_csv("/home/ryan/DATA/DAY_Global/AG/SH000001.csv",index=False)

print(" Index SH000001 saved to /home/ryan/DATA/DAY_Global/AG/SH000001.csv")
print("Done!")
os._exit(0)




#================= Below Obsolated ==============
#This script to save index data to file,


#          date      open      high     close       low       volume  \
#243 2016-12-19  3120.696  3125.284  3118.085  3110.082  15416971600
#242 2016-12-20  3115.898  3117.015  3102.876  3084.800  15742675900
#           amount
#243  174500240651
#242  171611881109


import tushare as ts
import pandas as pd
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt
import pandas
import re



dmp2001="/home/ryan/DATA/pickle/INDEX/SH00001.20010101-20021231.pickle"
dmp2003="/home/ryan/DATA/pickle/INDEX/SH00001.20030101-20041231.pickle"
dmp2005="/home/ryan/DATA/pickle/INDEX/SH00001.20050101-20061231.pickle"
dmp2007="/home/ryan/DATA/pickle/INDEX/SH00001.20070101-20081231.pickle"
dmp2009="/home/ryan/DATA/pickle/INDEX/SH00001.20090101-20101231.pickle"
dmp2011="/home/ryan/DATA/pickle/INDEX/SH00001.20110101-20121231.pickle"
dmp2013="/home/ryan/DATA/pickle/INDEX/SH00001.20130101-20141231.pickle"
dmp2015="/home/ryan/DATA/pickle/INDEX/SH00001.20150101-20161231.pickle"
dmp2017="/home/ryan/DATA/pickle/INDEX/SH00001.20170101-20171231.pickle"
dmp2018="/home/ryan/DATA/pickle/INDEX/SH00001.20180101-20181231.pickle"
dmp2019="/home/ryan/DATA/pickle/INDEX/SH00001.20190101-20191231.pickle"
dmp2020="/home/ryan/DATA/pickle/INDEX/SH00001.20200101-20201231.pickle"
dmp2021="/home/ryan/DATA/pickle/INDEX/SH00001.20210101-20211231.pickle"


#year 2001, 2 years
if not os.path.isfile(dmp2001):
    df2001 = ts.get_h_data('000001', index=True, start='2001-01-01', end='2002-12-31')
    df2001.to_pickle(dmp2001)
else:
    df2001=pandas.read_pickle(dmp2001)

#year 2003, 2 years
if not os.path.isfile(dmp2003):
    df2003 = ts.get_h_data('000001', index=True, start='2003-01-01', end='2004-12-31')
    df2003.to_pickle(dmp2003)
else:
    df2003=pandas.read_pickle(dmp2003)

#year 2005, 2 years
if not os.path.isfile(dmp2005):
    df2005 = ts.get_h_data('000001', index=True, start='2005-01-01', end='2006-12-31')
    df2005.to_pickle(dmp2005)
else:
    df2005=pandas.read_pickle(dmp2005)



#year 2007, 2 years
if not os.path.isfile(dmp2007):
    df2007 = ts.get_h_data('000001', index=True, start='2007-01-01', end='2008-12-31')
    df2007.to_pickle(dmp2007)
else:
    df2007=pandas.read_pickle(dmp2007)

#year 2009, 2 years
if not os.path.isfile(dmp2009):
    df2009 = ts.get_h_data('000001', index=True, start='2009-01-01', end='2010-12-31')
    df2009.to_pickle(dmp2009)
else:
    df2009=pandas.read_pickle(dmp2009)

#year 2011, 2 years
if not os.path.isfile(dmp2011):
    df2011 = ts.get_h_data('000001', index=True, start='2011-01-01', end='2012-12-31')
    df2011.to_pickle(dmp2011)
else:
    df2011=pandas.read_pickle(dmp2011)

#year 2013, 2 years
if not os.path.isfile(dmp2013):
    df2013 = ts.get_h_data('000001', index=True, start='2013-01-01', end='2014-12-31')
    df2013.to_pickle(dmp2013)
else:
    df2013=pandas.read_pickle(dmp2013)

#year 2015, 2 years
if not os.path.isfile(dmp2015):
    df2015 = ts.get_h_data('000001', index=True, start='2015-01-01', end='2016-12-31')
    df2015.to_pickle(dmp2015)
else:
    df2015=pandas.read_pickle(dmp2015)

#year 2017, 1 years
if not os.path.isfile(dmp2017):
    df2017 = ts.get_h_data('000001', index=True, start='2017-01-01', end='2017-12-31')
    df2017.to_pickle(dmp2017)
else:
    df2017=pandas.read_pickle(dmp2017)


#year 2018, 1 years
if not os.path.isfile(dmp2018):
    df2018 = ts.get_h_data('000001', index=True, start='2018-01-01', end='2018-12-31')
    df2018.to_pickle(dmp2018)
else:
    df2018=pandas.read_pickle(dmp2018)


#year 2019, 1 years
if not os.path.isfile(dmp2019):
    df2019 = ts.get_h_data('000001', index=True, start='2019-01-01', end='2019-12-31')
    df2019.to_pickle(dmp2019)
else:
    df2019=pandas.read_pickle(dmp2019)

#year 2020, 1 years
#if not os.path.isfile(dmp2018):
#    df2018 = ts.get_h_data('000001', index=True, start='2018-01-01', end='2018-12-31')
#    df2018.to_pickle(dmp2018)
#else:
#    df2018=pandas.read_pickle(dmp2018)

# always read current year. It is 2018 at this time.

#            open   high  close    low     volume      amount
#date
#2015-03-16  13.27  13.45  13.39  13.00   81212976  1073862784

#df2020 = ts.get_h_data('000001', index=True, start='2020-01-01')
df2020 = ts.get_h_data('000001', start='2020-01-01')


print("fetched 2020 sh000001 data, line "+str(df2020.__len__()))


    #get_k_data doesn't require:
    #1. revert data sequence, it is already in Ascending:  df = df.reindex(index=df.index[::-1])
    #2. reset index, it is already in index by seq number: df.reset_index(inplace=True)
    #df2017 = ts.get_k_data('000001', index=True, start='2017-01-01')

    #df2017=pandas.read_pickle(dmp2017)



df = df2020.append(df2019)
df = df.append(df2018)
df = df.append(df2017)
df = df.append(df2015)
df = df.append(df2013)
df = df.append(df2011)
df = df.append(df2009)
df = df.append(df2007)
df = df.append(df2005)
df = df.append(df2003)
df = df.append(df2001)


df = df.reindex(index=df.index[::-1])
df.reset_index(inplace=True)

#insert code to head
df = pd.DataFrame(['SH000001']*df.__len__(),columns=['code']).join(df)

cols = df.columns.tolist()

cols = ['code', 'date', 'open', 'high','low', 'close', 'volume', 'amount']


df = df[cols]

df.to_csv("/home/ryan/DATA/DAY_Global/AG/SH000001.csv",index=False)

print(" Index SH000001 saved to /home/ryan/DATA/DAY_Global/AG/SH000001.csv")
print("Done!")
os._exit(0)
