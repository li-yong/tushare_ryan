# coding: utf-8

# # List of all symbol / tickers in US's major indices and exchanges
#
# In this notebook you can get the list of all constituents / components and tickers in S&P500, S&P400, Dow Jones and NASDAQ100
# Retrieved from wikipedia and Nasdaq websites
#
# You'll get a dictionary with dataframes and csv files

# In[1]:

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests

output_csv_base = '~/DATA/pickle/INDEX_US_HK'

# ## Get components / constituents of indices

# In[2]:

#this code is stable as long as the following addresses are not being changed and thier structure not change
urls = {'SNP500': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'sp400': 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 'dow': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average', 'nasdqa100': 'https://www.nasdaq.com/market-activity/quotes/nasdaq-ndx-index?render=download'}
indices = {}
for url in urls:
    print("getting " + url + " from " + urls[url])

    if url == 'nasdqa100':
        df = pd.read_csv(urls[url], usecols=[0, 1], header=0)
        df.columns = ['code', 'name']
    else:
        result = requests.get(urls[url], verify=False)
        c = result.content
        bs = BeautifulSoup(c, 'lxml')
        table = str(bs.find('table', {'class': 'wikitable sortable'}))
        df = pd.read_html(table)[0]
        #df.columns = df.iloc[0]  #set the first row as columns header
        #df.drop(0, inplace=True)  #drop the first row
        if url == 'dow':
            df = df[['Symbol', 'Company']]
            df.columns = ['code', 'name']
        elif url == 'SNP500':
            #df = df[['Ticker symbol', 'Security', 'CIK']]
            df = df[['Symbol', 'Security', 'CIK', 'Founded']]
            df.columns = ['code', 'name', 'cik', 'founded']
        else:  #SP400
            #df = df[['Ticker Symbol', 'Company']]
            df = df[['Ticker symbol', 'Security']]
            #df.columns = ['Symbol', 'Name']
            df.columns = ['code', 'name']
    df = df.rename(columns={'Symbol': 'code', 'Name': 'name'})
    df.to_csv(output_csv_base + "/" + url + '.csv', index=False, encoding='utf-8')
    print("saved " + output_csv_base + "/" + url + '.csv')
    indices[url] = df.reset_index(drop=True)

# ## Get symbols of major exchanges in US
#
# You'll get a dictionaty of dataframes, merged dataframe and csv files

# In[3]:

urls = {'NYSE': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download', 'AMEX': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download', 'NASDAQ': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download'}

exchanges = {}
for i in urls:
    print("getting exchange from " + i)
    exchanges[i] = pd.read_csv(urls[i], usecols=[0, 1], header=0)

#exchanges = {i: pd.read_csv(urls[i], usecols=[0, 1], header=0) for i in urls}

# In[4]:

merged_exchanges = pd.DataFrame(columns=['code'])
for i in exchanges:
    exchanges[i].columns = ['code', 'name']
    exchanges[i].to_csv(output_csv_base + "/" + i + '.csv', index=False, encoding='utf-8')
    print("saved " + output_csv_base + "/" + i + '.csv')

    merged_exchanges = merged_exchanges.merge(exchanges[i], 'outer', ['code'], sort=True).fillna('')
merged_exchanges.to_csv(output_csv_base + "/" + 'merged_exchanges.csv', index=False, encoding='utf-8')
print("saved " + output_csv_base + "/" + 'merged_exchanges.csv')
