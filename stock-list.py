# -*- coding: utf-8 -*-
"""
Created on Sun Feb 28 21:01:42 2021

@author: Lonelygod
"""

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup as bf

url = 'https://www.twse.com.tw/zh/stockSearch/stockSearch'
html = requests.get(url)
sp = bf(html.text, 'html.parser')

stock_list = []

links = sp.select('td a')

for link in links:
    matches = re.search(r'(\d+)(\S+)', link.text)
    stock_list.append([matches.group(1),  matches.group(2)])

columns = ['stock_id', 'company']  #幫收集到的資料設定表頭

df = pd.DataFrame(stock_list, columns = columns)
#將Twstock抓到的清單轉成Data Frame格式的資料表

filename = './stock-list.csv'
#指定Data Frame轉存Csv檔案的檔名與路徑

df.to_csv(filename, encoding = 'utf-8-sig', index = False)
#將Data Frame轉存為Csv檔案