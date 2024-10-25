# -*- coding: utf-8 -*-
"""
Created on Sun Feb 28 21:01:42 2021

@author: Lonelygod
"""

import base64
import random
import re
import time
import requests
import pandas as pd
import os
from dotenv import dotenv_values

# 讀取 .env 文件中的所有變數
env_vars = dotenv_values(".env")

# api 網址從 .env 讀取
api_url = database_url = env_vars["API_URL"]
stock_list = []
i = 0

while i <= 99:
    print('現在搜尋: ', i)

    query = 'https://www.twse.com.tw/rwd/zh/api/codeQuery?query=' + str(i).zfill(2)
    query = query.encode('ascii')

    if api_url is not None:
        url = api_url + '?query=' + base64.b64encode(query).decode('ascii')
        print('url: ', url)
        response = requests.get(url)
        data = response.json()

        for suggestion in data['suggestions']:
            matches = re.search(r'(\d+)\t(.*)', suggestion)

            if matches:
                stock_list.append([matches.group(1),  matches.group(2)])
        i += 1
        time.sleep(1)

columns = ['code', 'company']  #幫收集到的資料設定表頭

df = pd.DataFrame(stock_list, columns = columns)
#將Twstock抓到的清單轉成Data Frame格式的資料表

filename = './stock-list-v2.csv'
#指定Data Frame轉存Csv檔案的檔名與路徑

df.to_csv(filename, encoding = 'utf-8-sig', index = False)
#將Data Frame轉存為Csv檔案