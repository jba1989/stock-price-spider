# -*- coding: utf-8 -*-
"""
Created on Sun Feb 28 21:01:42 2021

@author: Lonelygod
"""

import requests
import pandas as pd
from dotenv import dotenv_values

# 讀取 .env 文件中的所有變數
env_vars = dotenv_values(".env")

# api 網址從 .env 讀取
url = database_url = env_vars["APP_STOCK_LIST_API"]
response = requests.get(url)
data = response.json()

columns = ['code', 'company']  #幫收集到的資料設定表頭

df = pd.DataFrame(data, columns = columns)
#將Twstock抓到的清單轉成Data Frame格式的資料表

filename = './stock-list-v3.csv'
#指定Data Frame轉存Csv檔案的檔名與路徑

df.to_csv(filename, encoding = 'utf-8-sig', index = False)
#將Data Frame轉存為Csv檔案