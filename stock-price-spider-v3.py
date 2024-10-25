# -*- coding: utf-8 -*-
"""
Created on Sun April 28 20:48:00 2024
Description: 深度優先, 時間倒序

@author: Lonelygod
"""

import json
import requests
import pandas as pd
import os
import time
import re
import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import dotenv_values

# 讀取 .env 文件中的所有變數
env_vars = dotenv_values(".env")

# api 網址從 .env 讀取
api_url = env_vars["PRICE_API_URL"]

stock_list = []
exportColumns = ['date', 'capacity', 'turnover', 'open', 'high', 'low', 'close', 'change', 'trascation']
main_path = './history/'
user='root'
request_count = 0
error_count = 0
begin_date = datetime.datetime(2024, 5, 1)
end_date = datetime.datetime(2013, 1, 1)
filter_exist_file = True
skip = True
skip_until_code = '00900'

def getStockList():
    with open('./STOCK_DAY_ALL.json', 'r') as f:
        data = json.load(f)

        global stock_list
        stock_list = [stock['Code'] for stock in data]

def checkDir(path):
    if (not os.path.exists(path)):
        print('建立資料夾: ' + path)
        os.mkdir(path, 755)
        os.system('sudo chown ' + user + ' ' + path)
        os.system('sudo chmod 755 ' + path)

def makeAllDir():
    for code in stock_list:
        # 建立資料夾
        save_path = main_path + str(code)
        checkDir(save_path)

def initialize(from_date = begin_date):
    getStockList()
    checkDir(main_path)
    makeAllDir()
    mainProcess(from_date)

def curl(date, code):
    if api_url is None:
        raise Exception('api_url is not defined')

    url = api_url + '?date=' + date.strftime("%Y%m%d") + '&code=' + str(code)
    print('url:', url)

    return requests.get(url, timeout = 10)

def getFilenameWithPath(code, date):
    return main_path + str(code) + '/' + date.strftime("%Y_%-m") + '.csv'

def myCurl(date, code):
    url = 'https://wvr50l7l8j.execute-api.ap-southeast-2.amazonaws.com/price?date=' + date.strftime("%Y%m%d") + '&code=' + str(code)
    print('url:', url)

    # 創建一個 Session 物件
    s = requests.Session()

    # 創建一個 Retry 物件，設定最大重試次數和回滾因子
    retries = Retry(total=2, backoff_factor=3, status_forcelist=[500, 502, 503, 504])

    # 創建一個 HTTPAdapter 並將 Retry 物件傳入
    s.mount('http://', HTTPAdapter(max_retries=retries))

    # 使用 Session 物件發送請求
    return s.get(url, timeout=10)

def getHistory(code, date) -> bool:
    global request_count

    request_count += 1
    res = myCurl(date, code)
    res_json = res.json()
    filename = getFilenameWithPath(code, date)

    if 'stat' in res_json and res_json['stat'] == 'OK' and res_json['date'] == date.strftime("%Y%m01"):
        res_code = re.search(r'([0-9]{4,8}[A-Z]{0,2})', res_json['title'])

        if res_code != None:
            print('res_code:', res_code.group(1))

            if str(code) == res_code.group(1):
                filename = getFilenameWithPath(code, date)
                appendToExcel(res_json['data'], filename)

            # 如果 res_json['data'] 的長度大於 1，則返回 True
            if len(res_json['data']) > 1:
                return True
    return False
def mainProcess(from_date):
    date = from_date
    global skip

    for code in stock_list:
        if code == skip_until_code and skip == True:
            skip = False

        while skip == False and date >= end_date:
            if date <= from_date:
                if filter_exist_file and os.path.isfile(getFilenameWithPath(code, date)):
                    print('file exist:', getFilenameWithPath(code, date))
                else:
                    if (getHistory(code, date) == False):
                        print('no data: ' + date.strftime("%Y%m%d") + ' code:', code)
                        break
                    sleepStrategy()

            date = date - datetime.timedelta(days=1)
            if date.day != 1:
                date = date.replace(day=1)
        date = from_date

def appendToExcel(data, filename):
    df = pd.DataFrame(columns = exportColumns, data = data)
    print('save:' + filename)

    if os.path.isfile(filename):
        df.to_csv(filename, mode = 'a', header = False, index = False)
    else:
        df.to_csv(filename, header = True, index = False)

def sleepStrategy():
    print('sleep', 0.2, 'sec to next request, request_count:', request_count)
    time.sleep(0.2)

initialize()
