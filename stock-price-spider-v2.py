# -*- coding: utf-8 -*-
"""
Created on Sun April 28 20:48:00 2024
Description: 廣度優先, 時間順序

@author: Lonelygod
"""

import requests
import pandas as pd
import os
import time
import re
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
begin_year = 2024
begin_month = 4
end_year = 2024
end_month = 4
filter_exist_file = False

def getStockList():
    data = pd.read_csv('./stock-list.csv', dtype = str)
    global stock_list
    stock_list = list(data['code'][:])

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

def initialize(from_year = begin_year, from_month = begin_month):
    getStockList()
    checkDir(main_path)
    makeAllDir()
    mainProcess(from_year, from_month)

def curl(date, code):
    if api_url is None:
        raise Exception('api_url is not defined')

    url = api_url + '?date=' + date + '&code=' + str(code)
    print('url:', url)

    return requests.get(url, timeout = 3)

def uniformDate(year, month):
    if len(str(month)) == 1:
        date = str(year) + '0' + str(month) + '01'
    else:
        date = str(year) + str(month) + '01'

    return date

def getFilenameWithPath(code, year, month):
    return main_path + str(code) + '/' + str(year) + '_' + str(month) + '.csv'

def getHistory(code, year, month):
    global request_count
    global error_count

    date = uniformDate(year, month)

    try:
        request_count += 1
        res = curl(date, code)
        res_json = res.json()
        filename = getFilenameWithPath(code, year, month)

        if res_json['stat'] == 'OK' and res_json['date'] == '{:04}{:02}01'.format(year, month):
            res_code = re.search(r'([0-9]{4,8}[A-Z]{0,2})', res_json['title'])

            if res_code != None:
                print('res_code:', res_code.group(1))

                if str(code) == res_code.group(1):
                    filename = getFilenameWithPath(code, year, month)
                    appendToExcel(res_json['data'], filename)

                error_count = 0
            else:
                raise Exception('request error')
        elif (res_json['stat'] == '很抱歉，沒有符合條件的資料!'):
            appendToExcel([], filename)
            print('no data')
        else:
            raise Exception('request error')
    except:
        sleepStrategy()

        if error_count < 2:
            print('request error, error_count:', error_count)

            error_count += 1
            getHistory(code, year, month)
        else:
            print('request error, error_count:', error_count, 'goto next stock')

def mainProcess(from_year, from_month):
    year = from_year
    month = from_month

    while year < end_year or (year == end_year and month <= end_month):
        for code in stock_list:
            if (year >= from_year):
                # 檔案不存在才爬
                if filter_exist_file and os.path.isfile(getFilenameWithPath(code, year, month)):
                    print('file exist:', getFilenameWithPath(code, year, month))
                else:
                    getHistory(code, year, month)
                    sleepStrategy()

        if month >= 12:
            month = 1
            year += 1
        else:
            month += 1

def appendToExcel(data, filename):
    df = pd.DataFrame(columns = exportColumns, data = data)
    print('save:' + filename)

    if os.path.isfile(filename):
        df.to_csv(filename, mode = 'a', header = False, index = False)
    else:
        df.to_csv(filename, header = True, index = False)

def sleepStrategy():
    print('sleep', 0.3, 'sec to next request, request_count:', request_count)
    time.sleep(0.3)

initialize()
