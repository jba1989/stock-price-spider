from http.client import USE_PROXY
import requests
import pandas as pd
import os
import time
import random
from bs4 import BeautifulSoup

http_proxy_list = []
https_proxy_list = []
stock_list = []
exportColumns = ['date', 'capacity', 'turnover', 'open', 'high', 'low', 'close', 'change', 'trascation']
main_path = './history/'
request_count = 0
error_count = 0
begin_year = 2010
begin_month = 1
end_year = 2022
end_month = 9
filter_exist_file = True
use_proxy = True

def resetProxyList():
    global http_proxy_list
    global https_proxy_list

    http_proxy_list = []
    https_proxy_list = []

def getProxy():
    if use_proxy:
        resetProxyList()

        url = 'http://www.us-proxy.org/'
        response = requests.get(url)
        parseProxyHtml(response.text)

def parseProxyHtml(html):
    global http_proxy_list
    global https_proxy_list

    soup = BeautifulSoup(html, 'html.parser')
    trs = soup.select('table.table-striped tbody tr')

    for tr in trs:
        tds = tr.find_all('td')

        if len(tds) == 8:
            if tds[1].text == '80':
                http_proxy_list.append(tds[0].text + ':' + tds[1].text)

def getRandomProxy():
    global http_proxy_list
    global https_proxy_list

    if random.choice([1, 2]) == 2 and len(https_proxy_list) > 0:
        proxy = {'https': 'https://' + random.choice(https_proxy_list)}
    else:
        proxy = {'http': 'http://' + random.choice(http_proxy_list)}

    return proxy

def getStockList():
    data = pd.read_csv('./stock-list.csv')
    global stock_list
    stock_list = list(data['stock_id'][:])

def checkDir(path):
    if (not os.path.exists(path)):
        os.mkdir(path, 755)
        os.system('sudo chmod 755 ' + path)

def initialize(from_id = 1101, from_year = begin_year, from_month = begin_month):
    getProxy()
    checkDir(main_path)
    getStockList()
    mainProcess(from_id, from_year, from_month)

def curl(date, stock_id):
    url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=' + date + '&stockNo=' + str(stock_id)
    proxy = getRandomProxy()

    print('url:', url, 'proxy:', proxy)

    if use_proxy:
        return requests.get(url, proxies = proxy, timeout = 3)
    else:
        return requests.get(url, timeout = 3)

def uniformDate(year, month):
    if len(str(month)) == 1:
        date = str(year) + '0' + str(month) + '01'
    else:
        date = str(year) + str(month) + '01'

    return date

def getFilenameWithPath(stock_id, year, month):
    return main_path + str(stock_id) + '/' + str(year) + '_' + str(month) + '.csv'

def getHistory(stock_id, year, month):
    global request_count
    global error_count

    date = uniformDate(year, month)

    try:
        request_count += 1
        res = curl(date, stock_id)
        res_json = res.json()

        if res_json['stat'] == 'OK':
            filename = getFilenameWithPath(stock_id, year, month)
            appendToExcel(res_json['data'], filename)
        elif (res_json['stat'] == '很抱歉，沒有符合條件的資料!'):
            print('no data')
        else:
            raise Exception('request error')
    except:
        if error_count < 2:
            print('request error, error_count:', error_count)

            error_count += 1
            mainProcess(stock_id, year, month)
        else:
            print('request error, sleep 120s, error_count:', error_count)
            time.sleep(120)

            error_count = 0
            index = stock_list.index(stock_id)
            mainProcess(stock_list[index + 1], year, month)

def mainProcess(from_id, from_year, from_month):
    global filterExistFile

    year = from_year
    month = from_month

    while year < end_year or (year == end_year and month <= end_month):
        for stock_id in stock_list:
            if (stock_id >= from_id or year > from_year) and stock_id < 9999:
                # 檔案不存在才爬
                if filter_exist_file and not os.path.isfile(getFilenameWithPath(stock_id, year, month)):
                    # 建立資料夾
                    save_path = main_path + str(stock_id)
                    checkDir(save_path)

                    # 爬資料
                    getHistory(stock_id, year, month)
                    sleepStrategy()

        if month == 12:
            month = begin_month
            year += 1
        else:
            month += 1

def appendToExcel(data, filename):
    df = pd.DataFrame(columns = exportColumns, data = data)

    if os.path.isfile(filename):
        df.to_csv(filename, mode = 'a', header = False, index = False)
    else:
        df.to_csv(filename, header = True, index = False)

def sleepStrategy():
    sleep_time = random.randrange(2, 4)

    if request_count / 10 > 0:
        if request_count % 60 == 0:
            getProxy()
        elif request_count % 30 == 0:
            sleep_time += 20
        elif request_count % 10 == 0:
            sleep_time += 10

    print('sleep', sleep_time, 'sec to next request, request_count:', request_count)
    time.sleep(sleep_time)

initialize()
