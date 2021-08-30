import requests
import pandas as pd
import os
import time
import random

stock_list = [1,2,3]
exportColumns = ['date', 'capacity', 'turnover', 'open', 'high', 'low', 'close', 'change', 'trascation']
main_path = './history/'
request_count = 0
error_count = 0
begin_year = 2013
begin_month = 1
end_year = 2021
end_month = 4


def getStockList():
    data = pd.read_csv('./stock-list.csv')
    global stock_list
    stock_list = list(data['stock_id'][:])

def checkDir(path):
    if (not os.path.exists(path)):
        os.mkdir(path, 755)

def initialize(from_id = 1101, from_year = begin_year, from_month = begin_month):
    checkDir(main_path)
    getStockList()
    getHistory(from_id, from_year, from_month)

def curl(date, stock_id):
    url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=' + date + '&stockNo=' + str(stock_id)
    print('url:', url)

    return requests.get(url)

def uniformDate(year, month):
    if len(str(month)) == 1:
        date = str(year) + '0' + str(month) + '01'
    else:
        date = str(year) + str(month) + '01'

    return date

def getHistory(from_id, from_year, from_month):
    global request_count
    global error_count
    year = from_year
    month = from_month

    for stock_id in stock_list:
        if stock_id >= from_id and stock_id < 9999:
            while year < end_year or (year == end_year and month <= end_month):
                date = uniformDate(year, month)

                try:
                    request_count += 1
                    res = curl(date, stock_id)
                    res_json = res.json()

                    if (res_json['stat'] != 'OK'):
                        sleepStrategy()
                        break
                except:
                    print('error, sleep 1hr, error_count:', error_count)
                    time.sleep(3600)

                    if error_count == 0:
                        error_count += 1
                        getHistory(stock_id, year, month)
                    else:
                        error_count = 0
                        index = stock_list.index(stock_id)
                        initialize(stock_list[index + 1])

                save_path = main_path + str(stock_id) + '/'
                checkDir(save_path)
                filename = save_path + str(year) + '.csv'
                appendToExcel(res_json['data'], filename)

                print('write stock:', stock_id, 'date:', date)
                sleepStrategy()


                if year == end_year and month == end_month:
                    if stock_id == stock_list[-1]:
                        break
                    else:
                        year = begin_year
                        month = begin_month
                        break
                elif month == 12:
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
    sleep_time = random.randrange(2, 6)

    if request_count / 10 > 0:
        if request_count % 30 == 0:
            sleep_time += 120
        elif request_count % 10 == 0:
            sleep_time += 60

    print('sleep', sleep_time, 'sec to next request, request_count:', request_count)
    time.sleep(sleep_time)

initialize()
