#导入需要使用到的模块
import sys
import urllib
import re
import pandas as pd
import pymysql
import os
import random
import time
import datetime
from matplotlib import pyplot as plt
# import matplotlib.finance as mpf
from mpl_finance import candlestick_ohlc
from matplotlib.pylab import date2num
from pandas.plotting import register_matplotlib_converters

# %matplotlib inline

#数据库名称和密码
name = 'lzy'
password = '19970623lzy'  #替换为自己的账户名和密码
dbname = 'stockDataBase'
stockpath = "stock.txt"
macd_fast = 12
macd_slow = 26
macd_aver = 9

#建立本地数据库连接(需要先开启数据库服务)
print('开始数据库连接')
db = pymysql.connect(host="localhost", user=name, password=password, charset="utf8")
cursor = db.cursor()

#选择使用当前数据库
sqlSentence2 = "use %s;"%dbname
try:
    cursor.execute(sqlSentence2)
    print(sqlSentence2)
except:
    print('数据库%s连接失败'%dbname)
    exit(0)

with open(stockpath, "r") as f:
    for code in f.readlines():
        code = code.strip('\n')  # 去掉列表中每一个元素的换行符
        cursor.execute('select * from stock_%s order by id asc' % code)
        results = cursor.fetchall()
        start = 0
        quotes = []

        for row in results:
            id = row[15]
            close = float(row[3])

            # print(row)
            if id <= 3700 or close <= 0:
                continue

            if start == 0:
                start = 1

            date = datetime.datetime.strftime(row[0],'%Y-%m-%d')
            sdate_plt = date2num(datetime.datetime.strptime(date, '%Y-%m-%d'))
            #sdate_plt = date2num(row[0])

            sopen = float(row[6])
            shigh = float(row[4])
            slow = float(row[5])
            sclose = float(row[3])
            datas = (sdate_plt, sopen, shigh, slow, sclose)  # 按照 candlestick_ohlc 要求的数据结构准备数据
            quotes.append(datas)

        fig, ax = plt.subplots(facecolor=(0, 0.3, 0.5), figsize=(12, 8))
        fig.subplots_adjust(bottom=0.1)
        ax.xaxis_date()
        plt.xticks(rotation=45)  # 日期显示的旋转角度
        plt.title(code)
        plt.xlabel('time')
        plt.ylabel('price')
        candlestick_ohlc(ax, quotes, width=0.7, colorup='r', colordown='green')  # 上涨为红色K线，下跌为绿色，K线宽度为0.7
        plt.grid(True)
        # plt.grid(False)
        # ax.xaxis_date()
        ax.autoscale_view()
        plt.setp(plt.gca().get_xticklabels(), rotation=30)
        plt.show()


f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')