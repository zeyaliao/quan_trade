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

#数据库名称和密码
name = 'lzy'
password = '19970623lzy'  #替换为自己的账户名和密码
dbname = 'stockDataBase'
stockpath = "stock.txt"
j_fast = 12
j_slow = 26
j_aver = 9

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
        buy = 0.0
        sale = 0.0
        pre_k = 0.0
        pre_d = 0.0
        pre_j = 0.0
        money = 10000.0
        min_j = 10000.0
        max_j = -10000.0
        buy_cnt = 0
        sale_cnt = 0

        # if code != "002142":
        #     print("code=%s"%code)
        #     continue

        start = 0

        for row in results:
            id = row[15]
            close = float(row[3])
            date = datetime.datetime.strftime(row[0], "%Y-%m-%d")  # 日期转换为字符串

            # print(row)
            if date == "2020-05-29":
                start = 1
                init_price = close
            if start == 0 or close <= 0:
                continue

            k = float(row[32])
            d = float(row[33])
            j = float(row[34])

            # 金叉买，死叉卖
            if pre_j < pre_d and j >= d and buy == 0 and sale == 0:
                buy = close
                # print("%s" % row[0] + ", buy=%f" % buy)
            if pre_j > pre_d and j <= d and buy != 0 and sale == 0:
                sale = close
                earn = (sale - buy) / buy
                money = money * (1 + earn)
                # print("%s" % row[0] + ", sale=%f" % sale + ", earn=%f"%(100 * earn) + r'%' + ", money=%f"%money)
                buy = 0.0
                sale = 0.0
                buy_cnt = 0
                sale_cnt = 0
                min_j = 10000.0
                max_j = -10000.0

            # j最低点买，最高点卖
            # if j < min_j and buy == 0:
            #     min_j = j
            # if j > max_j and sale == 0:
            #     max_j = j
            #
            # if j < 25 and j > min_j and buy == 0:
            #     buy_cnt = buy_cnt + 1
            # if j > 75 and j < max_j and sale == 0:
            #     sale_cnt = sale_cnt + 1
            # # 1 >
            # if buy_cnt >= 1 and buy == 0 and sale == 0:
            #     buy = close
            #     buy_cnt = 0
            #     sale_cnt = 0
            #     min_j = 10000.0
            #     max_j = -10000.0
            #     # print("%s" % row[0] + ", buy=%f" % buy + "j=%f"%j + ", min_j=%f"%min_j)
            # if sale_cnt >= 1 and buy != 0 and sale == 0:
            #     sale = close
            #     buy_cnt = 0
            #     sale_cnt = 0
            #     earn = (sale - buy) / buy
            #     money = money * (1 + earn)
            #     # print("%s" % row[0] + ", sale=%f" % sale + ", earn=%f"%(100 * earn) + r'%' + ", money=%f"%money)
            #     buy = 0.0
            #     sale = 0.0
            #     min_j = 10000.0
            #     max_j = -10000.0

            # print("%s"%row[0] + ", dif=%f"%dif + ", dea=%f"%dea + ", bar=%f"%j)

            # if id > 100:
            #     exit()
            pre_k = float(row[32])
            pre_d = float(row[33])
            pre_j = float(row[34])

        print("%s" % code + ": money=%f" % money + ", keep=%f"%(10000 * close / init_price))

f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')