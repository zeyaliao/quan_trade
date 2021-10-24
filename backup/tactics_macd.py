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
        buy = 0.0
        sale = 0.0
        predif = 0.0
        predea = 0.0
        premacd = 0.0
        money = 10000.0
        min_macd = 10000.0
        max_macd = -10000.0
        buy_cnt = 0
        sale_cnt = 0

        # if code != "002142":
        #     print("code=%s"%code)
        #     continue

        start = 0
        earn_sum = 0.0
        earn_num = 0

        for row in results:
            id = row[15]
            close = float(row[3])
            date = datetime.datetime.strftime(row[0], "%Y-%m-%d") # 日期转换为字符串

            # print(row)
            # if date == "2020-05-29":
            if date == "2017-01-06":
                start = 1
                init_price = close
            if start == 0 or close <= 0:
                continue

            dif = float(row[29])
            dea = float(row[30])
            macd = float(row[31])

            # 金叉买，死叉卖
            # if predif < predea and dif >= dea and buy == 0 and sale == 0:
            #     buy = close
            #     # print("%s" % row[0] + ", buy=%f" % buy)
            if predif > predea and dif <= dea and buy != 0 and sale == 0:
                sale = close
                earn = (sale - buy) / buy
                earn_sum = earn_sum + earn
                earn_num = earn_num + 1
                money = money * (1 + earn)
                # print("%s" % row[0] + ", sale=%f" % sale + ", earn=%f"%(100 * earn) + r'%' + ", money=%f"%money)
                buy = 0.0
                sale = 0.0
                buy_cnt = 0
                sale_cnt = 0

            # macd最低点买，最高点卖
            if dif < 0 and dea < 0 and macd < 0 and macd < min_macd and buy == 0:
                min_macd = macd
            if dif > 0 and dea > 0 and macd > 0 and macd > max_macd and sale == 0:
                max_macd = macd

            if dif < 0 and dea < 0 and macd < 0 and macd > min_macd and buy == 0:
                buy_cnt = buy_cnt + 1
            if dif > 0 and dea > 0 and macd > 0 and macd < max_macd and sale == 0:
                sale_cnt = sale_cnt + 1
            # 4 > 5 > 6 > 1 > 2 > 3 > 7
            if dif < 0 and dea < 0 and buy_cnt >= 4 and buy == 0 and sale == 0:
                buy = close
                buy_cnt = 0
                sale_cnt = 0
                # print("%s" % row[0] + ", buy=%f" % buy)
            # if dif > 0 and dea > 0 and sale_cnt >= 1 and buy != 0 and sale == 0:
            #     sale = close
            #     buy_cnt = 0
            #     sale_cnt = 0
            #     earn = (sale - buy) / buy
            #     earn_sum = earn_sum + earn
            #     earn_num = earn_num + 1
            #     money = money * (1 + earn)
            #     # print("%s" % row[0] + ", sale=%f" % sale + ", earn=%f"%(100 * earn) + r'%' + ", money=%f"%money)
            #     buy = 0.0
            #     sale = 0.0

            # print("%s"%row[0] + ", dif=%f"%dif + ", dea=%f"%dea + ", bar=%f"%macd)

            # if id > 100:
            #     exit()
            predif = float(row[29])
            predea = float(row[30])
            premacd = float(row[31])

        print("%s" % code + ": money=%f" % money + ", earn_aver=%f"%(earn_sum / earn_num) + "; keep=%f"%(10000 * close / init_price))

f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')