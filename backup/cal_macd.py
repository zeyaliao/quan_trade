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
        print('计算%s' % code)

        dif = 0.0
        dea = 0.0
        bar = 0.0
        macd = 0.0

        for row in results:
            id = row[15]
            close = float(row[3])

            if id == 1:
                preclose = close
                continue
            elif id == 2:
                ema_fast = preclose * (macd_fast - 1) / (macd_fast + 1) + close * 2 / (macd_fast + 1)
                ema_slow = preclose * (macd_slow - 1) / (macd_slow + 1) + close * 2 / (macd_slow + 1)
                dif = ema_fast - ema_slow
                dea = dif * 2 / (macd_aver + 1)
                bar = 2 * (dif - dea)
            else:
                ema_fast = ema_fast * (macd_fast - 1) / (macd_fast + 1) + close * 2 / (macd_fast + 1)
                ema_slow = ema_slow * (macd_slow - 1) / (macd_slow + 1) + close * 2 / (macd_slow + 1)
                dif = ema_fast - ema_slow
                dea = dea * (macd_aver - 1) / (macd_aver + 1) + dif * 2 / (macd_aver + 1)
                bar = 2 * (dif - dea)

            print("%s"%row[0] + ", dif=%f"%dif + ", dea=%f"%dea + ", bar=%f"%bar)

            # if id > 20:
            #     exit()

            sqlSentence = "UPDATE stock_%s" % code + " SET MACD_DIF=%f" % dif + ", MACD_DEA=%f" % dea + ", \
                                       MACD=%f" % bar + " where id=%d" % id
            cursor.execute(sqlSentence)

f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')