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
kdj_param1 = 9
kdj_param2 = 3
kdj_param3 = 3

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

        k = 100.0
        d = 100.0
        j = 0.0
        rsv = 0.0
        highest = -100000.0
        least = 100000.0
        a = 0

        for row in results:
            id = row[15]
            close = float(row[3])

            # if datetime.datetime.strftime(row[0], "%Y-%m-%d") == '2021-05-24':
            #     k = 83.13
            #     d = 75.53
            #     j = 98.32
            #     a = 1
            #     continue
            if id < kdj_param2:
            # elif a == 0:
                continue
            else:
                highest = -100000.0
                least = 100000.0
                for i in range(0, kdj_param1):
                    if id - i <= 0:
                        break
                    cursor.execute("select * from stock_%s"%code + " where id=%d"%(id - i))
                    tmpres = cursor.fetchall()
                    high = float(tmpres[0][4])
                    low = float(tmpres[0][5])
                    if high > highest:
                        highest = high
                    if low < least:
                        least = low
                    # print("%s" % tmpres[0][0] + ", high=%f" % high + ", low=%f" % low + ", close=%f" % float(tmpres[0][3]) + ", highest=%f" % highest + ", least=%f" % least)

                rsv = 100 * (close - least) / (highest - least)
                k = k * (kdj_param2 - 1) / kdj_param2 + rsv / kdj_param2
                d = d * (kdj_param3 - 1) / kdj_param3 + k / kdj_param3
                j = 3 * k - 2 * d

            # print("%s"%row[0] + ", k=%f"%k + ", d=%f"%d + ", j=%f"%j + ", close=%f"%close + ", highest=%f"%highest + ", least=%f"%least + ", rsv=%f"%rsv)

            # if id > 5:
            #     exit()

            sqlSentence = "UPDATE stock_%s" % code + " SET KDJ_K=%f" % k + ", KDJ_D=%f" % d + ", \
                                       KDJ_J=%f" % j + " where id=%d" % id
            cursor.execute(sqlSentence)

f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')