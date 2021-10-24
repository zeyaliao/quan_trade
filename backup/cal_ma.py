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
        print('计算%s'%code)
        results = cursor.fetchall()
        sum5 = 0.0
        sum10 = 0.0
        sum20 = 0.0
        sum30 = 0.0
        sum60 = 0.0
        sum120 = 0.0
        closepre5 = 0.0
        closepre5 = 0.0
        closepre10 = 0.0
        closepre20 = 0.0
        closepre30 = 0.0
        closepre60 = 0.0
        closepre120 = 0.0
        for row in results:
            id = row[15]
            close = float(row[3])

            # 5日
            if id < 5:
                sum5 = sum5 + close
            else:
                if id == 5:
                    sum5 = sum5 + close
                else:
                    cursor.execute("select 收盘价 from stock_%s" % code + " where id=%d" % (id - 5))
                    results = cursor.fetchall()
                    closepre5 = float(results[0][0])
                    sum5 = sum5 + close - closepre5
                ma5 = sum5 / 5;
                cursor.execute("UPDATE stock_%s" % code + " SET 5日均价=%f" % ma5 + " where id=%d" % id)

            # 10日
            if id < 10:
                sum10 = sum10 + close
            else:
                if id == 10:
                    sum10 = sum10 + close
                else:
                    cursor.execute("select 收盘价 from stock_%s" % code + " where id=%d" % (id - 10))
                    results = cursor.fetchall()
                    closepre10 = float(results[0][0])
                    sum10 = sum10 + close - closepre10
                ma10 = sum10 / 10;
                cursor.execute("UPDATE stock_%s" % code + " SET 10日均价=%f" % ma10 + " where id=%d" % id)

                #if datetime.datetime.strftime(row[0], "%Y-%m-%d") == '2021-06-11':
                #if id > 4715:
                # if id <= 15:
                #     print("sum10=%f"%sum10 + ", ma10=%f"%ma10 + ", close=%f"%close + ", closepre10=%f"%closepre10 + ", id=%d"%id + ", date=%s"%row[0])
                # elif id > 15:
                #     break

            if id < 20:
                sum20 = sum20 + close
            else:
                if id == 20:
                    sum20 = sum20 + close
                else:
                    cursor.execute("select 收盘价 from stock_%s" % code + " where id=%d" % (id - 20))
                    results = cursor.fetchall()
                    closepre20 = float(results[0][0])
                    sum20 = sum20 + close - closepre20
                ma20 = sum20 / 20;
                cursor.execute("UPDATE stock_%s" % code + " SET 20日均价=%f" % ma20 + " where id=%d" % id)

            if id < 30:
                sum30 = sum30 + close
            else:
                if id == 30:
                    sum30 = sum30 + close
                else:
                    cursor.execute("select 收盘价 from stock_%s" % code + " where id=%d" % (id - 30))
                    results = cursor.fetchall()
                    closepre30 = float(results[0][0])
                    sum30 = sum30 + close - closepre30
                ma30 = sum30 / 30;
                cursor.execute("UPDATE stock_%s" % code + " SET 30日均价=%f" % ma30 + " where id=%d" % id)

            if id < 60:
                sum60 = sum60 + close
            else:
                if id == 60:
                    sum60 = sum60 + close
                else:
                    cursor.execute("select 收盘价 from stock_%s" % code + " where id=%d" % (id - 60))
                    results = cursor.fetchall()
                    closepre60 = float(results[0][0])
                    sum60 = sum60 + close - closepre60
                ma60 = sum60 / 60;
                cursor.execute("UPDATE stock_%s" % code + " SET 60日均价=%f" % ma60 + " where id=%d" % id)

            if id < 120:
                sum120 = sum120 + close
            else:
                if id == 120:
                    sum120 = sum120 + close
                else:
                    cursor.execute("select 收盘价 from stock_%s" % code + " where id=%d" % (id - 120))
                    results = cursor.fetchall()
                    closepre120 = float(results[0][0])
                    sum120 = sum120 + close - closepre120
                ma120 = sum120 / 120;
                cursor.execute("UPDATE stock_%s" % code + " SET 120日均价=%f" % ma120 + " where id=%d" % id)


f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')