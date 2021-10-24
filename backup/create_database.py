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

try:
    #创建数据库stockDataBase
    sqlSentence1 = "create database %s"%dbname
    cursor.execute(sqlSentence1)
    print('创建数据库%s'%dbname)
except:
    print('创建数据库%s失败'%dbname)

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
        # 创建数据表，如果数据表已经存在，会跳过继续执行下面的步骤
        # 0-2
        # 3-8
        # 9-14
        # 15
        # 16-21
        # 22-27
        # 28
        # 29-31
        # 32-34
        sqlSentence3 = "create table stock_%s" % code + "(日期 date unique, 股票代码 VARCHAR(10),     名称 VARCHAR(10),\
                                       收盘价 float,    最高价    float, 最低价 float, 开盘价 float, 前收盘 float, 涨跌额    float, \
                                       涨跌幅 float, 换手率 float, 成交量 bigint, 成交金额 bigint, 总市值 bigint, 流通市值 bigint, \
                                       id int unique, \
                                       5日均价 float, 10日均价 float, 20日均价 float, 30日均价 float, 60日均价 float, 120日均价 float, \
                                       5周均价 float, 10周均价 float, 20周均价 float, 30周均价 float, 60周均价 float, 120周均价 float, \
                                       总股本 bigint, \
                                       MACD_DIF float, MACD_DEA float, MACD float, \
                                       KDJ_K float, KDJ_D float, KDJ_J float)"
        try:
            cursor.execute(sqlSentence3)
            print('创建数据表stock_%s' % code)
        except:
            print('创建数据表stock_%s失败' % code)
            continue

f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')