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
filepath = 'C:\\Users\\liaozeya\\Desktop\\Quantitative_trading\\1.python\\1\\data\\'#定义数据文件保存路径

# 建立数据库连接
try:
    db = pymysql.connect(host="localhost", user=name, password=password, database=dbname, charset="utf8")
    cursor = db.cursor()
except:
    print('使用数据库%s失败'%dbname)

#查询数据库并打印内容
print('查询数据库')
#code = input("请输入需要查询的股票代码：")
code = '600519'
cursor.execute('select * from stock_%s order by 日期 asc'%code)
#cursor.execute('select * from stock_%s where id=9'%code)
#cursor.execute("select * from stock_%s"%code + " where 日期='2021-06-11'")
results = cursor.fetchall()
for row in results:
    id = row[15]
    if id > 4700:
        print(row)
    # else:
    #     break

#关闭
cursor.close()
db.commit()
db.close()
print('关闭数据库')