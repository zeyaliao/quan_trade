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

#建立本地数据库连接(需要先开启数据库服务)
print('开始数据库连接')
db = pymysql.connect(host="localhost", user=name, password=password, charset="utf8")
cursor = db.cursor()

try:
    #删除数据库stockDataBase
    sqlSentence1 = "drop database %s"%dbname
    cursor.execute(sqlSentence1)
    print('删除数据库%s'%dbname)
except:
    print('删除数据库%s失败'%dbname)

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')