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
import os
import configparser

# 读取静态配置
path = os.path.split(os.path.realpath(__file__))[0] + "\\"
csv_path = path + "stock_list\\"
cf = configparser.ConfigParser()
cf.read(path + "config.ini")

#数据库名称和密码
name = cf.get("db", "user")
password = cf.get("db", "password")
dbname = cf.get("db", "dbname")
csv_path = path + cf.get("csv", "dir_name") + "\\"

#建立本地数据库连接(需要先开启数据库服务)
try:
    print('开始数据库连接')
    db = pymysql.connect(host="localhost", user=name, password=password, charset="utf8")
    cursor = db.cursor()
except:
    print('数据库连接失败')

#选择使用当前数据库
try:
    cursor.execute("use %s;"%dbname)
    print("开始使用：%s"%dbname)
except:
    print('数据库%s连接失败'%dbname)
    exit(0)
##################################################开始干活###########################################################
#获取本地文件列表
fileList = os.listdir(csv_path)
#依次对每个数据文件进行存储
for fileName in fileList:
    data = pd.read_csv(csv_path + fileName, encoding="gbk")
    length = len(data)
    for i in range(0, length):
        record = tuple(data.loc[i])
        code = record[1]
        name = record[2]
        print(code + name)
        exit(0)
