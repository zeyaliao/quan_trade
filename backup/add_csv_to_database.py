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

##########################将股票数据存入数据库###########################

#数据库名称和密码
name = 'lzy'
password = '19970623lzy'  #替换为自己的账户名和密码
dbname = 'stockDataBase'
filepath = 'C:\\Users\\liaozeya\\Desktop\\Quantitative_trading\\1.python\\1\\data\\'#定义数据文件保存路径

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

#获取本地文件列表
fileList = os.listdir(filepath)
#依次对每个数据文件进行存储
for fileName in fileList:
    data = pd.read_csv(filepath+fileName, encoding="gbk")

    #迭代读取表中每行数据，依次存储（整表存储还没尝试过）
    print('正在存储stock_%s'% fileName[0:6])

    try:
        cursor.execute("select MAX(日期) from stock_%s"% fileName[0:6])
        results = cursor.fetchall()
        for row in results:
            lastdate = row[0]
            lastdate = datetime.datetime.strftime(lastdate, "%Y-%m-%d") # 日期转换为字符串
            print('数据库最新条目日期为%s' % lastdate)
            break
    except:
        lastdate = '0'
        print("查询最新日期失败")

    length = len(data)
    for i in range(0, length):
        record = tuple(data.loc[i])
        tmpdate = record[0]
        if tmpdate == lastdate:
            break

        #插入数据语句
        try:
            sqlSentence4 = "insert ignore into stock_%s" % fileName[0:6] + "(日期, 股票代码, 名称, 收盘价, 最高价, 最低价, 开盘价, 前收盘, 涨跌额, 涨跌幅, 换手率, \
            成交量, 成交金额, 总市值, 流通市值) values ('%s',%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % record
            #获取的表中数据很乱，包含缺失值、Nnone、none等，插入数据库需要处理成空值
            sqlSentence4 = sqlSentence4.replace('nan','null').replace('None','null').replace('none','null') 
            cursor.execute(sqlSentence4)
        except:
            #如果以上插入过程出错，跳过这条数据记录，继续往下进行
            print('插入数据出错')
            continue

    print('存储stock_%s成功' % fileName[0:6])

    cursor.execute('select * from stock_%s order by 日期 asc' % fileName[0:6])
    results = cursor.fetchall()
    id = 0
    for row in results:
        date = row[0]
        close = float(row[3])
        gmv = float(row[13])

        # 停牌
        if close == 0:
            cursor.execute("delete from stock_%s" % fileName[0:6] + " where 日期='%s'" % date)
            continue

        id = id + 1
        sqlSentence5 = "UPDATE stock_%s" % fileName[0:6] + " SET id=%d"%id + " where 日期='%s'" % date
        cursor.execute(sqlSentence5)

        num = int(gmv / close)
        sqlSentence6 = "UPDATE stock_%s" % fileName[0:6] + " SET 总股本=%d" % num + " where 日期='%s'" % date
        cursor.execute(sqlSentence6)
        # if datetime.datetime.strftime(row[0], "%Y-%m-%d") == '2021-04-14':
        #     print("2021-04-14 gmv：%f" % gmv + ", close=%f" % close + ", num=%d" % num)

    print('stock_%s id、总股本更新成功' % fileName[0:6])

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')