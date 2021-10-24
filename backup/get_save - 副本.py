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
    
##################################################开始干活###########################################################

print('开始下载股票历史数据')
filepath = 'C:\\Users\\liaozeya\\Desktop\\Quantitative_trading\\1.python\\1\\data\\'#定义数据文件保存路径
stockpath = "stock.txt"
curdate = time.strftime("%Y%m%d", time.localtime())
print("当前日期：%s"%curdate)

#抓取数据并保存到本地csv文件
with open(stockpath, "r") as f:
    for code in f.readlines():
        code = code.strip('\n')  # 去掉列表中每一个元素的换行符
        print(code)
        if code[0] == '6':
            url = 'http://quotes.money.163.com/service/chddata.html?code=0'+code+\
                '&end='+curdate+'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
        elif code[0] == '0' or code[0] == '3':
            url = 'http://quotes.money.163.com/service/chddata.html?code=1' + code + \
                  '&end='+curdate+'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
        urllib.request.urlretrieve(url, filepath+code+'.csv')
        time.sleep(random.randrange(0,2))

##########################将股票数据存入数据库###########################

#数据库名称和密码
name = 'lzy'
password = '19970623lzy'  #替换为自己的账户名和密码
dbname = 'stockDataBase'

#建立本地数据库连接(需要先开启数据库服务)
db = pymysql.connect(host="localhost", user=name, password=password, charset="utf8")
cursor = db.cursor()

#选择使用当前数据库
sqlSentence2 = "use stockDataBase;"
cursor.execute(sqlSentence2)
print(sqlSentence2)

#获取本地文件列表
fileList = os.listdir(filepath)
#依次对每个数据文件进行存储
for fileName in fileList:
    data = pd.read_csv(filepath+fileName, encoding="gbk")
   #创建数据表，如果数据表已经存在，会跳过继续执行下面的步骤
    sqlSentence3 = "create table stock_%s" % fileName[0:6] + "(id int unique, \
                       日期 date unique, 股票代码 VARCHAR(10),     名称 VARCHAR(10),\
                       收盘价 float,    最高价    float, 最低价 float, 开盘价 float, 前收盘 float, 涨跌额    float, \
                       涨跌幅 float, 换手率 float, 成交量 bigint, 成交金额 bigint, 总市值 bigint, 流通市值 bigint, \
                       5日均价 float, 10日均价 float, 20日均价 float, 30日均价 float, 60日均价 float, 120日均价 float, \
                       5周均价 float, 10周均价 float, 20周均价 float, 30周均价 float, 60周均价 float, 120周均价 float)"
    try:
        cursor.execute(sqlSentence3)
        print('创建数据表stock_%s' % fileName[0:6])
    except:
        print('已存在数据表stock_%s' % fileName[0:6])

    #迭代读取表中每行数据，依次存储（整表存储还没尝试过）
    print('正在存储stock_%s'% fileName[0:6])

    try:
        cursor.execute("select MAX(日期) from stock_%s"% fileName[0:6])
        results = cursor.fetchall()
        for row in results:
            lastdate = row[0]
            lastdate = datetime.datetime.strftime(lastdate, "%Y-%m-%d")
            print('最新日期为%s' % lastdate)
            break
    except:
        lastdate = '0'
        print("查询最新日期失败")

    length = len(data)
    for i in range(0, length):
        record = tuple(data.loc[i])
        tmpdate = record[0]
        print("lastdate=%s"%lastdate+" tmpdate=%s"%tmpdate)
        if tmpdate == lastdate:
            print('日期已存在')
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

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('关闭数据库')

###########################查询刚才操作的成果##################################
exit(1);
#重新建立数据库连接
db = pymysql.connect(host="localhost", user=name, password=password, database="stockDataBase", charset="utf8")
cursor = db.cursor()
#查询数据库并打印内容
print('查询数据库')
cursor.execute('select * from stock_301005 order by 日期 asc')
results = cursor.fetchall()
for row in results:
    print(row)
#关闭
cursor.close()
db.commit()
db.close()
print('关闭数据库')