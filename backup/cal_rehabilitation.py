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
        cursor.execute('select * from stock_%s order by id desc' % code)
        results = cursor.fetchall()
        preclose = 0.0
        prenum = 0
        multi = 1.0
        bonus = 0.0
        tmpnum = 0
        ttl = 0
        latestnum = 0

        for row in results:
            id = row[15]
            close = float(row[3])
            num = float(row[28])
            date = row[0]

            if close == 0:
                continue

            if latestnum == 0:
                latestnum = num

            #print("prenum=%f" % prenum + ", num=%f" % num + ", preclose=%f" % preclose + ", close=%f" % close)
            # 送转（非增发）
            if ttl == 0:
                tmpnum = 0
            elif ttl > 0:
                ttl = ttl - 1

            if tmpnum != 0 and tmpnum - num > 10 and prenum - num <= 10 and preclose != close:
                prenum = tmpnum

            close = float(row[3]) * multi - bonus
            if prenum != 0 and prenum - num > 10:
                if preclose != close:
                    print("%s送转: "%date + "10转%d"%(10 * prenum / num))
                    multi = multi * float(num / prenum)
                    tmpnum = 0
                    #print("prenum=%f" % prenum + ", num=%f" % num + ", preclose=%f" % preclose + ", close=%f" % close)
                elif tmpnum == 0:
                    print("%s定增或送转未定: "%date + "%d股"%(prenum - num))
                    tmpnum = prenum
                    ttl = 3

            # 分红
            close = float(row[3]) * multi - bonus
            if preclose != 0 and preclose < close:
                bonus = bonus + close - preclose
                print("%s分红: " % date + "10派%f" % (10 * (close - preclose) * latestnum / num))
                #print("close=%f" % close + ", preclose=%f" % preclose + ", bonus=%f" % bonus)
                #print("prenum=%f" % prenum + ", num=%f" % num + ", preclose=%f" % preclose + ", close=%f" % close)

            close = (float(row[3]) * multi) - bonus
            highest = (float(row[4]) * multi) - bonus
            least = (float(row[5]) * multi) - bonus
            open = (float(row[6]) * multi) - bonus
            preclose = (float(row[7]) * multi) - bonus
            diff = (float(row[8]) * multi)
            prenum = float(row[28])

            # if datetime.datetime.strftime(row[0], "%Y-%m-%d") == '2001-12-31':
            #     print("最高：%f"%highest + ", open=%f"%open + ", multi=%f"%multi + ", bonus=%f"%bonus)
            #     print("close=%f" % float(row[3]) + ", preclose=%f" % float(row[7]) + ", num=%f" % float(row[28]))
            #     break
            sqlSentence5 = "UPDATE stock_%s" % code + " SET 收盘价=%f" % close + ", 最高价=%f"%highest + ", \
                           最低价=%f"%least + ", 开盘价=%f"%open + ", 前收盘=%f"%preclose + ", 涨跌额=%f"%diff + "\
                           where id=%d" % id
            cursor.execute(sqlSentence5)

f.close()

#关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()
print('断开数据库连接')