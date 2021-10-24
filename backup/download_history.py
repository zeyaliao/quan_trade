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

path = os.path.split(os.path.realpath(__file__))[0]
    
##################################################开始干活###########################################################
filepath = path + "\\data\\" #定义数据文件保存路径
path1 = "hs300.csv"
path2 = "sz50.csv"
path3 = "zz500.csv"
path4 = "user_focus_stock.txt"

curdate = time.strftime("%Y%m%d", time.localtime())

print('开始下载股票历史数据')
print("当前日期：%s"%curdate)
#抓取数据并保存到本地csv文件
with open(stockpath, "r") as f:
    for code in f.readlines():
        code = code.strip('\n')  # 去掉列表中每一个元素的换行符
        print('下载：%s'%code)
        if code[0] == '6': # 沪市
            codeprefix = '0'
        elif code[0] == '0' or code[0] == '3': # 深市
            codeprefix = '1'
        else:
            break
        url = 'http://quotes.money.163.com/service/chddata.html?code=' + codeprefix + code + \
              '&end=' + curdate + '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
        urllib.request.urlretrieve(url, filepath+code+'.csv')
        time.sleep(random.randrange(0,2))

print('全部下载完成')
f.close()