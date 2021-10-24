import baostock as bs
import pandas as pd
import urllib
import time
import os
import datetime
from .stocktime import stocktime

class dl:

    ############################################# 成分股 ##################################################
    def get_hs300_sz50_zz500(path):
        # 登陆系统
        try:
            lg = bs.login()
        except:
            # 显示登陆返回信息
            print('login respond error_code:'+lg.error_code)
            print('login respond  error_msg:'+lg.error_msg)

        ############################### 获取沪深300成分股 ###############################
        try:
            rs = bs.query_hs300_stocks()
        except:
            print('query_hs300 error_code:'+rs.error_code)
            print('query_hs300  error_msg:'+rs.error_msg)

        # 打印结果集
        hs300_stocks = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            hs300_stocks.append(rs.get_row_data())
        result = pd.DataFrame(hs300_stocks, columns=rs.fields)
        # 结果集输出到csv文件
        result.to_csv(path + "hs300.csv", encoding="gbk", index=False)
        # print(result)
        print("保存沪深300成分股至：" + path + "hs300.csv")

        ############################### 获取上证50成分股 ###############################
        try:
            rs = bs.query_sz50_stocks()
        except:
            print('query_sz50 error_code:'+rs.error_code)
            print('query_sz50  error_msg:'+rs.error_msg)

        # 打印结果集
        sz50_stocks = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            sz50_stocks.append(rs.get_row_data())
        result = pd.DataFrame(sz50_stocks, columns=rs.fields)
        # 结果集输出到csv文件
        result.to_csv(path + "sz50.csv", encoding="gbk", index=False)
        # print(result)
        print("保存上证50成分股至：" + path + "sz50.csv")

        ############################### 获取中证500成分股 ###############################
        try:
            rs = bs.query_zz500_stocks()
        except:
            print('query_zz500 error_code:'+rs.error_code)
            print('query_zz500  error_msg:'+rs.error_msg)

        # 打印结果集
        zz500_stocks = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            zz500_stocks.append(rs.get_row_data())
        result = pd.DataFrame(zz500_stocks, columns=rs.fields)
        # 结果集输出到csv文件
        result.to_csv(path + "zz500.csv", encoding="gbk", index=False)
        # print(result)
        print("保存中证500成分股至：" + path + "zz500.csv")

        # 登出系统
        bs.logout()

    ############################################# 下载CSV ##################################################
    def get_stock_history(code : str,
                          path : str):
        if len(code) == 6:
            if code[0] == '6': # 沪市
                code = '0' + code
            elif code[0] == '0' or code[0] == '3': # 深市
                code = '1' + code
        elif len(code) == 9:
            code = code.replace("sh.", "0")
            code = code.replace("sz.", "1")
        else:
            print("ERROR: 股票代码格式异常！")

        csv_path = path + code[1:] + '.csv'

        # 防止重复更新
        cur_time = datetime.datetime.now()
        if os.path.exists(path+code[1:]+'.csv'):
            # 获取文件最近修改时间
            statinfo = os.stat(path+code[1:]+'.csv')
            last_modify_time = datetime.datetime.fromtimestamp(statinfo.st_mtime)
            # print("last_modify_time=%s"%last_modify_time)

            # 获取最近收盘时间
            last_trade_date = stocktime.get_last_trade_time(datetime.datetime.now())
            # print("last_trade_date=%s" % last_trade_date)
            if last_modify_time > last_trade_date:
                # print(code[1:]+".csv无需更新")
                return csv_path

        curdate = time.strftime("%Y%m%d", time.localtime())
        # curdate = "20210616"
        url = 'http://quotes.money.163.com/service/chddata.html?code=' + code + \
              '&end=' + curdate + '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
        print(url)
        print(path + code[1:] + '.csv')
        for i in range(10):
            try:
                urllib.request.urlretrieve(url, csv_path)
            except:
                print("ERROR: 下载失败！")
                time.sleep(5)
                continue
            break

        return csv_path


    # get_stock_history("sz.002142", "C:\\Users\\liaozeya\\Desktop\\Quantitative_trading\\1.python\\1\\init\\data\\hist\\")