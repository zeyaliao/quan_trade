import baostock as bs
import pandas as pd
import os

path = os.path.split(os.path.realpath(__file__))[0] + "\\stock_list\\"

def get_hs300_sz50_zz500(path):
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    ############################### 获取沪深300成分股 ###############################
    rs = bs.query_hs300_stocks()
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
    rs = bs.query_sz50_stocks()
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
    rs = bs.query_zz500_stocks()
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