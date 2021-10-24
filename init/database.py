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
from .config import conf
from .download import dl
from .stocktime import stocktime

class mysql:

    ############################################# 连接 ##################################################
    def connect(cf : conf):
        # 建立本地数据库连接(需要先开启数据库服务)
        try:
            print('开始数据库连接')
            db = pymysql.connect(host="localhost", user=cf.db_user, password=cf.db_pwd, charset="utf8")
            cursor = db.cursor()
        except:
            print('数据库连接失败')
            cursor.close()
            exit(0)

        try:
            # 尝试创建数据库stockDataBase
            cursor.execute("create database %s"%cf.db_name)
            print('创建数据库%s'%cf.db_name)
        except:
            print('数据库%s已存在'%cf.db_name)

        #选择使用当前数据库
        try:
            cursor.execute("use %s"%cf.db_name)
            print("开始使用：%s"%cf.db_name)
        except:
            print('ERROR: 数据库%s无法使用'%cf.db_name)
            cursor.close()
            exit(0)

        cursor.close()

        return db

    ############################################# 断开 ##################################################
    def disconnect(db : pymysql.connections):
        db.commit()
        db.close()
        print('断开数据库连接')

    ############################################# 创建表 ##################################################
    def create_stock_table(db : pymysql.connections,
                           table : str):
        cursor = db.cursor()
        try:
            cursor.execute("create table %s" % table + "(日期 date unique, 股票代码 VARCHAR(10),     名称 VARCHAR(10),\
                                               收盘价 float,    最高价    float, 最低价 float, 开盘价 float, 前收盘 float, 涨跌额    float, \
                                               涨跌幅 float, 换手率 float, 成交量 bigint, 成交金额 bigint, 总市值 bigint, 流通市值 bigint, \
                                               id int unique, 总股本 bigint, 均价 float, 分红 float, 送转 float)")
            print('创建数据表%s' % table)
        except:
            print('数据表%s已存在' % table)

        cursor.close()

    ############################################# 导入CSA到DB ##################################################
    def update_csv_to_table(db : pymysql.connections,
                            table : str,
                            csv_path : str):
        cursor = db.cursor()

        # 获取表中最新日期
        try:
            cursor.execute("select MAX(日期) from %s" % table)
            max_date = cursor.fetchall()[0][0]
        except:
            max_date = None

        # 读取CSV数据
        data = pd.read_csv(csv_path, encoding="gbk")
        length = len(data)
        for i in range(0, length):
            record = tuple(data.loc[i])
            tmpdate = datetime.datetime.strptime(record[0], "%Y-%m-%d").date()
            if max_date != None and tmpdate == max_date:
                # print('%s无需更新' % tmpdate)
                break

            # 插入数据语句
            try:
                sql = "insert ignore into %s" % table + "(日期, 股票代码, 名称, 收盘价, 最高价, 最低价, 开盘价, 前收盘, 涨跌额, 涨跌幅, 换手率, " + \
                                "成交量, 成交金额, 总市值, 流通市值) values ('%s',%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % record
                # 获取的表中数据很乱，包含缺失值、Nnone、none等，插入数据库需要处理成空值
                sql = sql.replace('nan', '0').replace('None', '0').replace('none', '0')
                cursor.execute(sql)
                # print('插入%s' % sql)
            except:
                # 如果以上插入过程出错，跳过这条数据记录，继续往下进行
                print('插入数据出错: %s' % sql)
                continue

        print('已导入csv数据到%s' % table)

        cursor.close()

    ############################################# 导入成分股列表到DB ##################################################
    def add_list_to_db(db : pymysql.connections,
                       cf : conf):
        cursor = db.cursor()
        try:
            cursor.execute("create table %s"%cf.db_list_table_name + "(代码 VARCHAR(10) unique, 名称 VARCHAR(10))")
            print('创建数据表%s' % cf.db_list_table_name)
        except:
            print('数据%s已存在' % cf.db_list_table_name)

        # 获取本地文件列表
        fileList = os.listdir(cf.stock_list_path)
        # 依次对每个数据文件进行存储
        for fileName in fileList:
            data = pd.read_csv(cf.stock_list_path + fileName, encoding="gbk")
            length = len(data)
            for i in range(0, length):
                record = tuple(data.loc[i])
                code = record[1]
                name = record[2]
                if code == 'nan' or name == 'nan' or code == None or name == None:
                    continue
                cursor.execute("insert ignore into %s" % cf.db_list_table_name + "(代码, 名称) values('%s'"%code + ", '%s')"%name)
                # print(code + name)

        cursor.close()

    ############################################# 删除非法条目 ##################################################
    def delete_table_unvalid(db : pymysql.connections,
                             table : str):
        cursor = db.cursor()
        cursor.execute("delete from %s" % table + " where 收盘价=0;")
        cursor.close()

    ############################################# 标记ID ##################################################
    def update_table_id(db : pymysql.connections,
                        table : str):
        cursor = db.cursor()

        cursor.execute('select 日期, id from %s order by 日期 asc' % table)
        acs_result = cursor.fetchall()
        index = 0
        for row in acs_result:
            date = row[0]
            id = row[1]
            index = index + 1
            if id != 0 and id != None:
                index = id
                continue
            cursor.execute("UPDATE %s" % table + " SET id=%d" % index + " where 日期='%s'" % date)

        print('已更新%s id' % table)

        cursor.close()

    ############################################# 计算总股本 ##################################################
    def update_table_stocknum(db : pymysql.connections,
                              table : str):
        cursor = db.cursor()

        cursor.execute('select 日期, 收盘价, 总市值, 总股本 from %s order by 日期 asc' % table)
        acs_result = cursor.fetchall()
        for row in acs_result:
            date = row[0]
            close = float(row[1])
            gmv = row[2]
            stocknum = row[3]
            if stocknum != 0 and stocknum != None:
                continue
            cursor.execute("UPDATE %s" % table + " SET 总股本=%d" % (gmv / close) + " where 日期='%s'" % date)

        print('已更新%s 总股本' % table)

        cursor.close()

    ############################################# 计算日内均价 ##################################################
    def update_table_averprice(db : pymysql.connections,
                               table : str):
        cursor = db.cursor()

        cursor.execute('select 日期, 成交量, 成交金额, 均价, 收盘价 from %s order by 日期 asc' % table)
        acs_result = cursor.fetchall()
        for row in acs_result:
            date = row[0]
            trade_num = float(row[1])
            trade_sum = float(row[2])
            trade_aver = row[3]
            close = row[4]

            # if trade_aver != 0 and trade_aver != None:
            #     continue
            if trade_num == 0:
                print('ERROR: %s' % date + " 成交量为0")
                if close != 0:
                    aver = close
                else:
                    aver = 0
            else:
                aver = float(trade_sum / trade_num)
            cursor.execute("UPDATE %s" % table + " SET 均价=%f" % aver + " where 日期='%s'" % date)

        print('已更新%s 均价' % table)

        cursor.close()

    ############################################# 前复权 ##################################################
    def update_table_rehabilitation(db : pymysql.connections,
                                    table : str):
        cursor = db.cursor()

        preclose = 0.0
        prenum = 0
        multi = 1.0
        bonus = 0.0
        tmpnum = 0
        ttl = 0
        latestnum = 0
        delta = 0.01
        bonus_cnt = 0
        increase_cnt = 0

        cursor.execute('select 日期, id, 收盘价, 总股本, 最高价, 最低价, 开盘价, 前收盘, 涨跌额, 分红, 送转, 均价 from %s order by id desc' % table)
        results = cursor.fetchall()
        for row in results:
            date = row[0]
            id = row[1]
            close = float(row[2])
            num = float(row[3])
            tb_bonus = row[9]
            tb_increase = row[10]

            # 当前到上次记录的分红送转时间，无新分红送转记录，无需继续复权
            if (bonus_cnt == 0 and increase_cnt == 0) and (tb_bonus != None or tb_increase != None):
                if tb_bonus != None:
                    print("上次分红记录点: %s" % date + ", 10派%f" % tb_bonus)
                if tb_increase != None:
                    print("上次送转记录点: %s"%date + ", 10送%f"%tb_increase)
                break

            if latestnum == 0:
                latestnum = num

            # print("prenum=%f" % prenum + ", num=%f" % num + ", preclose=%f" % preclose + ", close=%f" % close)
            # 送转（非增发）
            if ttl == 0:
                tmpnum = 0
            elif ttl > 0:
                ttl = ttl - 1

            # if tmpnum != 0 and tmpnum - num > 10 and prenum - num <= 10 and (preclose - close > delta or close - preclose > delta):
            if tmpnum != 0 and tmpnum - num > 10 and prenum - num <= 10 and preclose != close:
                prenum = tmpnum

            close = float(row[2]) * multi - bonus
            if prenum != 0 and prenum - num > 10:
                # if (preclose - close > delta or close - preclose > delta):
               if preclose != close:
                    print("%s送转: " % date + "10转%d" % (10 * prenum / num))
                    cursor.execute("UPDATE %s" % table + " SET 送转=%f" % (10 * prenum / num) + "where id=%d" % id)
                    increase_cnt = increase_cnt + 1
                    multi = multi * float(num / prenum)
                    tmpnum = 0
                    # print("prenum=%f" % prenum + ", num=%f" % num + ", preclose=%f" % preclose + ", close=%f" % close)
               elif tmpnum == 0:
                    print("%s定增或送转未定: " % date + "%d股" % (prenum - num))
                    tmpnum = prenum
                    ttl = 3

            # 分红
            close = float(row[2]) * multi - bonus
            # if preclose != 0 and close - preclose > delta:
            if preclose != 0 and preclose < close:
                bonus = bonus + close - preclose
                print("%s分红: " % date + "10派%f" % (10 * (close - preclose) * latestnum / num))
                cursor.execute("UPDATE %s" % table + " SET 分红=%f" % (10 * (close - preclose) * latestnum / num) + " where id=%d" % id)
                bonus_cnt = bonus_cnt + 1
                # print("close=%f" % close + ", preclose=%f" % preclose + ", bonus=%f" % bonus)
                # print("prenum=%f" % prenum + ", num=%f" % num + ", preclose=%f" % preclose + ", close=%f" % close)

            close = (float(row[2]) * multi) - bonus
            highest = (float(row[4]) * multi) - bonus
            least = (float(row[5]) * multi) - bonus
            open = (float(row[6]) * multi) - bonus
            preclose = (float(row[7]) * multi) - bonus
            aver = (float(row[11]) * multi) - bonus
            diff = (float(row[8]) * multi)
            prenum = row[3]

            # if datetime.datetime.strftime(row[0], "%Y-%m-%d") == '2001-12-31':
            #     print("最高：%f"%highest + ", open=%f"%open + ", multi=%f"%multi + ", bonus=%f"%bonus)
            #     print("close=%f" % float(row[3]) + ", preclose=%f" % float(row[7]) + ", num=%f" % float(row[28]))
            #     break
            cursor.execute("UPDATE %s" % table + " SET 收盘价=%f" % close + ", 最高价=%f" % highest + ", \
                                   最低价=%f" % least + ", 开盘价=%f" % open + ", 前收盘=%f" % preclose + ", \
                                   涨跌额=%f" % diff + ", 均价=%f" % aver + " \
                                   where id=%d" % id)

        print('已更新%s 分红、送转、前复权' % table)

        cursor.close()

    ############################################# 检查前复权 ##################################################
    def check_table_rehabilitation(db : pymysql.connections,
                                   table : str):
        cursor = db.cursor()

        preclose = 0.0

        cursor.execute('select 日期, id, 收盘价, 前收盘 from %s order by id desc' % table)
        results = cursor.fetchall()
        for row in results:
            date = row[0]
            id = row[1]
            close = float(row[2])

            if preclose != 0 and preclose == close:
                print("%s: " % date + ", close=%f" % close + ", preclose=%f" % preclose)
                # exit(0)

            preclose = float(row[3])

        cursor.close()

    ############################################# 更新列表股票的历史数据 ##################################################
    def update_stock_history(db : pymysql.connections,
                             cf : conf):
        cursor = db.cursor()
        cursor.execute("select 代码 from %s" % cf.db_list_table_name)
        results = cursor.fetchall()
        for row in results:
            print(row)
            code = row[0][3:]
            table = cf.db_stock_prefix + code

            try:
                # 若已存在表，查看最新日期判断是否需要更新
                cursor.execute("select MAX(日期) from %s" % table)
                max_date = cursor.fetchall()[0][0]
            except:
                # 否则，创建表
                mysql.create_stock_table(db, table)
                max_date = None

            # 数据表最新条目已是最新
            if max_date != None and max_date >= stocktime.get_last_trade_time(datetime.datetime.now()).date():
                print("%s已最新" % table)
                continue

            # 下载对应股票历史数据CSV
            csv_path = dl.get_stock_history(code, cf.stock_hist_path)

            # 将CSV数据更新到数据表中
            mysql.update_csv_to_table(db, table, csv_path)

            # 删除非法条目
            mysql.delete_table_unvalid(db, table)

            # 标记ID
            mysql.update_table_id(db, table)

            # 计算总股本
            mysql.update_table_stocknum(db, table)

            # 计算日成交均价
            mysql.update_table_averprice(db, table)

            # 计算前复权
            mysql.update_table_rehabilitation(db, table)

            print("DB commit")
            db.commit()

        cursor.close()
