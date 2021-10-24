#导入需要使用到的模块
import pymysql
import sys
sys.path.append("..")
from init.config import conf

class cal_index:

    ############################################# MA ##################################################
    def cal_ma(db : pymysql.connections,
               table: str,
               days : int):
        cursor = db.cursor()

        # 插入列
        try:
            cursor.execute("alter table %s " % table + "add column %d日均价 float;" % days)
        except:
            print("表: %s" % table + "已存在字段：%d日均价" % days)

        cursor.execute('select id, 收盘价 from %s order by id asc' % table)
        results = cursor.fetchall()
        sum = 0.0
        for row in results:
            id = row[0]
            close = float(row[1])

            if id < days:
                sum = sum + close
            else:
                if id == days:
                    sum = sum + close
                else:
                    cursor.execute("select 收盘价 from %s" % table + " where id=%d" % (id - days))
                    results = cursor.fetchall()
                    closepre = float(results[0][0])
                    sum = sum + close - closepre
                ma = sum / days
                cursor.execute("UPDATE %s" % table + " SET %d日均价"%days + "=%f" % ma + " where id=%d" % id)

        print('完成计算%s: ' % table + "MA%d" % days)
        cursor.close()

    ############################################# 均价 ##################################################
    def cal_mav(db: pymysql.connections,
                table: str,
                days: int):
        cursor = db.cursor()

        # 插入列
        try:
            cursor.execute("alter table %s " % table + "add column MAV%d float;" % days)
        except:
            print("表: %s" % table + "已存在字段：MAV%d" % days)

        cursor.execute('select id, 均价 from %s order by id asc' % table)
        results = cursor.fetchall()
        sum = 0.0
        for row in results:
            id = row[0]
            aver = float(row[1])

            if id < days:
                sum = sum + aver
            else:
                if id == days:
                    sum = sum + aver
                else:
                    cursor.execute("select 均价 from %s" % table + " where id=%d" % (id - days))
                    results = cursor.fetchall()
                    averpre = float(results[0][0])
                    sum = sum + aver - averpre
                mav = sum / days
                cursor.execute("UPDATE %s" % table + " SET MAV%d" % days + "=%f" % mav + " where id=%d" % id)

        print('完成计算%s: ' % table + "MAV%d" % days)
        cursor.close()

    ############################################# 均量 ##################################################
    def cal_aver_amount(db: pymysql.connections,
                table: str,
                days: int):
        cursor = db.cursor()

        # 插入列
        try:
            cursor.execute("alter table %s " % table + "add column %d日均量 float;" % days)
        except:
            print("表: %s" % table + "已存在字段：%d日均量" % days)

        try:
            cursor.execute("alter table %s " % table + "add column %d日均额 float;" % days)
        except:
            print("表: %s" % table + "已存在字段：%d日均额" % days)

        cursor.execute('select id, 成交量, 成交金额 from %s order by id asc' % table)
        results = cursor.fetchall()
        count_sum = 0.0
        amount_sum = 0.0
        for row in results:
            id = row[0]
            count = float(row[1])
            amount = float(row[2])

            if id < days:
                count_sum = count_sum + count
                amount_sum = amount_sum + amount
            else:
                if id == days:
                    count_sum = count_sum + count
                    amount_sum = amount_sum + amount
                else:
                    # 成交量之和
                    cursor.execute("select 成交量 from %s" % table + " where id=%d" % (id - days))
                    results = cursor.fetchall()
                    countpre = float(results[0][0])
                    count_sum = count_sum + count - countpre
                    # 成交额之和
                    cursor.execute("select 成交金额 from %s" % table + " where id=%d" % (id - days))
                    results = cursor.fetchall()
                    amountpre = float(results[0][0])
                    amount_sum = amount_sum + amount - amountpre

                aver_count = count_sum / days
                aver_amount = amount_sum / days
                cursor.execute("UPDATE %s" % table + " SET %d日均量" % days + "=%f" % aver_count +\
                                                        ", %d日均额" % days + "=%f" % aver_amount +\
                                                        " where id=%d" % id)

        print('完成计算%s: ' % table + "%d日均量、均额" % days)
        cursor.close()

    ############################################# MACD ##################################################
    def cal_macd(db: pymysql.connections,
                 table: str,
                 fast : float,
                 slow : float,
                 aver : float):
        cursor = db.cursor()

        # 插入列
        try:
            cursor.execute("alter table %s " % table + "add column MACD_DIF float, \
                                                        add column MACD_DEA float, \
                                                        add column MACD float;")
        except:
            print("表: %s" % table + "已存在MACD字段")

        dif = 0.0
        dea = 0.0
        bar = 0.0
        macd = 0.0

        cursor.execute('select id, 收盘价 from %s order by id asc' % table)
        results = cursor.fetchall()
        for row in results:
            id = row[0]
            close = float(row[1])

            if id == 1:
                preclose = close
                continue
            elif id == 2:
                ema_fast = preclose * (fast - 1) / (fast + 1) + close * 2 / (fast + 1)
                ema_slow = preclose * (slow - 1) / (slow + 1) + close * 2 / (slow + 1)
                dif = ema_fast - ema_slow
                dea = dif * 2 / (aver + 1)
                bar = 2 * (dif - dea)
            else:
                ema_fast = ema_fast * (fast - 1) / (fast + 1) + close * 2 / (fast + 1)
                ema_slow = ema_slow * (slow - 1) / (slow + 1) + close * 2 / (slow + 1)
                dif = ema_fast - ema_slow
                dea = dea * (aver - 1) / (aver + 1) + dif * 2 / (aver + 1)
                bar = 2 * (dif - dea)

            # print("%s" % row[0] + ", dif=%f" % dif + ", dea=%f" % dea + ", bar=%f" % bar)
            cursor.execute("UPDATE %s" % table + " SET MACD_DIF=%f" % dif + ", MACD_DEA=%f" % dea + ", \
                                       MACD=%f" % bar + " where id=%d" % id)

        print('完成计算%s: ' % table + "MACD")
        cursor.close()

    ############################################# KDJ ##################################################
    def cal_kdj(db: pymysql.connections,
                table: str,
                a: float,
                b: float,
                c: float):
        cursor = db.cursor()

        # 插入列
        try:
            cursor.execute("alter table %s " % table + "add column KDJ_K float, \
                                                        add column KDJ_D float, \
                                                        add column KDJ_J float;")
        except:
            print("表: %s" % table + "已存在KDJ字段")

        k = 100.0
        d = 100.0

        cursor.execute('select id, 收盘价 from %s order by id asc' % table)
        results = cursor.fetchall()
        for row in results:
            id = row[0]
            close = float(row[1])

            if id < b:
                continue
            else:
                highest = -100000.0
                least = 100000.0
                for i in range(0, a):
                    if id - i <= 0:
                        break
                    cursor.execute("select 最高价, 最低价 from %s" % table + " where id=%d" % (id - i))
                    tmpres = cursor.fetchall()
                    high = float(tmpres[0][0])
                    low = float(tmpres[0][1])
                    if high > highest:
                        highest = high
                    if low < least:
                        least = low
                    # print("%s" % tmpres[0][0] + ", high=%f" % high + ", low=%f" % low + ", close=%f" % float(tmpres[0][3]) + ", highest=%f" % highest + ", least=%f" % least)

                rsv = 100 * (close - least) / (highest - least)
                k = k * (b - 1) / b + rsv / b
                d = d * (c - 1) / c + k / c
                j = 3 * k - 2 * d

            # print("%s"%row[0] + ", k=%f"%k + ", d=%f"%d + ", j=%f"%j + ", close=%f"%close + ", highest=%f"%highest + ", least=%f"%least + ", rsv=%f"%rsv)

            cursor.execute("UPDATE %s" % table + " SET KDJ_K=%f" % k + ", KDJ_D=%f" % d + ", \
                                       KDJ_J=%f" % j + " where id=%d" % id)

        print('完成计算%s: ' % table + "KDJ")
        cursor.close()

    ############################################# 计算所有指标 ##################################################
    def cal_all_indexs(db : pymysql.connections,
                       cf : conf):
        cursor = db.cursor()
        cursor.execute("select 代码 from %s" % cf.db_list_table_name)
        results = cursor.fetchall()
        for row in results:
            print(row)
            code = row[0][3:]
            table = cf.db_stock_prefix + code

            # cal_index.cal_ma(db, table, 5)
            # cal_index.cal_ma(db, table, 10)
            # cal_index.cal_ma(db, table, 20)
            # cal_index.cal_ma(db, table, 30)
            # cal_index.cal_ma(db, table, 60)
            # cal_index.cal_ma(db, table, 120)
            # cal_index.cal_macd(db, table, 12, 26, 9)
            # cal_index.cal_kdj(db, table, 9, 3, 3)

            cal_index.cal_mav(db, table, 5)
            cal_index.cal_mav(db, table, 10)
            cal_index.cal_mav(db, table, 20)
            cal_index.cal_mav(db, table, 30)
            cal_index.cal_mav(db, table, 60)
            cal_index.cal_mav(db, table, 120)

            cal_index.cal_aver_amount(db, table, 5)
            cal_index.cal_aver_amount(db, table, 10)
            cal_index.cal_aver_amount(db, table, 20)
            cal_index.cal_aver_amount(db, table, 30)
            cal_index.cal_aver_amount(db, table, 60)
            cal_index.cal_aver_amount(db, table, 120)

            # break
