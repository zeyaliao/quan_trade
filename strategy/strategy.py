#导入需要使用到的模块
import datetime
import pymysql
import sys
sys.path.append("..")
from init.config import conf

class strategy:

    ############################################# 测试MACD交叉策略 ##################################################
    def strategy_macd_cross(db: pymysql.connections,
                            table: str,
                            start : datetime.date,
                            end : datetime.date):
        cursor = db.cursor()

        buy = 0.0
        sale = 0.0
        predif = 0.0
        predea = 0.0
        premacd = 0.0
        money = 10000.0
        earn_sum = 0.0
        earn_num = 0

        cursor.execute('select id, 日期, 均价, MACD_DIF, MACD_DEA, MACD from %s order by id asc' % table)
        results = cursor.fetchall()
        sum = 0.0
        for row in results:
            id = row[0]
            date = row[1]
            aver = row[2]
            dif = row[3]
            dea = row[4]
            macd = row[5]

            if date <= start:
                continue
            elif date > end:
                break

            # 金叉买，死叉卖
            if predif < predea and dif >= dea and buy == 0 and sale == 0:
                buy = aver
                # print("%s" % row[0] + ", buy=%f" % buy)
            if predif > predea and dif <= dea and buy != 0 and sale == 0:
                sale = aver
                earn = (sale - buy) / buy
                earn_sum = earn_sum + earn
                earn_num = earn_num + 1
                money = money * (1 + earn)
                # print("%s" % row[0] + ", sale=%f" % sale + ", earn=%f"%(100 * earn) + r'%' + ", money=%f"%money)
                buy = 0.0
                sale = 0.0

            predif = row[3]
            predea = row[4]
            premacd = row[5]

        print('完成回测%s: ' % table + "MACD交叉")
        cursor.close()

    ############################################# test ##################################################
    def strategy_test(db : pymysql.connections,
                      cf : conf):
        cursor = db.cursor()

        start = datetime.date(2020, 1, 1)
        end = datetime.date(2021, 1, 1)

        cursor.execute("select 代码 from %s" % cf.db_list_table_name)
        results = cursor.fetchall()
        for row in results:
            print(row)
            code = row[0][3:]
            table = cf.db_stock_prefix + code

            strategy.strategy_macd_cross(db, table, start, end)