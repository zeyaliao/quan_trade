#导入需要使用到的模块
import datetime
import pymysql
import sys
sys.path.append("..")
from init.config import conf
from .data_struct import percent_data

class statistics:

    ############################################# 查找float ##################################################
    def find_float_value(db: pymysql.connections,
                   table: str,
                   col : str,
                   id : int):
        cursor = db.cursor()
        value = 0.0

        try:
            cursor.execute("select %s" % col + " from %s" % table + " where id=%d" % id)
        except:
            return 0.0

        results = cursor.fetchall()
        for row in results:
            value = float(row[0])

        cursor.close()
        return value

    ############################################# 统计涨跌幅 ##################################################
    def statistics_precent(db: pymysql.connections,
                            table: str,
                            result : str,
                            start : datetime.date,
                            end : datetime.date):
        cursor = db.cursor()
        data = percent_data.init(1, 0)
        data.start = start
        data.end = end

        num = 0
        num_m9 = 0
        num_p9 = 0
        num_half = 0
        num_double = 0

        cursor.execute('select id, 日期, 股票代码, 名称, 涨跌幅, 均价, 成交金额, 20日均额 from %s order by id asc' % table)
        results = cursor.fetchall()
        for row in results:
            id = row[0]

            # 日期
            date = row[1]
            if date < start:
                continue
            elif date > end:
                break
            data.end = date
            if id == 1:
                data.start = date

            data.code = row[2]
            data.name = row[3]
            per = float(row[4])
            aver = float(row[5])
            amount = float(row[6])
            if row[7] != None:
                amount_av20 = float(row[7])
            else:
                amount_av20 = amount

            data.change = data.change * (1 + per / 100)

            num = num + 1
            if per <= -9.9:
                data.m_10 = data.m_10 + 1
                data.except_m9 = data.except_m9 * (1 + per / 100)
            elif per > -9.9 and per <= -9:
                data.m_10_9 = data.m_10_9 + 1
                data.except_m9 = data.except_m9 * (1 + per / 100)
            elif per > -9 and per <= -7:
                data.m_9_7 = data.m_9_7 + 1
            elif per > -7 and per <= -5:
                data.m_7_5 = data.m_7_5 + 1
            elif per > -5 and per <= -3:
                data.m_5_3 = data.m_5_3 + 1
            elif per > -3 and per <= -1:
                data.m_3_1 = data.m_3_1 + 1
            elif per > -1 and per <= 0:
                data.m_1_0 = data.m_1_0 + 1
            elif per > 0 and per <= 1:
                data.p0_1 = data.p0_1 + 1
            elif per > 1 and per <= 3:
                data.p1_3 = data.p1_3 + 1
            elif per > 3 and per <= 5:
                data.p3_5 = data.p3_5 + 1
            elif per > 5 and per <= 7:
                data.p5_7 = data.p5_7 + 1
            elif per > 7 and per <= 9:
                data.p7_9 = data.p7_9 + 1
            elif per > 9 and per <= 9.9:
                data.p9_10 = data.p9_10 + 1
                data.except_p9 = data.except_p9 * (1 + per / 100)
            elif per > 9.9:
                data.p10 = data.p10 + 1
                data.except_p9 = data.except_p9 * (1 + per / 100)

            if per <= -9.9 or per > 9.9 or amount < amount_av20 * 0.5 or amount > amount_av20 * 2:
                aver_1 = statistics.find_float_value(db, table, "均价", id + 1)
                aver_5 = statistics.find_float_value(db, table, "均价", id + 5)
                aver_10 = statistics.find_float_value(db, table, "均价", id + 10)
                aver_20 = statistics.find_float_value(db, table, "均价", id + 20)
                if aver_1 == 0:
                    aver_1 = aver
                if aver_5 == 0:
                    aver_5 = aver
                if aver_10 == 0:
                    aver_10 = aver
                if aver_20 == 0:
                    aver_20 = aver

                if per <= -9.9 and aver != 0:
                    data.after_m10_1 = data.after_m10_1 + (aver_1 / aver) - 1
                    data.after_m10_5 = data.after_m10_5 + (aver_5 / aver) - 1
                    data.after_m10_10 = data.after_m10_10 + (aver_10 / aver) - 1
                    data.after_m10_20 = data.after_m10_20 + (aver_20 / aver) - 1
                    num_m9 = num_m9 + 1
                elif per > 9.9 and aver != 0:
                    data.after_p10_1 = data.after_p10_1 + (aver_1 / aver) - 1
                    data.after_p10_5 = data.after_p10_5 + (aver_5 / aver) - 1
                    data.after_p10_10 = data.after_p10_10 + (aver_10 / aver) - 1
                    data.after_p10_20 = data.after_p10_20 + (aver_20 / aver) - 1
                    num_p9 = num_p9 + 1

                if amount < amount_av20 * 0.5 and aver != 0:
                    data.amount_half_1 = data.amount_half_1 + (aver_1 / aver) - 1
                    data.amount_half_5 = data.amount_half_5 + (aver_5 / aver) - 1
                    data.amount_half_10 = data.amount_half_10 + (aver_10 / aver) - 1
                    data.amount_half_20 = data.amount_half_20 + (aver_20 / aver) - 1
                    num_half = num_half + 1
                elif amount > amount_av20 * 2 and aver != 0:
                    data.amount_double_1 = data.amount_double_1 + (aver_1 / aver) - 1
                    data.amount_double_5 = data.amount_double_5 + (aver_5 / aver) - 1
                    data.amount_double_10 = data.amount_double_10 + (aver_10 / aver) - 1
                    data.amount_double_20 = data.amount_double_20 + (aver_20 / aver) - 1
                    num_double = num_double + 1

        top_per1 = int(1 * num / 100)

        cursor.execute('select id, 日期, 涨跌幅 from %s order by 涨跌幅 asc' % table + " limit %d" % top_per1)
        results = cursor.fetchall()
        for row in results:
            # 日期
            date = row[1]
            if date < start or date > end:
                continue
            per = float(row[2])
            data.except_min_per1 = data.except_min_per1 * (1 + per / 100)

        cursor.execute('select id, 日期, 涨跌幅 from %s order by 涨跌幅 desc' % table + " limit %d" % top_per1)
        results = cursor.fetchall()
        for row in results:
            # 日期
            date = row[1]
            if date < start or date > end:
                continue
            per = float(row[2])
            data.except_top_per1 = data.except_top_per1 * (1 + per / 100)


        data.m_10 = data.m_10 / num
        data.m_10_9 = data.m_10_9 / num
        data.m_9_7 = data.m_9_7 / num
        data.m_7_5 = data.m_7_5 / num
        data.m_5_3 = data.m_5_3 / num
        data.m_3_1 = data.m_3_1 / num
        data.m_1_0 = data.m_1_0 / num
        data.p0_1 = data.p0_1 / num
        data.p1_3 = data.p1_3 / num
        data.p3_5 = data.p3_5 / num
        data.p5_7 = data.p5_7 / num
        data.p7_9 = data.p7_9 / num
        data.p9_10 = data.p9_10 / num
        data.p10 = data.p10 / num
        data.except_m9 = data.change / data.except_m9
        data.except_p9 = data.change / data.except_p9

        if num_m9 == 0:
            num_m9 = 1
        if num_p9 == 0:
            num_p9 = 1
        if num_half == 0:
            num_half = 1
        if num_double == 0:
            num_double = 1
        data.after_m10_1 = data.after_m10_1 / num_m9
        data.after_m10_5 = data.after_m10_5 / num_m9
        data.after_m10_10 = data.after_m10_10 / num_m9
        data.after_m10_20 = data.after_m10_20 / num_m9
        data.after_p10_1 = data.after_p10_1 / num_p9
        data.after_p10_5 = data.after_p10_5 / num_p9
        data.after_p10_10 = data.after_p10_10 / num_p9
        data.after_p10_20 = data.after_p10_20 / num_p9
        data.amount_half_1 = data.amount_half_1 / num_half
        data.amount_half_5 = data.amount_half_5 / num_half
        data.amount_half_10 = data.amount_half_10 / num_half
        data.amount_half_20 = data.amount_half_20 / num_half
        data.amount_double_1 = data.amount_double_1 / num_double
        data.amount_double_5 = data.amount_double_5 / num_double
        data.amount_double_10 = data.amount_double_10 / num_double
        data.amount_double_20 = data.amount_double_20 / num_double
        data.except_min_per1 = data.change / data.except_min_per1
        data.except_top_per1 = data.change / data.except_top_per1

        cursor.execute("insert ignore into %s (股票代码, 名称, 开始日期, 结束日期, 期间变化," % result + \
                                                                    " m_10, m_10_9, m_9_7, m_7_5, m_5_3, m_3_1, m_1_0," + \
                                                                    " p0_1, p1_3, p3_5, p5_7, p7_9, p9_10, p10_," + \
                                                                    " except_p9, except_m9," + \
                                                                    " after_m10_1, after_m10_5, after_m10_10, after_m10_20," + \
                                                                    " after_p10_1, after_p10_5, after_p10_10, after_p10_20," + \
                                                                    " amount_half_1, amount_half_5, amount_half_10, amount_half_20," + \
                                                                    " amount_double_1, amount_double_5, amount_double_10, amount_double_20," + \
                                                                    " except_top_per1, exceot_min_per1)" + \
                                                            " values ('%s'" % data.code + ",'%s'"%data.name + ",'%s'"%data.start + ",'%s'"%data.end + ",%f"%data.change + \
                                                                    ",%f" % data.m_10 + ",%f" % data.m_10_9 + ",%f" % data.m_9_7 + ",%f" % data.m_7_5 + ",%f" % data.m_5_3 + ",%f" % data.m_3_1 + ",%f" % data.m_1_0 + \
                                                                    ",%f" % data.p0_1 + ",%f" % data.p1_3 + ",%f" % data.p3_5 + ",%f" % data.p5_7 + ",%f" % data.p7_9 + ",%f" % data.p9_10 + ",%f" % data.p10 + \
                                                                    ",%f" % data.except_p9 + ",%f" % data.except_m9 + \
                                                                    ",%f" % data.after_m10_1 + ",%f" % data.after_m10_5 + ",%f" % data.after_m10_10 + ",%f" % data.after_m10_20 + \
                                                                    ",%f" % data.after_p10_1 + ",%f" % data.after_p10_5 + ",%f" % data.after_p10_10 + ",%f" % data.after_p10_20 + \
                                                                    ",%f" % data.amount_half_1 + ",%f" % data.amount_half_5 + ",%f" % data.amount_half_10 + ",%f" % data.amount_half_20 + \
                                                                    ",%f" % data.amount_double_1 + ",%f" % data.amount_double_5 + ",%f" % data.amount_double_10 + ",%f" % data.amount_double_20 + \
                                                                    ",%f" % data.except_top_per1 + ",%f)" % data.except_min_per1)

        print('完成统计%s: ' % table + "涨跌幅")
        db.commit()
        cursor.close()
        return data

    ############################################# test ##################################################
    def statistics_test(db : pymysql.connections,
                        cf : conf):
        cursor = db.cursor()

        result = "statistics_percent_recent_4years"
        start = datetime.date(2017, 1, 1)
        end = datetime.date(2021, 6, 26)
        num = 0
        sum = percent_data.init(0, 0)

        cursor.execute("select 代码 from %s" % cf.db_list_table_name)
        results = cursor.fetchall()
        for row in results:
            num = num + 1
            code = row[0][3:]

            print("正在执行：%s" % code + ", No.%d" % num)
            table = cf.db_stock_prefix + code

            data = statistics.statistics_precent(db, table, result, start, end)
            sum = percent_data.add(sum, data)

            # break

        data = percent_data.divide(sum, num)
        cursor.execute("insert ignore into %s (股票代码, 名称, 期间变化," % result + \
                       " m_10, m_10_9, m_9_7, m_7_5, m_5_3, m_3_1, m_1_0," + \
                       " p0_1, p1_3, p3_5, p5_7, p7_9, p9_10, p10_," + \
                       " except_p9, except_m9," + \
                       " after_m10_1, after_m10_5, after_m10_10, after_m10_20," + \
                       " after_p10_1, after_p10_5, after_p10_10, after_p10_20," + \
                       " amount_half_1, amount_half_5, amount_half_10, amount_half_20," + \
                       " amount_double_1, amount_double_5, amount_double_10, amount_double_20," + \
                       " except_top_per1, exceot_min_per1)" + \
                       " values ('all'" + ",'all'" + ",%f" % data.change + \
                       ",%f" % data.m_10 + ",%f" % data.m_10_9 + ",%f" % data.m_9_7 + ",%f" % data.m_7_5 + ",%f" % data.m_5_3 + ",%f" % data.m_3_1 + ",%f" % data.m_1_0 + \
                       ",%f" % data.p0_1 + ",%f" % data.p1_3 + ",%f" % data.p3_5 + ",%f" % data.p5_7 + ",%f" % data.p7_9 + ",%f" % data.p9_10 + ",%f" % data.p10 + \
                       ",%f" % data.except_p9 + ",%f" % data.except_m9 + \
                       ",%f" % data.after_m10_1 + ",%f" % data.after_m10_5 + ",%f" % data.after_m10_10 + ",%f" % data.after_m10_20 + \
                       ",%f" % data.after_p10_1 + ",%f" % data.after_p10_5 + ",%f" % data.after_p10_10 + ",%f" % data.after_p10_20 + \
                       ",%f" % data.amount_half_1 + ",%f" % data.amount_half_5 + ",%f" % data.amount_half_10 + ",%f" % data.amount_half_20 + \
                       ",%f" % data.amount_double_1 + ",%f" % data.amount_double_5 + ",%f" % data.amount_double_10 + ",%f" % data.amount_double_20 + \
                       ",%f" % data.except_top_per1 + ",%f)" % data.except_min_per1)

# data = percent_data()
# data.p10 = 0
# print("%f" % data.p10)