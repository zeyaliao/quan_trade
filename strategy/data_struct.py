#导入需要使用到的模块
import datetime

class percent_data:
    code : str
    name : str
    start : datetime.date
    end : datetime.date
    change : float
    m_10 : float
    m_10_9 : float
    m_9_7 : float
    m_7_5 : float
    m_5_3 : float
    m_3_1 : float
    m_1_0 : float
    p0_1 : float
    p1_3 : float
    p3_5 : float
    p5_7 : float
    p7_9 : float
    p9_10 : float
    p10 : float
    except_p9 : float
    except_m9 : float
    after_m10_1 : float
    after_m10_5 : float
    after_m10_10 : float
    after_m10_20 : float
    after_p10_1 : float
    after_p10_5 : float
    after_p10_10 : float
    after_p10_20 : float
    amount_half_1 : float
    amount_half_5 : float
    amount_half_10 : float
    amount_half_20 : float
    amount_double_1 : float
    amount_double_5 : float
    amount_double_10 : float
    amount_double_20 : float
    except_top_per1 : float
    except_min_per1 : float

    def init(multi : float,
             add : float):
        data = percent_data()

        data.change = multi
        data.except_m9 = multi
        data.except_p9 = multi
        data.except_min_per1 = multi
        data.except_top_per1 = multi

        data.m_10 = add
        data.m_10_9 = add
        data.m_9_7 = add
        data.m_7_5 = add
        data.m_5_3 = add
        data.m_3_1 = add
        data.m_1_0 = add
        data.p0_1 = add
        data.p1_3 = add
        data.p3_5 = add
        data.p5_7 = add
        data.p7_9 = add
        data.p9_10 = add
        data.p10 = add
        data.after_m10_1 = add
        data.after_m10_5 = add
        data.after_m10_10 = add
        data.after_m10_20 = add
        data.after_p10_1 = add
        data.after_p10_5 = add
        data.after_p10_10 = add
        data.after_p10_20 = add
        data.amount_half_1 = add
        data.amount_half_5 = add
        data.amount_half_10 = add
        data.amount_half_20 = add
        data.amount_double_1 = add
        data.amount_double_5 = add
        data.amount_double_10 = add
        data.amount_double_20 = add

        return data

    def add(sum, add):
        sum.change = sum.change + add.change
        sum.except_m9 = sum.except_m9 + add.except_m9
        sum.except_p9 = sum.except_p9 + add.except_p9
        sum.except_min_per1 = sum.except_min_per1 + add.except_min_per1
        sum.except_top_per1 = sum.except_top_per1 + add.except_top_per1

        sum.m_10 = sum.m_10 + add.m_10
        sum.m_10_9 = sum.m_10_9 + add.m_10_9
        sum.m_9_7 = sum.m_9_7 + add.m_9_7
        sum.m_7_5 = sum.m_7_5 + add.m_7_5
        sum.m_5_3 = sum.m_5_3 + add.m_5_3
        sum.m_3_1 = sum.m_3_1 + add.m_3_1
        sum.m_1_0 = sum.m_1_0 + add.m_1_0
        sum.p0_1 = sum.p0_1 + add.p0_1
        sum.p1_3 = sum.p1_3 + add.p1_3
        sum.p3_5 = sum.p3_5 + add.p3_5
        sum.p5_7 = sum.p5_7 + add.p5_7
        sum.p7_9 = sum.p7_9 + add.p7_9
        sum.p9_10 = sum.p9_10 + add.p9_10
        sum.p10 = sum.p10 + add.p10
        sum.after_m10_1 = sum.after_m10_1 + add.after_m10_1
        sum.after_m10_5 = sum.after_m10_5 + add.after_m10_5
        sum.after_m10_10 = sum.after_m10_10 + add.after_m10_10
        sum.after_m10_20 = sum.after_m10_20 + add.after_m10_20
        sum.after_p10_1 = sum.after_p10_1 + add.after_p10_1
        sum.after_p10_5 = sum.after_p10_5 + add.after_p10_5
        sum.after_p10_10 = sum.after_p10_10 + add.after_p10_10
        sum.after_p10_20 = sum.after_p10_20 + add.after_p10_20
        sum.amount_half_1 = sum.amount_half_1 + add.amount_half_1
        sum.amount_half_5 = sum.amount_half_5 +add.amount_half_5
        sum.amount_half_10 = sum.amount_half_10 + add.amount_half_10
        sum.amount_half_20 = sum.amount_half_20 + add.amount_half_20
        sum.amount_double_1 = sum.amount_double_1 + add.amount_double_1
        sum.amount_double_5 = sum.amount_double_5 + add.amount_double_5
        sum.amount_double_10 = sum.amount_double_10 + add.amount_double_10
        sum.amount_double_20 = sum.amount_double_20 + add.amount_double_20

        return sum

    def divide(sum, num):
        sum.change = sum.change / num
        sum.except_m9 = sum.except_m9 / num
        sum.except_p9 = sum.except_p9 / num
        sum.except_min_per1 = sum.except_min_per1 / num
        sum.except_top_per1 = sum.except_top_per1 / num

        sum.m_10 = sum.m_10 / num
        sum.m_10_9 = sum.m_10_9 / num
        sum.m_9_7 = sum.m_9_7 / num
        sum.m_7_5 = sum.m_7_5 / num
        sum.m_5_3 = sum.m_5_3 / num
        sum.m_3_1 = sum.m_3_1 / num
        sum.m_1_0 = sum.m_1_0 / num
        sum.p0_1 = sum.p0_1 / num
        sum.p1_3 = sum.p1_3 / num
        sum.p3_5 = sum.p3_5 / num
        sum.p5_7 = sum.p5_7 / num
        sum.p7_9 = sum.p7_9 / num
        sum.p9_10 = sum.p9_10 / num
        sum.p10 = sum.p10 / num
        sum.after_m10_1 = sum.after_m10_1 / num
        sum.after_m10_5 = sum.after_m10_5 / num
        sum.after_m10_10 = sum.after_m10_10 / num
        sum.after_m10_20 = sum.after_m10_20 / num
        sum.after_p10_1 = sum.after_p10_1 / num
        sum.after_p10_5 = sum.after_p10_5 / num
        sum.after_p10_10 = sum.after_p10_10 / num
        sum.after_p10_20 = sum.after_p10_20 / num
        sum.amount_half_1 = sum.amount_half_1 / num
        sum.amount_half_5 = sum.amount_half_5 / num
        sum.amount_half_10 = sum.amount_half_10 / num
        sum.amount_half_20 = sum.amount_half_20 / num
        sum.amount_double_1 = sum.amount_double_1 / num
        sum.amount_double_5 = sum.amount_double_5 / num
        sum.amount_double_10 = sum.amount_double_10 / num
        sum.amount_double_20 = sum.amount_double_20 / num

        return sum
