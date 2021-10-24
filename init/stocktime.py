import time
import datetime
from chinese_calendar import is_workday, is_holiday

class stocktime:
    def is_tradeday(time : datetime.datetime):
        dayofweek = time.isoweekday()
        if is_workday(time) and dayofweek >= 1 and dayofweek <= 5:
            return True
        else:
            return False

    def get_last_trade_time(time : datetime.datetime):
        # 获取最近收盘时间
        if time.hour < 15 or not stocktime.is_tradeday(time):
            last_trade_date = (time - datetime.timedelta(days=1))
            while is_holiday(last_trade_date):
                last_trade_date = (last_trade_date - datetime.timedelta(days=1))
            last_trade_date = last_trade_date.replace(hour=15, minute=0, second=0, microsecond=0)
            return last_trade_date
        else:
            last_trade_date = time.replace(hour=15, minute=0, second=0, microsecond=0)
            return last_trade_date


    # get_stock_history("sz.002142", "C:\\Users\\liaozeya\\Desktop\\Quantitative_trading\\1.python\\1\\init\\data\\hist\\")