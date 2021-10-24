from init.config import get_config
from init.download import dl
from init.database import mysql
from index.cal_index import cal_index
from strategy.strategy import strategy
from strategy.statistics import percent_data
from strategy.statistics import statistics

if __name__=="__main__":
    #初始化静态数据
    cf = get_config()

    # 下载指数成分股
    # dl.get_hs300_sz50_zz500(cf.stock_list_path)

    # 数据库连接
    db = mysql.connect(cf)

    # 添加股票列表到数据库
    # mysql.add_list_to_db(db, cf)

    # 下载股票列表历史数据并存入数据库，并进行基础处理
    # mysql.update_stock_history(db, cf)

    # 计算所需指标
    # cal_index.cal_all_indexs(db, cf)

    # 统计
    statistics.statistics_test(db, cf)

    # 测试策略
    # strategy.strategy_test(db, cf)

    # 数据库断开
    mysql.disconnect(db)
