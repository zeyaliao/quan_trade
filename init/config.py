#导入需要使用到的模块
import os
import configparser

class conf:
    path : str
    data_path : str
    stock_list_path : str
    stock_hist_path : str
    db_user : str
    db_pwd : str
    db_name : str
    db_list_table_name : str
    db_stock_prefix : str

def get_config():
    cf = conf()
    cf.path = os.path.split(os.path.realpath(__file__))[0] + "\\"

    cfp = configparser.ConfigParser()
    cfp.read(cf.path + "config.ini")

    cf.data_path = cf.path + cfp.get("csv", "data_dir") + "\\"
    cf.stock_list_path = cf.data_path + cfp.get("csv", "stock_list_dir") + "\\"
    cf.stock_hist_path = cf.data_path + cfp.get("csv", "stock_hist_dir") + "\\"
    cf.db_user = cfp.get("db", "user")
    cf.db_pwd = cfp.get("db", "pwd")
    cf.db_name = cfp.get("db", "db_name")
    cf.db_list_table_name = cfp.get("db", "list_table_name")
    cf.db_stock_prefix = cfp.get("db", "stock_prefix")

    return cf