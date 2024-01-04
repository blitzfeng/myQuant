# import akshare as ak
# import pandas as pd
# import time

# 获取郑商所的所有主力合约
# main_contracts = ak.match_main_contract(symbol='czce')
# print(type(main_contracts))

# filter_data = main_contracts[~main_contracts.str.constains("无主力合约")]

# print(",".join([main_contracts]))

# futures_zh_daily_df = ak.futures_zh_daily_sina(symbol="JR2301")
# print(futures_zh_daily_df)

# while True:
#     time.sleep(3)
#     futures_zh_spot_df = ak.futures_zh_spot(
#         symbol=",".join([main_contracts]),
#         market="CF",
#         adjust='0')
#     print(futures_zh_spot_df)
import sqlite3
from datetime import datetime, timedelta

def query_data(db_path, input_timestamp):
    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 计算5天前的时间戳
    end_timestamp = datetime.fromisoformat(input_timestamp)
    start_timestamp = end_timestamp - timedelta(days=5)
    print(start_timestamp)
    print(end_timestamp)
    # 构建查询语句
    query = f'''
    SELECT * FROM sa2405_30_minute_data
    WHERE datetime BETWEEN '{start_timestamp.isoformat()}' AND '{end_timestamp.isoformat()}'
    ORDER BY timestamp
    '''

    # 执行查询
    cursor.execute(query)
    rows = cursor.fetchall()

    # 关闭数据库连接
    conn.close()

    return rows

# 示例用法

db_path = 'futures_data.db'  # 数据库文件路径
input_timestamp = '2024-01-02 10:00:00'  # 示例输入时间戳
data = query_data(db_path, input_timestamp)

# 输出结果
for row in data:
    print(row)
