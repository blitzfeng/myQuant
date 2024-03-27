
import sqlite3
from datetime import datetime, timedelta
import time
import threading
import subprocess
import pandas as pd
import platform
import ctypes

import requests
import os

def load_data(db_name, table_name, symbols):
    # 建立数据库连接
    conn = sqlite3.connect(db_name)
    #在SQL语句中把datetime转换为日期格式
    date = datetime(2024,1,30).timestamp()
    # query = f"SELECT * FROM {table_name} where symbol='{symbols}' AND timestamp < '{date}' ORDER BY datetime DESC LIMIT 26"
    # query = f"SELECT * FROM {table_name} where   timestamp='1706457600'"

    # 读取2024-1-28之前的7条数据
    query = f"SELECT * FROM {table_name} WHERE symbol='{symbols}' ORDER BY datetime DESC LIMIT 30"
    
    df = pd.read_sql_query(query, conn)

    # 关闭数据库连接
    conn.close()
    df = df.iloc[::-1]
    return df
#斐波那契数列计算支撑位 
def supportLevel(df,symbol):
    high_price = df['high'].max()
    low_price = df['low'].min()
    print(f"symbol:{symbol}  最高价：{high_price}")
    print(f"symbol:{symbol}  最低价：{low_price}")
    fibonacci_levels = [0.092,0.236,0.382, 0.5, 0.618,0.764,0.908, 1, 1.236, 1.382]

    for level in fibonacci_levels:
        fib_price = low_price + (high_price - low_price) * level
        print(f"Fibonacci {level*100}% level: {fib_price}")
# 计算Pivot Points
def calculate_pivot_points(high, low, close):
    """
    计算Pivot Points。
    
    :param high: 最高价
    :param low: 最低价
    :param close: 收盘价
    :return: 一个包含Pivot Point及支撑和阻力水平的字典
    """
    # 计算Pivot Point及支撑和阻力水平 并保留整数
    
    pivot_point = int((high + low + close) / 3)
    support1 = int((2 * pivot_point) - high)
    support2 = int(pivot_point - (high - low))
    resistance1 = int((2 * pivot_point) - low)
    resistance2 = int(pivot_point + (high - low))

    return {
        'pivot_point': pivot_point,
        'support1': support1,
        'support2': support2,
        'resistance1': resistance1,
        'resistance2': resistance2
    }
if __name__ == "__main__":

    symbols = ['SA2405','SA2409','JM2405','FG2405','M2405','RM2409','OI2405']
    for symbol in symbols:
        df = load_data("futures_data.db","daily_data",symbol)
        supportLevel(df,symbol)
        print('------------------')
    for symbol in symbols:
        df = load_data("futures_data.db","_60_minute_data",symbol)#daily_data
        # print(df['datetime'])
        high = df['high'].max()
        low = df['low'].min()
        close = df['close'].iloc[-1]
        result = calculate_pivot_points(high,low,close)
        print(f"symbol:{symbol}  {result}")
        # p1 = high - low
        # resistance = low + p1 * 7/8
        # support = low + p1 *0.5/8
        # print(f"symbol:{symbol}  resistance:{resistance} support:{support}  ")
        
