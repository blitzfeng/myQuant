import akshare as ak
import sqlite3
from datetime import datetime, timedelta
import time
import threading
import random

def create_minute_table_if_not_exists(table_name, columns):
    conn = sqlite3.connect('futures_data.db')
    cursor = conn.cursor()

    # 使用传入的列名创建表
    create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER UNIQUE,
            {', '.join([f"{column} REAL" for column in columns])}
        )
    '''
    cursor.execute(create_table_sql)

    conn.commit()
    conn.close()

def create_daily_table_if_not_exists(table_name, columns):
    conn = sqlite3.connect('futures_data.db')
    cursor = conn.cursor()

    # 使用传入的列名创建表
    create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER UNIQUE,
            {', '.join([f"{column} REAL" for column in columns])}
            
        )
    '''
    cursor.execute(create_table_sql)

    conn.commit()
    conn.close()

def fetch_and_store_minute_data(symbol, period=1):
    # 获取分时数据
    minute_data = ak.futures_zh_minute_sina(symbol=symbol, period=period)

    if not minute_data.empty:
        # 过滤掉表头
        minute_data = minute_data.iloc[1:]

        table_name = f'sa2405_{period}_minute_data'
        columns = ['timestamp','datetime', 'open', 'high', 'low', 'close', 'volume', 'hold']

        # 连接数据库
        conn = sqlite3.connect('futures_data.db')
        cursor = conn.cursor()

        # 创建表（如果不存在）
        create_minute_table_if_not_exists(table_name, columns)

        # 插入数据到数据库表
        for _, row in minute_data.iterrows():
            # print(row)
            # datetime_str = row['日期'] + ' ' + row['时间']
            timestamp = int(datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S').timestamp())
            values = [timestamp,row['datetime'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['hold']]
            insert_sql = f'''
                INSERT OR IGNORE INTO {table_name} ( {', '.join(columns)})
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            cursor.execute(insert_sql, values)

        # 提交并关闭连接
        conn.commit()
        conn.close()
def fetch_and_store_daily_data(symbol):
    # 获取日线数据
    daily_data = ak.futures_zh_daily_sina(symbol=symbol)

    if not daily_data.empty:
        # 过滤掉表头
        daily_data = daily_data.iloc[1:]
        table_name = 'daily_data'
        columns = ['datetime','symbol', 'open', 'high', 'low', 'close', 'volume', 'hold']

        # 连接数据库
        conn = sqlite3.connect('futures_data.db')
        cursor = conn.cursor()

        # 创建表（如果不存在）
        create_daily_table_if_not_exists(table_name, columns)
        # 插入数据到数据库表
        for _, row in daily_data.iterrows():
            timestamp = int(datetime.strptime(row['date'], '%Y-%m-%d').timestamp())
            values = [timestamp, row['date'],symbol, row['open'], row['high'], row['low'], row['close'], row['volume'], row['hold']]
            insert_sql = f'''
                INSERT OR IGNORE INTO {table_name} (timestamp,{', '.join(columns)})
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            cursor.execute(insert_sql, values)
        print(f"Daily data for {symbol} fetched and stored at {datetime.now()},data length:{len(daily_data)}")
        # 提交并关闭连接
        conn.commit()
        conn.close()


def fetch_and_store_all_periods(symbol):
    # 创建数据库表
    # for period in [1, 5, 15, 30, 60]:
    #     create_minute_table_if_not_exists(f'sa2405_{period}_minute_data', ['datetime', 'open', 'high', 'low', 'close', 'volume', 'hold'])

    while True:
        current_time = datetime.now()

        # 使用多线程同时进行不同周期的数据采集
        threads = []
        for period, interval in [(1, 10), (5, 30), (15, 30), (30, 150), (60, 150)]:
            thread = threading.Thread(target=fetch_and_store_minute_data, args=(symbol,period))
            threads.append(thread)
            thread.start()
            print(f"Thread for {period}-minute data started at {current_time}. Waiting for {interval} minutes...")

        # 等待所有线程执行完成
        for thread, interval in zip(threads, [10, 30, 30, 150, 150]):
            thread.join()
            time.sleep(interval * 60 )  # 等待指定的时间，并添加一个小的随机值
            print(f"  {period}-minute data started at {current_time}. Waiting for {interval} minutes...")
        
        # time.sleep(600 + random.randint(1, 10))  # 等待10分钟，并添加一个小的随机值


def copy_data_from_sa2405_to_minute_data(db_file,table,newtable):
    try:
        # 连接到你的 SQLite 数据库
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # 从 SA2405_1_minute_data 表中读取数据
        cursor.execute(f"SELECT * FROM {table}")
        data_to_copy = cursor.fetchall()

        # 插入数据到 1_minute_data 表中，并赋值 symbol 为 'SA2405'
        for row in data_to_copy:
            cursor.execute(f"INSERT OR IGNORE INTO {newtable} (timestamp,datetime, symbol,open, high, low, close, volume, hold) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)",
                           (row[1], row[2], 'SA2405',row[3], row[4], row[5], row[6], row[7],row[8]))

        # 提交更改并关闭连接
        conn.commit()
        conn.close()
        
        print("数据复制成功！")

    except Exception as e:
        print(f"数据复制出现错误: {str(e)}")


if __name__ == "__main__":
    product = ['SA2405','SA2409','FG2405','JM2405']
    for p in product:
        fetch_and_store_daily_data(p)
        fetch_and_store_all_periods(p)
    # for period in [ 'daily']:
    #     copy_data_from_sa2405_to_minute_data("futures_data.db",f"sa2405_{period}_data",f"{period}_data")
    