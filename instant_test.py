import akshare as ak
import sqlite3
from datetime import datetime, timedelta
import time
import threading
import subprocess
import pandas as pd


####################获取数据####################
def fetch_and_store_minute_data(symbol, period):
    # 获取分时数据
    minute_data = ak.futures_zh_minute_sina(symbol=symbol, period=period)

    if not minute_data.empty:
        # 过滤掉表头
        minute_data = minute_data.iloc[1:]

        table_name = f'_{period}_minute_data'
        columns = ['datetime', 'symbol','open', 'high', 'low', 'close', 'volume', 'hold']

        # 连接数据库
        conn = sqlite3.connect('futures_data.db')
        cursor = conn.cursor()


        # 插入数据到数据库表
        for _, row in minute_data.iterrows():
            # print(row)
            # datetime_str = row['日期'] + ' ' + row['时间']
            timestamp = int(datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S').timestamp())
            flag = symbol + str(timestamp)
            values = [timestamp,flag,row['datetime'],symbol, row['open'], row['high'], row['low'], row['close'], row['volume'], row['hold']]
            insert_sql = f'''
                INSERT OR IGNORE INTO {table_name} (timestamp,flag, {', '.join(columns)})
                VALUES (?, ?, ?, ?, ?, ?, ?, ?,?,?)
            '''
            cursor.execute(insert_sql, values)

        # 提交并关闭连接
        conn.commit()
        conn.close()
# def fetch_and_store_daily_data(symbol):
#     # 获取日线数据
#     daily_data = ak.futures_zh_daily_sina(symbol=symbol)

#     if not daily_data.empty:
#         # 过滤掉表头
#         daily_data = daily_data.iloc[1:]
#         table_name = 'daily_data'
#         columns = ['datetime','symbol', 'open', 'high', 'low', 'close', 'volume', 'hold']

#         # 连接数据库
#         conn = sqlite3.connect('futures_data.db')
#         cursor = conn.cursor()

        
#         # 插入数据到数据库表
#         for _, row in daily_data.iterrows():
#             timestamp = int(datetime.strptime(row['date'], '%Y-%m-%d').timestamp())
#             flag = symbol + str(timestamp)
#             values = [timestamp, flag,row['date'],symbol, row['open'], row['high'], row['low'], row['close'], row['volume'], row['hold']]
#             insert_sql = f'''
#                 INSERT OR IGNORE INTO {table_name} (timestamp,flag,{', '.join(columns)})
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?)
#             '''
#             cursor.execute(insert_sql, values)
#         print(f"Daily data for {symbol} fetched and stored at {datetime.now()},data length:{len(daily_data)}")
#         # 提交并关闭连接
#         conn.commit()
#         conn.close()




def fetch_periodically(symbol):
        while True:
            for period in [1, 5, 15, 30, 60] :
                fetch_and_store_minute_data(symbol, period)
                print(f"已抓取{symbol}的{period}分钟数据")
            handleData(symbol)
            time.sleep(900)  # Sleep for 15 minutes (900 seconds)

######################消费数据######################
            
open_price = 0 #开仓价格
stop_loss = 0 #止损价格
#加载数据
def load_data(db_name, table_name,symbols):
   # 建立数据库连接
    conn = sqlite3.connect(db_name)

    # 读取数据,选取数据库中symbol = symbols 的数据并以timestamp排序,最新的数据在前
    query = f"SELECT * FROM {table_name} where symbol='{symbols}'  ORDER BY timestamp DESC LIMIT 20"
    df = pd.read_sql_query(query, conn)

    # 关闭数据库连接
    conn.close()

    return df

def add_ema(df):
    df['EMA5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['EMA10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ma_diff'] = abs(df['EMA5'] - df['EMA10'])
    return df

# 步骤3: 定义交易信号
def define_signals(df,symbol):
    num_contracts = 2  # 每次交易的手数（合约数量）
    commission_rate = 0.002  # 手续费率，例如0.1%
    df['Position'] = 0  # 交易仓位状态：1表示多头，-1表示空头，0表示无仓位
    df['Stop_Loss'] = 0  # 止损价格
    df['Open_Price'] = 0  # 开仓价格
    df['Profit_Loss'] = 0  # 平仓时的利润或亏损
    df['value'] = 0 #平仓和开仓的价差
    # 识别趋势
    df['trend'] = 0
    threshold = df['ma_diff'].rolling(window=30).mean()
    df.loc[df['EMA5'] > df['EMA10'], 'trend'] = 1
    df.loc[df['EMA5'] < df['EMA10'], 'trend'] = -1
    df.loc[df['ma_diff'] < threshold -2 , 'trend'] = 2
    lossValue = 30
    global open_price
    global stop_loss

    latestPosition = 1
    i = latestPosition    
    # 检查是否符合开仓条件
    if df['Position'][i+1] == 0 :  # 如果之前没有持仓
        if df['trend'][i+1] == 2 and (df['trend'][i] == 1 ):
                df.loc[i,'Position'] = 1  # 做多
                df.loc[i,'Open_Price'] = df['close'][i]
                open_price = df['Open_Price'][i]
                df.loc[i,'Stop_Loss'] = df['close'][i] - lossValue
                stop_loss = df['Stop_Loss'][i]
                send_notification("交易提醒", f"触发{symbol}做多开仓条件,价格：{df['close'][i]}")

        elif  df['trend'][i+1] == 2 and( df['trend'][i] == -1):
                df.loc[i,'Position'] = -1
                df.loc[i,'Open_Price'] = df['close'][i]
                open_price = df['Open_Price'][i]
                df.loc[i,'Stop_Loss'] = df['close'][i] + lossValue
                stop_loss = df['Stop_Loss'][i]
                send_notification("交易提醒", f"触发{symbol}做空开仓条件,价格：{df['close'][i]}")
    # 持有多头仓位时的止损和止盈逻辑
    if df['Position'][i+1] == 1:
        # 止损条件
        if df['low'][i] <= stop_loss:
            df.loc[i,'Position'] = 0
            df.loc[i,'Commission'] = df['close'][i] * num_contracts * commission_rate
            df.loc[i,'Profit_Loss'] = num_contracts * (stop_loss - open_price) - df['Commission'][i]
            df.loc[i,'value'] = -lossValue
            stop_loss = 0
            send_notification("交易提醒", f"触发{symbol}止损平仓条件,价格：{stop_loss}")

        # 止盈条件
        elif df['trend'][i] == 2 and df['trend'][i+1] == 1:
                df.loc[i,'Position'] = 0
                df.loc[i,'Commission'] = df['close'][i] * num_contracts * commission_rate
                df.loc[i,'Profit_Loss'] = num_contracts * (df['close'][i] - open_price ) - df['Commission'][i]
                df.loc[i,'value'] = df['close'][i] - open_price
                send_notification("交易提醒", f"触发{symbol}止盈平仓条件,价格：{df['close'][i]}")
                open_price = 0
        
        else:
           df.loc[i,'Position'] = 1  # 继续持有
    elif df['Position'][i+1] == -1:
        # 止损条件
        if df['high'][i] >= stop_loss:
            df.loc[i,'Position'] = 0
            df.loc[i,'Commission'] = df['close'][i] * num_contracts * commission_rate
            df.loc[i,'Profit_Loss'] = num_contracts * ( open_price - stop_loss) - df['Commission'][i]
            stop_loss = 0
            df.loc[i,'value'] = -lossValue
            send_notification("交易提醒", f"触发{symbol}止损平仓条件,价格：{stop_loss}")

        # 止盈条件
        # 检查趋势突破平仓条件
        elif  df['trend'][i] == 2 and df['trend'][i+1] == -1:
            df.loc[i,'Position'] = 0
            df.loc[i,'Commission'] = df['close'][i] * num_contracts * commission_rate
            df.loc[i,'Profit_Loss'] = num_contracts * (open_price - df['close'][i]) - df['Commission'][i]
            df.loc[i,'value'] = open_price - df['close'][i]
            open_price = 0
            send_notification("交易提醒", f"触发{symbol}止盈平仓条件,价格：{df['close'][i]}")
        
        else:
            df.loc[i,'Position'] = -1# 继续持有
    return df

#系统通知
def send_notification(title, message):
    #交互式通知
    # script = f'''
    # tell application "System Events"
    #     display dialog "{message}" with title "{title}" buttons ["OK"] default button "OK"
    # end tell
    # '''
    # subprocess.run(["osascript", "-e", script])
    script = f'display notification "{message}" with title "{title}"'
    subprocess.run(["osascript", "-e", script])

def handleData(symbol):
    # 步骤1: 加载数据
    db_name = 'futures_data.db'
    table_name = '_15_minute_data'
    df = load_data(db_name, table_name,symbol)

    # 步骤2: 计算指标
    df = add_ema(df)

    # 步骤3: 定义交易信号
    df = define_signals(df,symbol)
    print(df)
    df.to_excel('test.xlsx')

def start(symbol):
    # 创建数据库表
    # for period in [1, 5, 15, 30, 60]:
    #     create_minute_table_if_not_exists(f'sa2405_{period}_minute_data', ['datetime', 'open', 'high', 'low', 'close', 'volume', 'hold'])

    # 开启一个线程
    thread = threading.Thread(target=fetch_periodically, args=(symbol,))
    thread.start()   

if __name__ == "__main__":
    product = ['SA2409','FG2405']
    for i in product:
        start(i)