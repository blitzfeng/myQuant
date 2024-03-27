import akshare as ak
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
import concurrent.futures

smsapi = "http://api.smsbao.com/"
user = "blitzfeng"
password = "61dedea83a334d0da3e299374aabf748"
phone = "18606531259"

isFirst = True
stop_loss_dict = {}
####################获取数据####################
def fetch_and_store_minute_data(symbol, period):
    try:
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
    except Exception as e:
        print(f"fetch_and_store_minute_data error")
def data_correct(symbol, period):
    try:
        # Connect to the database
        conn = sqlite3.connect('futures_data.db')
        cursor = conn.cursor()
        # Define the table name
        table_name = f'_{period}_minute_data'
        # Define the columns
        columns = ['timestamp', 'flag', 'datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']

        # Fetch the data
        data = ak.futures_zh_minute_sina(symbol=symbol, period=period).iloc[1:]

        # Check if data already exists in the table and update or insert accordingly
        for _, row in data.tail(20).iterrows():
            timestamp = int(datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S').timestamp())
            select_query = f"SELECT * FROM {table_name} WHERE symbol = ? AND timestamp = ?;"
            cursor.execute(select_query, (symbol,timestamp))
            # print(f"timestamp:{timestamp} row:{row}")
            flag = symbol + str(timestamp)
            existing_data = cursor.fetchone()
            # print(f"existing_data:{existing_data}")
            if existing_data:
                update_query = f"UPDATE {table_name} SET flag = ?, datetime = ?, open = ?, high = ?, low = ?, close = ?, volume = ? WHERE symbol = ? AND timestamp = ?;"
                cursor.execute(update_query, (flag, row['datetime'], row['open'], row['high'], row['low'], row['close'], row['volume'], symbol, timestamp))
                # print(f"update:{flag} volume:{row['volume']}")
            else:
                insert_query = f"INSERT INTO {table_name} (timestamp,flag, datetime, symbol, open, high, low, close, volume,hold) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?,?);"
                cursor.execute(insert_query, (timestamp,flag, row['datetime'], symbol, row['open'], row['high'], row['low'], row['close'], row['volume'],row['hold']))

        # Commit the changes and close the connection
        conn.commit()
        conn.close()
    except Exception as e:
        print("fetch_and_store_minute_data")


def fetch_data(symbol):
    # for symbol in symbols:
    period = 15
    fetch_and_store_minute_data(symbol, period)
    data_correct(symbol, period)
    #打印当前时间
    current_time = datetime.now()
    print(f"{current_time}已抓取{symbol}的{period}分钟数据")


def fetch_periodically(symbols):
        period = 15
        # while True:
        for symbol in symbols :
            fetch_and_store_minute_data(symbol, period)
            data_correct(symbol, period)
            #打印当前时间
            current_time = datetime.now()
            print(f"{current_time}已抓取{symbol}的{period}分钟数据")
            
            # time.sleep(60)  # Sleep for 1 minutes (900 seconds)

######################消费数据######################
            
open_price = 0 #开仓价格
#加载数据
def load_data(db_name, table_name, symbols):

    
    # 建立数据库连接
    conn = sqlite3.connect(db_name)

    # 读取数据,选取数据库中symbol = symbols且datetime到"2024-01-05 13:45:00"的数据并以timestamp排序,最新的数据在前  AND datetime <= '2024-01-05 13:45:00'
    query = f"SELECT * FROM {table_name} WHERE symbol='{symbols}'  ORDER BY timestamp DESC LIMIT 100"
    df = pd.read_sql_query(query, conn)

    # 关闭数据库连接
    conn.close()
    df = df.iloc[::-1]
    return df
def load_data_from_excel(file_path, symbols):
        if os.path.exists(file_path):
            df = pd.read_excel(file_path)
            df = df[df['symbol'] == symbols]
            df = df.sort_values('timestamp')
            return df
        else:
            return load_data('futures_data.db', 'daily_data', symbols)

       

def add_ema(df):
    df['EMA5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['EMA10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ma_diff'] = abs(df['EMA5'] - df['EMA10'])
    return df

# 步骤3: 定义交易信号
def define_signals(df,symbol):
    try:
        num_contracts = 4  # 每次交易的手数（合约数量）
        commission_rate = 0.002  # 手续费率，例如0.1%
        df['Position'] = 0  # 交易仓位状态：1表示多头，-1表示空头，0表示无仓位
        df['Stop_Loss'] = 0  # 止损价格
        df['Open_Price'] = 0  # 开仓价格
        df['Profit_Loss'] = 0.0  # 平仓时的利润或亏损
        df['value'] = 0 #平仓和开仓的价差
        # 识别趋势
        df['trend'] = 0
        threshold = df['ma_diff'].rolling(window=12).mean()
        # thresholdTemp = df['ma_diff'][::-1].rolling(window=30).mean()
        # threshold = thresholdTemp.iloc[::-1]
        df.loc[df['EMA5'] > df['EMA10'], 'trend'] = 1
        df.loc[df['EMA5'] < df['EMA10'], 'trend'] = -1
        df.loc[df['ma_diff'] < threshold -2 , 'trend'] = 2
        lossValue = 35
        global open_price

        # latestPosition = 0
        # i = latestPosition    
        
        for i in range(len(df)-2,-1,-1):
            if i == 0:
                print(f"time:{datetime.now()} symbol:{symbol},趋势:{df['trend'][i]}")
                sendToWechat(f"time:{datetime.now()} symbol:{symbol},趋势:{df['trend'][i]}")
            # print(f"{df.loc[i,'datetime']},i:{i}")
            # 检查是否符合开仓条件
            if df['Position'][i+1] == 0 :  # 如果之前没有持仓
                if (df['trend'][i+1] == 2 or df['trend'][i+1] == -1)and (df['trend'][i] == 1 ):
                        # print(f"做多{df.loc[i,'datetime']},i:{i}")
                        df.loc[i,'Position'] = 1  # 做多
                        df.loc[i,'Open_Price'] = df['close'][i]
                        open_price = df['Open_Price'][i]
                        df.loc[i,'Stop_Loss'] = df['close'][i] - lossValue
                        stop_loss_dict[symbol] = df['Stop_Loss'][i]
                        if i == 0:
                            print("做多开仓")
                            send_notification("交易提醒", f"触发{symbol}做多开仓条件,价格：{df['close'][i]}")

                elif (df['trend'][i+1] == 2 or df['trend'][i+1] == 1)and( df['trend'][i] == -1):
                        
                        df.loc[i,'Position'] = -1
                        df.loc[i,'Open_Price'] = df['close'][i]
                        open_price = df['Open_Price'][i]
                        df.loc[i,'Stop_Loss'] = df['close'][i] + lossValue
                        stop_loss_dict[symbol] = df['Stop_Loss'][i]
                        if i == 0:
                            print("做空开仓")
                            send_notification("交易提醒", f"触发{symbol}做空开仓条件,价格：{df['close'][i]}")
            # 持有多头仓位时的止损和止盈逻辑
            if df['Position'][i+1] == 1:
                # 止损条件
                if df['low'][i] <= stop_loss_dict[symbol]:
                    df.loc[i,'Position'] = 0
                    df.loc[i,'Commission'] = 6000 * num_contracts * commission_rate
                    df.loc[i,'Profit_Loss'] = num_contracts * (stop_loss_dict[symbol] - open_price)*20 - df['Commission'][i]
                    df.loc[i,'value'] = -lossValue
                    if i == 0:
                        send_notification("交易提醒", f"触发多头{symbol}止损平仓条件,价格：{stop_loss_dict[symbol]}")
                    stop_loss_dict[symbol] = 0

            # 止盈条件
                elif df['trend'][i] == 2 and df['trend'][i+1] == 1:
                        df.loc[i,'Position'] = 0
                        df.loc[i,'Commission'] = 6000 * num_contracts * commission_rate
                        df.loc[i,'Profit_Loss'] = num_contracts * (df['close'][i] - open_price )*20 - df['Commission'][i]
                        df.loc[i,'value'] = df['close'][i] - open_price
                        if i == 0:
                            send_notification("交易提醒", f"触发多头{symbol}止盈平仓条件,价格：{df['close'][i]}")
                        open_price = 0
            
                else:
                    df.loc[i,'Position'] = 1  # 继续持有
            elif df['Position'][i+1] == -1:
                # 止损条件
                if df['high'][i] >= stop_loss_dict[symbol]:
                    df.loc[i,'Position'] = 0
                    df.loc[i,'Commission'] = 6000 *  num_contracts * commission_rate
                    df.loc[i,'Profit_Loss'] = num_contracts * ( open_price - stop_loss_dict[symbol])*20 - df['Commission'][i]
                    df.loc[i,'value'] = -lossValue
                    if i == 0:
                        send_notification("交易提醒", f"触发空头{symbol}止损平仓条件,价格：{stop_loss_dict[symbol]}")
                    stop_loss_dict[symbol] = 0
                # 止盈条件
                # 检查趋势突破平仓条件
                elif  df['trend'][i] == 2 and df['trend'][i+1] == -1:
                    df.loc[i,'Position'] = 0
                    df.loc[i,'Commission'] = 6000 *  num_contracts * commission_rate
                    df.loc[i,'Profit_Loss'] = num_contracts * (open_price - df['close'][i])*20 - df['Commission'][i]
                    df.loc[i,'value'] = open_price - df['close'][i]
                    open_price = 0
                    if i == 0:
                        send_notification("交易提醒", f"触发空头{symbol}止盈平仓条件,价格：{df['close'][i]}")
                
                else:
                    df.loc[i,'Position'] = -1# 继续持有
    except Exception as e:
        print(f"define_signals error")
    return df
def sendToWechat(message):
    token = 'd6bea3335df8461d9a64d78a2162878d'#前边复制到那个token
    title = message
    content = message
    template = 'txt'
    channel = 'wechat'
    url = f"https://www.pushplus.plus/send?token={token}&title={title}&content={content}&template={template}&channel={channel}"
    requests.get(url=url)

#系统通知
def send_notification(title, message):
    os_name = platform.system()

    if os_name == 'Darwin':  # macOS
         #交互式通知
        # script = f'''
        # tell application "System Events"
        #     display dialog "{message}" with title "{title}" buttons ["OK"] default button "OK"
        # end tell
        # '''
        # subprocess.run(["osascript", "-e", script])
        script = f'display notification "{message}" with title "{title}"'
        subprocess.run(["osascript", "-e", script])
    elif os_name == 'Windows':  # Windows
        MessageBox = ctypes.windll.user32.MessageBoxW
        MessageBox(None, message, title, 0)
   
    smsToPhone(message)
    # script = f'display notification "{message}" with title "{title}"'
    # subprocess.run(["osascript", "-e", script])
def smsToPhone(message):
    token = 'd6bea3335df8461d9a64d78a2162878d'#前边复制到那个token
    title = message
    content = message
    template = 'txt'
    channel = 'sms'
    url = f"https://www.pushplus.plus/send?token={token}&title={title}&content={content}&template={template}&channel={channel}"
    print(url)
    r = requests.get(url=url)
    print(r.text)
def startHandleData(symbols):
    # while True:
        if is_specific_minute():
           #打印当前时间
            # current_time = datetime.now()
            # print(f"{current_time}开始处理数据")
           for symbol in symbols:
            handleData(symbol)
            
        # else:
        #     # 等待下一次调用
        #     time.sleep(60)  # 等待60秒，即1分钟

def handleData(symbol):
    if not is_specific_minute():
        return
    global isFirst
    # 步骤1: 加载数据
    db_name = 'futures_data.db'
    table_name = '_15_minute_data'
    #判断是否是首次调用
    # if isFirst:
    #     df = load_data_from_excel(f"{symbol}_test.xlsx",symbol)
    #     isFirst = False
    # else:
    df = load_data(db_name, table_name,symbol)

    supportLevel(df,symbol)

    # 步骤2: 计算指标
    df = add_ema(df)

    # 步骤3: 定义交易信号
    df = define_signals(df,symbol)
    # print(df)
    df.to_excel(f"{symbol}_test.xlsx")

def supportLevel(df,symbol):
    high_price = df['high'].max()
    low_price = df['low'].min()
    print(f"symbol:{symbol}  最高价：{high_price}")
    print(f"symbol:{symbol}  最低价：{low_price}")
    fibonacci_levels = [0.092,0.236,0.382, 0.5, 0.618,0.764,0.908]

    for level in fibonacci_levels:
        fib_price = low_price + (high_price - low_price) * level
        print(f"Fibonacci {level*100}% level: {fib_price}")
    
def is_specific_minute():
    current_minute = datetime.now().minute
    return current_minute in [14, 29, 44, 59]
def is_market_open():
    temp_current_time = datetime.now().strftime('%H:%M')
    current_time = datetime.strptime(temp_current_time, '%H:%M')
    market_open_1 = datetime.strptime('08:59', '%H:%M')
    market_close_1 = datetime.strptime('11:30', '%H:%M')
    market_open_2 = datetime.strptime('13:29', '%H:%M')
    market_close_2 = datetime.strptime('15:00', '%H:%M')
    market_open_3 = datetime.strptime('20:59', '%H:%M')
    market_close_3 = datetime.strptime('23:00', '%H:%M')

    if (market_open_1 <= current_time <= market_close_1) or (market_open_2 <= current_time <= market_close_2) or (market_open_3 <= current_time <= market_close_3):
        return True
    else:
        return False

def is_market_close():
    current_time = datetime.now().time()
    return  current_time.hour >= 15 and current_time.hour < 20

def is_today_market_break():
    temp_current_time = datetime.now().strftime('%H:%M')
    current_time = datetime.strptime(temp_current_time, '%H:%M')
    market_break = datetime.strptime('11:30', '%H:%M')
    market_break1 = datetime.strptime('13:20', '%H:%M')
    market_break2 = datetime.strptime('23:00', '%H:%M')
    market_break3 = datetime.strptime('00:00', '%H:%M')
    market_break4 = datetime.strptime('08:40', '%H:%M')
    if market_break <= current_time <= market_break1 or market_break2 <= current_time <= market_break3 or market_break3 <= current_time <= market_break4:
        return True
    else:
        return False

market_prepare = False

if __name__ == "__main__":
    product = ['SA2409', 'FG2405']
    while True:
        if is_market_open():
            with concurrent.futures.ThreadPoolExecutor() as executor:
                [executor.submit(fetch_data, p) for p in product]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                [executor.submit(handleData, p) for p in product]
            market_prepare = False
            time.sleep(60)
        elif is_market_close() and not market_prepare:
            #调用另一个python脚本 fetch_data.py
            os.system('python fetch_data.py')
            print("休盘中...")
            time.sleep(60 * 60)
        elif datetime.now().time().hour == 20 or datetime.now().time().hour == 8:
            print("准备开盘")
            market_prepare = True 
            time.sleep(60)
        elif is_today_market_break():
            print("中场休息")
            time.sleep(60 * 30)
        else :
            time.sleep(60)

    # dataThread = threading.Thread(target=fetch_data_periodically, args=(product,))
    # dataThread.start()

    # handleDataThread = threading.Thread(target=start_handle_data_periodically, args=(product,))
    # handleDataThread.start()
   