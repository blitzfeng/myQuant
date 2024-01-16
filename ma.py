# 使用历史数据回测均线策略
#
#
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import subprocess

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

# 步骤1: 加载数据
def load_data(db_name, table_name,symbols):
   # 建立数据库连接
    conn = sqlite3.connect(db_name)

    # 读取数据,选取数据库中symbol = symbols 的数据并以timestamp排序
    query = f"SELECT * FROM {table_name} where symbol='{symbols}'  ORDER BY timestamp"
    df = pd.read_sql_query(query, conn)

    # 关闭数据库连接
    conn.close()

    return df
# 日线趋势
def add_ma():
    daily_df = load_data("futures_data.db", "daily_data", symbol)

    # 计算移动平均线
    short_window = 5  # 短期窗口，例如5个时间间隔
    long_window = 10  # 长期窗口，例如15个时间间隔
    daily_df['short_ma'] = daily_df['close'].rolling(window=short_window).mean()
    daily_df['long_ma'] = daily_df['close'].rolling(window=long_window).mean()
    daily_df['ma_diff'] = abs(daily_df['short_ma'] - daily_df['long_ma'])
    daily_df['daily_trend'] = 0
    threshold = daily_df['ma_diff'].rolling(window=12).mean()
    daily_df.loc[daily_df['short_ma'] > daily_df['long_ma'], 'daily_trend'] = 1
    daily_df.loc[daily_df['short_ma'] < daily_df['long_ma'], 'daily_trend'] = -1
    daily_df.loc[daily_df['ma_diff'] < threshold -2 , 'daily_trend'] = 2
    return daily_df
# 日内买入信号
def add_ema(df):
    df['EMA5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['EMA10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ma_diff'] = abs(df['EMA5'] - df['EMA10'])
    return df

def set_daily_trend(df,daily_df):
    df['daily_trend'] = 0
    
    df['datetimes'] = pd.to_datetime(df['datetime'])
    daily_df['datetime'] = pd.to_datetime(daily_df['datetime'])
    # 把df的datetime转换为日期格式（去除时分秒）
    df['date'] = df['datetimes'].dt.date

    
    # 转换daily_df的datetime为日期格式，并设置为索引
    daily_df['date'] = daily_df['datetime'].dt.date
    daily_df.set_index('date', inplace=True)
    # 对df的每一行执行赋值操作
    # print(daily_df.index)
    df['daily_trend'] = df['date'].map(daily_df['daily_trend'])
    # for i, row in df.iterrows():
    #     # 如果该日期在daily_df中存在，则赋值
        
    #     if row['date'] in daily_df.index:
    #         # print(f"in index{row['date']}")
    #         df.at[i, 'daily_trend'] = daily_df.at[row['date'], 'daily_trend']
    #     else:
    #         # print(f"not in index{row['date']}")
    #         df.at[i, 'daily_trend'] = None  # 或者可以选择其他默认值

    # 删除辅助列
    df.drop(columns=['date'], inplace=True)
    df.drop(columns=['datetimes'], inplace=True)

    return df


Trend_Break_Price = 0
open_price = 0
stop_loss = 0


# 步骤3: 定义交易信号
def define_signals(df):
    num_contracts = 4  # 每次交易的手数（合约数量）
    commission_rate = 0.002  # 手续费率，例如0.1%
    df['Position'] = 0  # 交易仓位状态：1表示多头，-1表示空头，0表示无仓位
    df['Stop_Loss'] = 0  # 止损价格
    df['Open_Price'] = 0  # 开仓价格
    df['Profit_Loss'] = 0.0  # 平仓时的利润或亏损
    df['Commission'] = 0.0  # 手续费
    df['value'] = 0 #平仓和开仓的价差
    # df['trend_profit'] = 0
    # 识别趋势
    df['trend'] = 0
    threshold = df['ma_diff'].rolling(window=12).mean()
    df.loc[df['EMA5'] > df['EMA10'], 'trend'] = 1
    df.loc[df['EMA5'] < df['EMA10'], 'trend'] = -1
    df.loc[df['ma_diff'] < threshold - 2 , 'trend'] = 2
    lossValue = 35
    global Trend_Break_Price  # 使用全局变量
    global open_price
    global stop_loss
    df['trend_avg'] = df['trend'].rolling(window=2).mean()
    open_count = 1
    for i in range(1, len(df)):
        
        # 检查是否符合开仓条件
        if df['Position'][i-1] == 0 :  # 如果之前没有持仓
            # if df['daily_trend'][i] == 1 or df['daily_trend'][i] == 2:# 如果日线趋势为多头或震荡
                if (df['trend'][i-1] == 2 ) and df['trend'][i] == 1 : # 如果日内买入信号为多头  or df['trend'][i-1] == -1
                        df.loc[i,'Position'] = 1  # 做多
                        df.loc[i,'Open_Price'] = df['close'][i]
                        open_price = df['Open_Price'][i]
                        df.loc[i,'Stop_Loss'] = df['close'][i] - lossValue
                        stop_loss = df['Stop_Loss'][i]
                        send_notification("交易提醒", "触发做多开仓条件")
                        open_count = open_count + 1

            # elif  df['daily_trend'][i] == -1 or df['daily_trend'][i] == 2: # 如果日线趋势为空头或震荡
                elif (df['trend'][i-1] == 2 )and( df['trend'][i] == -1):#or df['trend'][i-1] == 1
                    df.loc[i,'Position'] = -1
                    df.loc[i,'Open_Price'] = df['close'][i]
                    open_price = df['Open_Price'][i]
                    df.loc[i,'Stop_Loss'] = df['close'][i] + lossValue
                    stop_loss = df['Stop_Loss'][i]
                    send_notification("交易提醒", "触发做空开仓条件")
                    open_count = open_count + 1

        # 持有多头仓位时的止损和止盈逻辑
        if df.loc[i-1,'Position'] == 1:
            # 止损条件
            if df['low'][i] <= stop_loss:
                df.loc[i,'Position'] = 0
                df.loc[i,'Commission'] =6000 * num_contracts * commission_rate
                df.loc[i,'Profit_Loss'] = num_contracts * (stop_loss - open_price) *20 - df['Commission'][i]
                df.loc[i,'value'] = -lossValue
                stop_loss = 0
                send_notification("交易提醒", "触发止损平仓条件")

            # 止盈条件
            
            elif df['trend'][i] == 2 and df['trend'][i-1] == 1:
                df.loc[i,'Position'] = 0
                df.loc[i,'Commission'] = 6000 * num_contracts * commission_rate
                df.loc[i,'Profit_Loss'] = num_contracts * (df['close'][i] - open_price )*20 - df['Commission'][i]
                df.loc[i,'value'] = df['close'][i] - open_price
                send_notification("交易提醒", "触发止盈平仓条件")
                open_price = 0
            
            else:
                df.loc[i,'Position'] = 1  # 继续持有
        elif df.loc[i-1,'Position'] == -1:
            # 止损条件
            if df['high'][i] >= stop_loss:
                df.loc[i,'Position'] = 0
                df.loc[i,'Commission'] = 6000 * num_contracts * commission_rate
                df.loc[i,'Profit_Loss'] = num_contracts * ( open_price - stop_loss)*20 - df['Commission'][i]
                stop_loss = 0
                df.loc[i,'value'] = -lossValue
                send_notification("交易提醒", "触发止损平仓条件")

            # 止盈条件
            # 检查趋势突破平仓条件
            elif  df['trend'][i] == 2 and df['trend'][i-1] == -1:
                df.loc[i,'Position'] = 0
                df.loc[i,'Commission'] = 6000 * num_contracts * commission_rate
                df.loc[i,'Profit_Loss'] = num_contracts * (open_price - df['close'][i])*20 - df['Commission'][i]
                df.loc[i,'value'] = open_price - df['close'][i]
                open_price = 0
                send_notification("交易提醒", "触发止盈平仓条件")
            
            else:
                df.loc[i,'Position'] = -1# 继续持有
    print(f"开仓次数{open_count}")
    return df


# 步骤4: 模拟交易
def simulate_trading(df, initial_capital=50000.0, margin_per_contract=8000, transaction_fee_rate=0.0001):
    # df.index = pd.to_datetime(df['datetime'])
    # print(df)
    positions = pd.DataFrame(index=df.index).fillna(0.0)
    portfolio = pd.DataFrame(index=df.index).fillna(0.0)

    positions['Asset'] = df['Position']  # 记录每日的持仓状态
    portfolio['positions'] = positions.multiply(df['close'], axis=0)  # 计算持仓价值
    portfolio['total'] = initial_capital  # 总资金初始化
    portfolio['cash'] = initial_capital  # 可用资金初始化

    for i in range(1, len(portfolio)):
        # 计算保证金要求
        margin_requirement = abs(portfolio['positions'][i]) * margin_per_contract
        # 如果保证金要求不超过80%的可用资金
        if margin_requirement <= portfolio['cash'][i-1] * 0.8:
            # 计算手续费
            transaction_fee = abs(portfolio['positions'][i] - portfolio['positions'][i-1]) * transaction_fee_rate
            # 更新可用资金
            portfolio['cash'][i] = portfolio['cash'][i-1] - transaction_fee
            # 更新总资金
            portfolio['total'][i] = portfolio['cash'][i] + portfolio['positions'][i]
        else:
            # 若超过资金使用率，保持前一天的总资金
            portfolio['total'][i] = portfolio['total'][i-1]
            positions['Asset'][i] = 0  # 无法开仓

    return portfolio

# 主程序
def main():
    file_name = "futures_data.db"  # Excel文件路径
    sheet_name = "_15_minute_data"  # Excel中的sheet名称
    
    # 加载数据
    df = load_data(file_name, sheet_name,symbol)

    # 
    df = add_ema(df)

    daily_df = add_ma()

    df = set_daily_trend(df,daily_df)

    # 定义信号
    df = define_signals(df)

    # 模拟交易
    portfolio = simulate_trading(df)

    df.to_excel("result.xlsx")
    print("计算完毕")

    # df.index = pd.to_datetime("datetime")  # 确保索引是日期时间格式
    df['Cumulative_Profit'] = df['Profit_Loss'].cumsum()

    # 绘制资金曲线
    plt.figure(figsize=(10, 6))
    plt.plot(df['datetime'], df['Cumulative_Profit'], label='Cumulative Profit')
    plt.xlabel('Date')
    plt.ylabel('Profit')
    plt.title('Cumulative Profit Over Time')
    plt.legend()
    plt.show()

symbol = 'SA2409'

if __name__ == "__main__":
    main()
