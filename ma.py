import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

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

def add_ma(df):
    # 计算移动平均线
    short_window = 5  # 短期窗口，例如5个时间间隔
    long_window = 15  # 长期窗口，例如15个时间间隔
    df['short_ma'] = df['close'].rolling(window=short_window).mean()
    df['long_ma'] = df['close'].rolling(window=long_window).mean()
    df['ma_diff'] = abs(df['short_ma'] - df['long_ma'])
    return df

def add_ema(df):
    df['EMA6'] = df['close'].ewm(span=6, adjust=False).mean()
    df['EMA12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ma_diff'] = abs(df['EMA6'] - df['EMA12'])
    return df

Trend_Break_Price = 0
open_price = 0
stop_loss = 0


# 步骤3: 定义交易信号
def define_signals(df):
    num_contracts = 2  # 每次交易的手数（合约数量）
    commission_rate = 0.001  # 手续费率，例如0.1%
    df['Position'] = 0  # 交易仓位状态：1表示多头，-1表示空头，0表示无仓位
    df['Stop_Loss'] = 0  # 止损价格
    df['Open_Price'] = 0  # 开仓价格
    df['Profit_Loss'] = 0  # 平仓时的利润或亏损
    df['Commission'] = 0  # 手续费
    df['value'] = 0 #平仓和开仓的价差
    # df['trend_profit'] = 0
    # 识别趋势
    df['trend'] = 0
    threshold = df['ma_diff'].rolling(window=30).mean()
    df.loc[df['EMA6'] > df['EMA12'], 'trend'] = 1
    df.loc[df['EMA6'] < df['EMA12'], 'trend'] = -1
    df.loc[df['ma_diff'] < threshold -2, 'trend'] = 2
    lossValue = 30
    global Trend_Break_Price  # 使用全局变量
    global open_price
    global stop_loss

    for i in range(1, len(df)):
        
        # 检查是否符合开仓条件
        if df['Position'][i-1] == 0 :  # 如果之前没有持仓
            if df['trend'][i-1] == 2 and (df['trend'][i] == 1 ):
                # if abs(df['Upper_Band'][i] - df['Upper_Band'][i-1]) < 4 : # 趋势震荡中，低点做多
                    df['Position'][i] = 1  # 做多
                    df['Open_Price'][i] = df['close'][i]
                    open_price = df['Open_Price'][i]
                    df['Stop_Loss'][i] = df['close'][i] - lossValue
                    stop_loss = df['Stop_Loss'][i]
                # else : # 趋势突破，顺势操作
                #     df['Position'][i] = -1 # 做空
                #     df['Open_Price'][i] = df['close'][i]
                #     open_price = df['Open_Price'][i]
                #     df['Stop_Loss'][i] = df['close'][i] + lossValue
                #     stop_loss = df['Stop_Loss'][i]

            elif  df['trend'][i-1] == 2 and( df['trend'][i] == -1):
                # if abs(df['Lower_Band'][i-1] - df['Lower_Band'][i]) < 4 :
                    df['Position'][i] = -1  # 做空
                    df['Open_Price'][i] = df['close'][i]
                    open_price = df['Open_Price'][i]
                    df['Stop_Loss'][i] = df['close'][i] + lossValue
                    stop_loss = df['Stop_Loss'][i]
                # else :
                #     df['Position'][i] = 1  # 做多
                #     df['Open_Price'][i] = df['close'][i]
                #     open_price = df['Open_Price'][i]
                #     df['Stop_Loss'][i] = df['close'][i] - lossValue
                #     stop_loss = df['Stop_Loss'][i]

        # 持有多头仓位时的止损和止盈逻辑
        if df['Position'][i-1] == 1:
            # 止损条件
            if df['low'][i] <= stop_loss:
                df['Position'][i] = 0
                df['Profit_Loss'][i] = num_contracts * (stop_loss - open_price) - df['Commission'][i-1]
                df['value'][i] = -lossValue
                stop_loss = 0

            # 止盈条件
            
            elif df['trend'][i] == 2 and df['trend'][i-1] == 1:
                df['Position'][i] = 0
                df['Profit_Loss'][i] = num_contracts * (df['close'][i] - open_price ) - df['Commission'][i-1]
                df['value'][i] = df['close'][i] - open_price
                # df['trend_profit'][i] = 1
                open_price = 0
                # Trend_Break_Price = 0  # 重置趋势突破价格
            
            else:
                df['Position'][i] = 1  # 继续持有
        elif df['Position'][i-1] == -1:
            # 止损条件
            if df['high'][i] >= stop_loss:
                df['Position'][i] = 0
                df['Profit_Loss'][i] = num_contracts * ( open_price - stop_loss) - df['Commission'][i-1]
                stop_loss = 0
                df['value'][i] = -lossValue

            # 止盈条件
            # 检查趋势突破平仓条件
            elif  df['trend'][i] == 2 and df['trend'][i-1] == -1:
                df['Position'][i] = 0
                df['Profit_Loss'][i] = num_contracts * (open_price - df['close'][i]) - df['Commission'][i-1]
                df['value'][i] = open_price - df['close'][i]
                # Trend_Break_Price = 0  # 重置趋势突破价格
                open_price = 0
                # df['trend_profit'][i] = 1
            
            else:
                df['Position'][i] = -1  # 继续持有
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
    sheet_name = "_5_minute_data"  # Excel中的sheet名称
    symbol = 'JM2405'
    # 加载数据
    df = load_data(file_name, sheet_name,symbol)

    # df = add_ma(df)
    df = add_ema(df)

    # 定义信号
    df = define_signals(df)

    # 模拟交易
    portfolio = simulate_trading(df)

    df.to_excel("result.xlsx")

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


if __name__ == "__main__":
    main()