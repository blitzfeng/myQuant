import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

# 步骤1: 加载数据
def load_data(db_name, table_name):
   # 建立数据库连接
    conn = sqlite3.connect(db_name)

    # 读取数据
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)

    # 关闭数据库连接
    conn.close()

    return df

# 步骤2: 计算布林线和趋势跟随指标
def calculate_indicators(df, window=20, trend_window=50):
    std_multiplier = 2
    df['SMA'] = df['close'].rolling(window=window).mean()
    df['STD'] = df['close'].rolling(window=window).std()
    df['Upper_Band'] = df['SMA'] + (df['STD'] * std_multiplier)
    df['Lower_Band'] = df['SMA'] - (df['STD'] * std_multiplier)
    # df['Trend_SMA'] = df['close'].rolling(window=trend_window).mean()
    # 计算指数移动平均（EMA）
    df['EMA5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['EMA10'] = df['close'].ewm(span=10, adjust=False).mean()
    return df

    # 添加趋势强度指标
def add_trend_strength_indicators(df):
    # 例如：计算14期RSI
    delta = df['close'].diff()
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0

    roll_up = up.rolling(window=14).mean()
    roll_down = down.abs().rolling(window=14).mean()

    RS = roll_up / roll_down
    df['RSI'] = 100.0 - (100.0 / (1.0 + RS))

    return df

def add_ma(df):
    # 计算移动平均线
    short_window = 5  # 短期窗口，例如5个时间间隔
    long_window = 15  # 长期窗口，例如15个时间间隔
    df['short_ma'] = df['close'].rolling(window=short_window).mean()
    df['long_ma'] = df['close'].rolling(window=long_window).mean()
    df['ma_diff'] = abs(df['short_ma'] - df['long_ma'])
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
    df['trend_profit'] = 0
    # 识别趋势
    df['trend'] = 0
    threshold = df['ma_diff'].rolling(window=30).mean()
    df.loc[df['short_ma'] > df['long_ma'], 'trend'] = 1
    df.loc[df['short_ma'] < df['long_ma'], 'trend'] = -1
    df.loc[df['ma_diff'] < threshold -2, 'trend'] = 2
    lossValue = 30
    global Trend_Break_Price  # 使用全局变量
    global open_price
    global stop_loss

    for i in range(1, len(df)):
        
        # 检查是否符合开仓条件
        if df['Position'][i-1] == 0 and df['Upper_Band'][i] - df['Lower_Band'][i] > 20:  # 如果之前没有持仓
            if df['close'][i] < df['Lower_Band'][i] and (df['trend'][i] == 1 or df['trend'][i] == 2):
                if abs(df['Upper_Band'][i] - df['Upper_Band'][i-1]) < 4 : # 趋势震荡中，低点做多
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

            elif df['close'][i] > df['Upper_Band'][i] and( df['trend'][i] == -1 or df['trend'][i] == 2):
                if abs(df['Lower_Band'][i-1] - df['Lower_Band'][i]) < 4 :
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
            # 检查趋势突破平仓条件
            elif Trend_Break_Price > 0 and df['high'][i] > Trend_Break_Price and df['high'][i] < df['Upper_Band'][i]:
                df['Position'][i] = 0
                df['Profit_Loss'][i] = num_contracts * (df['close'][i] - open_price ) - df['Commission'][i-1]
                df['value'][i] = df['close'][i] - open_price
                df['trend_profit'][i] = 1
                open_price = 0
                Trend_Break_Price = 0  # 重置趋势突破价格
            elif Trend_Break_Price == 0 and df['close'][i] >= df['Upper_Band'][i]:
                is_trend_break = (
                    abs(df['Upper_Band'][i] - df['Upper_Band'][i-1]) > 5
                )

                if is_trend_break:
                    Trend_Break_Price = df['close'][i]  # 更新趋势突破价格
                    df['Position'][i] = df['Position'][i-1]  # 继续持有
                else:
                    # 非趋势突破，直接平仓
                    df['Position'][i] = 0
                    df['Profit_Loss'][i] = num_contracts * (df['close'][i] - open_price) - df['Commission'][i-1]
                    df['value'][i] = df['close'][i] - open_price
                    open_price = 0
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
            elif Trend_Break_Price > 0 and df['EMA5'][i-1] <= df['EMA10'][i-1] and df['EMA5'][i] > df['EMA10'][i]:
                df['Position'][i] = 0
                df['Profit_Loss'][i] = num_contracts * (open_price - df['close'][i]) - df['Commission'][i-1]
                df['value'][i] = open_price - df['close'][i]
                Trend_Break_Price = 0  # 重置趋势突破价格
                open_price = 0
                df['trend_profit'][i] = 1
            elif Trend_Break_Price ==0 and df['close'][i] <= df['Lower_Band'][i]:
                # 检查趋势突破条件
                is_trend_break = (
                  abs( df['Lower_Band'][i - 1] - df['Lower_Band'][i]) > 5
                )

                if is_trend_break:
                    # 趋势突破，记录最低价并继续持有
                    Trend_Break_Price = df['low'][i]
                    df['Position'][i] = df['Position'][i-1]
                else:
                    # 非趋势突破，直接平仓
                    df['Position'][i] = 0
                    df['Profit_Loss'][i] = num_contracts * (open_price - df['close'][i]) - df['Commission'][i-1]
                    df['value'][i] = open_price - df['close'][i]
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
    sheet_name = "sa2405_15_minute_data"  # Excel中的sheet名称

    # 加载数据
    df = load_data(file_name, sheet_name)

    # 计算指标
    df = calculate_indicators(df)
    # 添加趋势强度指标
    # df = add_trend_strength_indicators(df)
    df = add_ma(df)

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
