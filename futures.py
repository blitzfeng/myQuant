import akshare as ak
import pandas as pd
import time





#获取所有品类的命名表 如白糖、焦煤等
futures_symbol_mark_df = ak.futures_symbol_mark()
print(futures_symbol_mark_df)
big_df = pd.DataFrame()
# for item in futures_symbol_mark_df:
#     print(item)
    # futures_zh_realtime_df = ak.futures_zh_realtime(symbol=item)
    # big_df = pd.concat([big_df, futures_zh_realtime_df], ignore_index=True)

# print(big_df)

# # 使用 akshare 获取期货合约历史数据
# df = ak.futures_zh_daily_sina(symbol="JM0")
# # # 添加日期列
# # df["date"] = df.index
# # 将数据写入CSV文件
# output_file = "jm_daily_data.csv"
# df.to_csv(output_file, index=False)

# # 打印数据的前几行
# print(df.head())
# print(f"数据已写入到文件: {output_file}")

# futures_zh_spot_df = ak.futures_zh_spot(symbol='JM2401', market="CF", adjust='0')
# print(futures_zh_spot_df)

# while True:
# futures_zh_minute_sina_df = ak.futures_zh_minute_sina(symbol="JM2401", period="60")
# print(futures_zh_minute_sina_df)
    # time.sleep(60)
