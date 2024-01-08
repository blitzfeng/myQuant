import akshare as ak
from datetime import datetime, timedelta
import time
import sqlite3

def data_correct(symbol, period):
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
            print(f"update:{flag} volume:{row['volume']}")
        else:
            insert_query = f"INSERT INTO {table_name} (timestamp,flag, datetime, symbol, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?,?);"
            cursor.execute(insert_query, (timestamp,row['flag'], row['datetime'], symbol, row['open'], row['high'], row['low'], row['close'], row['volume'],row['holde']))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    symbol = 'SA2409'
    period = 15
    data_correct(symbol, period)
    
    
    