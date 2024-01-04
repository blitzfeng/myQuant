import sqlite3

# 连接数据库，如果不存在则创建
conn = sqlite3.connect('futures_data.db')
cursor = conn.cursor()

# 创建1分钟数据表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS _1_minute_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        datetime DATETIME,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        hold INTEGER
    )
''')

# 创建5分钟数据表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS _5_minute_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        datetime DATETIME,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        hold INTEGER
    )
''')

# 创建15分钟数据表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS _15_minute_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        datetime DATETIME,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        hold INTEGER
    )
''')

# 创建30分钟数据表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS _30_minute_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        datetime DATETIME,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        hold INTEGER
    )
''')

# 创建60分钟数据表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS _60_minute_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        datetime DATETIME,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        hold INTEGER
    )
''')

# 创建日线数据表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        datetime DATE,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        hold INTEGER
    )
''')

# 提交并关闭连接
conn.commit()
conn.close()
