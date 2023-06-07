import yfinance as yf
import numpy as np
import pandas as pd
from clickhouse_driver import connect
import json
import time

def get_data(ticker: str, period: str = '1h', interval: str = '1h'):
    data = pd.DataFrame()
    try:
        tick = yf.Ticker(ticker = ticker)
    except:
        print(f"Не возможно получить данные по {ticker}")
        return data, ''
    info = tick.get_info()
    history = tick.history(period=period, interval=interval).reset_index()
    name = ticker.upper()
    history['Currency'] = info['financialCurrency']
    history['Datetime'] = history['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    data = pd.concat([data, history])
    return data, name

def con(connection: dict):
    try:
        cur = connect(**connection).cursor()
        cur.execute('SELECT version();')
        if isinstance(cur.fetchone()[0], str):
            return cur
    except:
        print('Ошибка подключения')
        return None
    
def upload_to_ch(data: pd.DataFrame, name: str, cur):
    if len(data) == 0:
        return
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {name} (
    timestamp DateTime,
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume UInt32,
    dividents Float32,
    stock_split Float32,
    currency FixedString(10)
    )
    ENGINE = MergeTree 
    PARTITION BY timestamp
    ORDER BY timestamp
    ;""")
    values = np.array2string(data.values, separator=', ', precision=None)[1:-1].replace('\n', '').replace('[', '(').replace(']', ')')
    cur.execute(f"""
    INSERT INTO {name} VALUES {values}
                ;""")

def load_data(tickers: list, connection, period='1h', interval='5m'):
    cur = con(connection)
    for tick in tickers:
        upload_to_ch(*get_data(tick, period=period, interval=interval), cur=cur)

if __name__ == '__main__':
    with open('settings.json', 'r', encoding = 'utf8') as settings:
        settings = json.load(settings)
    connection = settings['connection']
    tickers = settings['tickers']
    period = settings['period']
    interval = settings['interval']
    if 's' in period:
        stime = int(period[:-1])
    elif 'mo' in period:
        stime = int(period[:-2]) * 2635200
    elif 'm' in period:
        stime = int(period[:-1]) * 60
    elif 'h' in period:
        stime = int(period[:-1]) * 3600
    elif 'd' in period:
        stime = int(period[:-1]) * 86400
    elif 'w' in period:
        stime = int(period[:-1]) * 604800
    elif 'y' in period:
        stime = int(period[:-1]) * 31536000
    while True:
        load_data(tickers=tickers, connection=connection, period=period, interval=interval)
        print(f"{time.ctime()} - Загружены: {tickers}")
        time.sleep(stime)
