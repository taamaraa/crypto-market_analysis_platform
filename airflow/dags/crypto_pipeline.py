from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json
import time
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

DB_CONN = {
    'host': 'raw-db',
    'port': 5432,
    'dbname': 'raw_data',
    'user': 'admin',
    'password': 'admin'
}

COINS = ['bitcoin', 'ethereum', 'solana', 'binancecoin', 'cardano']
BINANCE_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'ADAUSDT']
ALPHA_SYMBOLS = ['TSLA']
ALPHA_API_KEY = os.getenv("ALPHA_API_KEY")


def fetch_coingecko():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    for coin in COINS:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        params = {"vs_currency": "usd", "days": 1, "interval": "daily"}

        response = requests.get(url, params=params)
        data = response.json()

        if "prices" not in data:
            continue

        for price_entry, market_entry, volume_entry in zip(
            data['prices'], data['market_caps'], data['total_volumes']
        ):
            ts = price_entry[0]
            date_val = datetime.fromtimestamp(ts / 1000).date()

            cur.execute("""
                INSERT INTO public.airflow_coingecko_daily
                    (coin_id, date, price, market_cap, total_volume, updated_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (coin_id, date) DO UPDATE SET
                    price = EXCLUDED.price,
                    market_cap = EXCLUDED.market_cap,
                    total_volume = EXCLUDED.total_volume,
                    updated_at = now()
            """, (
                coin,
                date_val,
                price_entry[1],
                market_entry[1],
                volume_entry[1]
            ))

        time.sleep(3)

    conn.commit()
    cur.close()
    conn.close()


def fetch_binance():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    for symbol in BINANCE_SYMBOLS:
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1d",
            "limit": 30
        }

        response = requests.get(url, params=params)
        data = response.json()

        if not isinstance(data, list):
            continue

        for kline in data:
            open_time_ms = int(kline[0])
            date_val = datetime.fromtimestamp(open_time_ms / 1000).date()

            cur.execute("""
                INSERT INTO public.airflow_binance_daily
                    (symbol, date, open_price, high_price, low_price, close_price,
                     volume, close_time, quote_volume, number_of_trades,
                     taker_buy_base_vol, taker_buy_quote_vol, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    close_time = EXCLUDED.close_time,
                    quote_volume = EXCLUDED.quote_volume,
                    number_of_trades = EXCLUDED.number_of_trades,
                    taker_buy_base_vol = EXCLUDED.taker_buy_base_vol,
                    taker_buy_quote_vol = EXCLUDED.taker_buy_quote_vol,
                    updated_at = now()
            """, (
                symbol,
                date_val,
                float(kline[1]),
                float(kline[2]),
                float(kline[3]),
                float(kline[4]),
                float(kline[5]),
                int(kline[6]),
                float(kline[7]),
                int(kline[8]),
                float(kline[9]),
                float(kline[10])
            ))

        time.sleep(1)

    conn.commit()
    cur.close()
    conn.close()


def fetch_alpha_vantage():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    for symbol in ALPHA_SYMBOLS:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": ALPHA_API_KEY
        }

        response = requests.get(url, params=params)
        data = response.json()

        time_series = data.get('Time Series (Daily)', {})

        if not time_series:
            continue

        for date_str, values in time_series.items():
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            except:
                continue

            cur.execute("""
                INSERT INTO public.airflow_alpha_vantage
                    (symbol, date, open, high, low, close, volume, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    updated_at = now()
            """, (
                symbol,
                date_obj,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume'])
            ))

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task_coingecko = PythonOperator(
        task_id="fetch_coingecko",
        python_callable=fetch_coingecko
    )

    task_binance = PythonOperator(
        task_id="fetch_binance",
        python_callable=fetch_binance
    )

    task_alpha = PythonOperator(
        task_id="fetch_alpha_vantage",
        python_callable=fetch_alpha_vantage
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="""
        cd /opt/airflow/dbt/my_dbt_project &&
        dbt run 2>&1 | tee /tmp/dbt.log
        """
    )

    [task_coingecko, task_binance, task_alpha] >> run_dbt