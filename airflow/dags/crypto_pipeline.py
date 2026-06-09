from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json

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
ALPHA_SYMBOL = 'TSLA'
ALPHA_API_KEY = 'Y6877PUOOAPA834L'


def fetch_coingecko():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE public.airflow_coingecko")

    for coin in COINS:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        params = {"vs_currency": "usd", "days": 30, "interval": "daily"}

        response = requests.get(url, params=params)
        data = response.json()

        if "prices" not in data:
            continue

        cur.execute("""
            INSERT INTO public.airflow_coingecko
                (coin_id, prices, market_caps, total_volumes, updated_at)
            VALUES (%s, %s, %s, %s, now())
        """, (
            coin,
            json.dumps(data.get('prices', [])),
            json.dumps(data.get('market_caps', [])),
            json.dumps(data.get('total_volumes', []))
        ))

    conn.commit()
    cur.close()
    conn.close()


def fetch_binance():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE public.airflow_binance")

    for symbol in BINANCE_SYMBOLS:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        params = {"symbol": symbol}

        response = requests.get(url, params=params)
        data = response.json()

        if "symbol" not in data:
            continue

        cur.execute("""
            INSERT INTO public.airflow_binance
                (symbol, open_price, high_price, low_price, last_price,
                 volume, quote_volume, price_change, price_change_percent,
                 weighted_avg_price, open_time, close_time, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
        """, (
            data['symbol'],
            float(data['openPrice']),
            float(data['highPrice']),
            float(data['lowPrice']),
            float(data['lastPrice']),
            float(data['volume']),
            float(data['quoteVolume']),
            float(data['priceChange']),
            float(data['priceChangePercent']),
            float(data['weightedAvgPrice']),
            int(data['openTime']),
            int(data['closeTime'])
        ))

    conn.commit()
    cur.close()
    conn.close()


def fetch_alpha_vantage():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ALPHA_SYMBOL,
        "outputsize": "compact",
        "apikey": ALPHA_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    time_series = data.get('Time Series (Daily)', {})

    if not time_series:
        return

    for date_str, values in time_series.items():
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
            ALPHA_SYMBOL,
            date_str,
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
    schedule="@hourly",
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