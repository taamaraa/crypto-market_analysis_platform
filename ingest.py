import requests
import psycopg2
import json
import time
import logging
from datetime import datetime
import os

from config import DB_CONN, COINS, BINANCE_SYMBOLS, ALPHA_SYMBOLS

ALPHA_API_KEY = os.getenv("ALPHA_API_KEY")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)


def safe_get(url, params=None, retries=3, sleep_sec=2):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r
            log.warning(f"Retry {i+1}/{retries} failed: {r.status_code}")
        except Exception as e:
            log.warning(f"Request error: {e}")

        time.sleep(sleep_sec)

    return r


def setup_tables(cur):
    log.info("Setting up tables...")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.airflow_coingecko (
            coin_id     varchar PRIMARY KEY,
            prices      jsonb,
            market_caps jsonb,
            total_volumes jsonb,
            updated_at  timestamptz DEFAULT now()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.airflow_binance (
            symbol                varchar PRIMARY KEY,
            open_price            float,
            high_price            float,
            low_price             float,
            last_price            float,
            volume                float,
            quote_volume          float,
            price_change          float,
            price_change_percent  float,
            weighted_avg_price    float,
            open_time             bigint,
            close_time            bigint,
            updated_at            timestamptz DEFAULT now()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.airflow_alpha_vantage (
            symbol     varchar,
            date       date,
            open       float,
            high       float,
            low        float,
            close      float,
            volume     bigint,
            updated_at timestamptz DEFAULT now(),
            PRIMARY KEY (symbol, date)
        )
    """)

    log.info("Tables ready.")


def ingest_coingecko(cur):
    log.info("Starting CoinGecko ingestion...")

    cur.execute("TRUNCATE TABLE public.airflow_coingecko")

    for coin in COINS:
        log.info(f"Fetching {coin}...")

        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        params = {"vs_currency": "usd", "days": 30, "interval": "daily"}

        response = safe_get(url, params)

        if not response or response.status_code != 200:
            log.error(f"Failed {coin}")
            continue

        data = response.json()

        if "prices" not in data:
            log.warning(f"No prices for {coin}")
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

        log.info(f"{coin} ingested ({len(data['prices'])} points)")
        time.sleep(3)

    log.info("CoinGecko done.")


def ingest_binance(cur):
    log.info("Starting Binance ingestion...")

    cur.execute("TRUNCATE TABLE public.airflow_binance")

    for symbol in BINANCE_SYMBOLS:
        log.info(f"Fetching {symbol}...")

        url = "https://api.binance.com/api/v3/ticker/24hr"
        params = {"symbol": symbol}

        response = safe_get(url, params)

        if not response or response.status_code != 200:
            log.error(f"Failed {symbol}")
            continue

        data = response.json()

        if "symbol" not in data or "code" in data:
            log.warning(f"Bad response for {symbol}: {data}")
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

        log.info(f"{symbol} ingested")

    log.info("Binance done.")


def ingest_alpha_vantage(cur):
    log.info("Starting Alpha Vantage ingestion...")

    for symbol in ALPHA_SYMBOLS:
        log.info(f"Fetching {symbol}...")

        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": ALPHA_API_KEY
        }

        response = safe_get(url, params)

        if not response or response.status_code != 200:
            log.error(f"Alpha Vantage failed for {symbol}")
            continue

        data = response.json()
        time_series = data.get('Time Series (Daily)', {})

        if not time_series:
            log.warning(f"No Alpha Vantage data for {symbol} (rate limit or key issue)")
            continue

        for date_str, values in time_series.items():
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            except:
                continue

            cur.execute("""
                INSERT INTO public.airflow_alpha_vantage
                    (symbol, date, open, high, low, close, volume, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,now())
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

        log.info(f"{symbol} ingested ({len(time_series)} days)")

    log.info("Alpha Vantage done.")


def main():
    log.info("=" * 50)
    log.info(f"Starting ingestion at {datetime.now()}")
    log.info("=" * 50)

    conn = psycopg2.connect(**DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        setup_tables(cur)
        conn.commit()

        ingest_coingecko(cur)
        conn.commit()

        ingest_binance(cur)
        conn.commit()

        ingest_alpha_vantage(cur)
        conn.commit()

        log.info("=" * 50)
        log.info("ALL INGESTION COMPLETE")
        log.info("=" * 50)

    except Exception as e:
        conn.rollback()
        log.error(f"FAILED PIPELINE: {e}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()