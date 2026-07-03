import requests
import psycopg2
import time
import logging
from datetime import datetime
import os

from config import DB_CONN, COINS, BINANCE_SYMBOLS, ALPHA_SYMBOLS

ALPHA_API_KEY = os.getenv("ALPHA_API_KEY")
PIPELINE_NAME = "crypto_pipeline"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def safe_get(url, params=None, retries=3, sleep_sec=2):
    response = None

    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=20)

            if response.status_code == 200:
                return response

            log.warning(f"Retry {i + 1}/{retries} failed: {response.status_code}")

        except Exception as e:
            log.warning(f"Request error: {e}")

        time.sleep(sleep_sec)

    return response


def setup_tables(cur):
    log.info("Setting up staging tables...")

    cur.execute("CREATE SCHEMA IF NOT EXISTS public_staging")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_staging.etl_control (
            pipeline_name varchar PRIMARY KEY,
            last_load_timestamp timestamp,
            is_first_load boolean NOT NULL DEFAULT true,
            last_run_status varchar,
            updated_at timestamptz DEFAULT now()
        )
    """)

    cur.execute("""
        INSERT INTO public_staging.etl_control
            (pipeline_name, last_load_timestamp, is_first_load, last_run_status)
        VALUES
            (%s, NULL, true, 'INITIALIZED')
        ON CONFLICT (pipeline_name) DO NOTHING
    """, (PIPELINE_NAME,))

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_staging.airflow_coingecko_daily (
            coin_id      varchar,
            date         date,
            price        float,
            market_cap   float,
            total_volume float,
            updated_at   timestamptz DEFAULT now(),
            PRIMARY KEY (coin_id, date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_staging.airflow_binance_daily (
            symbol               varchar,
            date                 date,
            open_price           float,
            high_price           float,
            low_price            float,
            close_price          float,
            volume               float,
            close_time           bigint,
            quote_volume         float,
            number_of_trades     bigint,
            taker_buy_base_vol   float,
            taker_buy_quote_vol  float,
            updated_at           timestamptz DEFAULT now(),
            PRIMARY KEY (symbol, date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_staging.airflow_alpha_vantage (
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

    log.info("Staging tables ready.")


def get_is_first_load(cur):
    cur.execute("""
        SELECT is_first_load
        FROM public_staging.etl_control
        WHERE pipeline_name = %s
    """, (PIPELINE_NAME,))

    result = cur.fetchone()
    return result[0] if result else True


def truncate_staging_tables(cur):
    log.info("Truncating staging tables before latest load...")

    cur.execute("TRUNCATE TABLE public_staging.airflow_coingecko_daily")
    cur.execute("TRUNCATE TABLE public_staging.airflow_binance_daily")
    cur.execute("TRUNCATE TABLE public_staging.airflow_alpha_vantage")

    log.info("Staging tables truncated.")


def mark_pipeline_success(cur):
    cur.execute("""
        UPDATE public_staging.etl_control
        SET
            is_first_load = false,
            last_load_timestamp = now(),
            last_run_status = 'SUCCESS',
            updated_at = now()
        WHERE pipeline_name = %s
    """, (PIPELINE_NAME,))


def mark_pipeline_failed(cur):
    cur.execute("""
        UPDATE public_staging.etl_control
        SET
            last_run_status = 'FAILED',
            updated_at = now()
        WHERE pipeline_name = %s
    """, (PIPELINE_NAME,))


def ingest_coingecko(cur, first_load):
    log.info("Starting CoinGecko ingestion...")

    days = 30 if first_load else 1

    for coin in COINS:
        log.info(f"Fetching {coin}...")

        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": days,
            "interval": "daily"
        }

        response = safe_get(url, params)

        if not response or response.status_code != 200:
            log.error(f"Failed CoinGecko for {coin}")
            continue

        data = response.json()

        if "prices" not in data:
            log.warning(f"No prices for {coin}")
            continue

        for price_entry, market_entry, volume_entry in zip(
            data["prices"],
            data["market_caps"],
            data["total_volumes"]
        ):
            ts = price_entry[0]
            date_val = datetime.fromtimestamp(ts / 1000).date()

            cur.execute("""
                INSERT INTO public_staging.airflow_coingecko_daily
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

        log.info(f"{coin} ingested")
        time.sleep(3)

    log.info("CoinGecko done.")


def ingest_binance(cur, first_load):
    log.info("Starting Binance ingestion...")

    limit = 30 if first_load else 1

    for symbol in BINANCE_SYMBOLS:
        log.info(f"Fetching {symbol} daily kline...")

        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1d",
            "limit": limit
        }

        response = safe_get(url, params)

        if not response or response.status_code != 200:
            log.error(f"Failed Binance for {symbol}")
            continue

        data = response.json()

        if not isinstance(data, list):
            log.warning(f"Bad response for {symbol}: {data}")
            continue

        for kline in data:
            open_time_ms = int(kline[0])
            date_val = datetime.fromtimestamp(open_time_ms / 1000).date()

            cur.execute("""
                INSERT INTO public_staging.airflow_binance_daily
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

        log.info(f"{symbol} ingested ({len(data)} day)")
        time.sleep(1)

    log.info("Binance done.")


def ingest_alpha_vantage(cur, first_load):
    log.info("Starting Alpha Vantage ingestion...")

    if not ALPHA_API_KEY:
        log.warning("ALPHA_API_KEY is missing. Skipping Alpha Vantage ingestion.")
        return

    today = datetime.today().date()

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
        time_series = data.get("Time Series (Daily)", {})

        if not time_series:
            log.warning(f"No Alpha Vantage data for {symbol}")
            continue

        inserted_count = 0

        for date_str, values in time_series.items():
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            except Exception:
                continue

            if not first_load and date_obj != today:
                continue

            cur.execute("""
                INSERT INTO public_staging.airflow_alpha_vantage
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
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(values["5. volume"])
            ))

            inserted_count += 1

        log.info(f"{symbol} ingested ({inserted_count} latest records)")

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

        first_load = get_is_first_load(cur)
        log.info(f"is_first_load = {first_load}")

        truncate_staging_tables(cur)
        conn.commit()

        ingest_coingecko(cur, first_load)
        conn.commit()

        ingest_binance(cur, first_load)
        conn.commit()

        ingest_alpha_vantage(cur, first_load)
        conn.commit()

        mark_pipeline_success(cur)
        conn.commit()

        log.info("=" * 50)
        log.info("ALL INGESTION COMPLETE")
        log.info("=" * 50)

    except Exception as e:
        conn.rollback()

        try:
            mark_pipeline_failed(cur)
            conn.commit()
        except Exception:
            conn.rollback()

        log.error(f"FAILED PIPELINE: {e}")
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
