import requests
import psycopg2
import time
import logging
from datetime import datetime, timedelta, timezone
import os

from config import DB_CONN, COINS, BINANCE_SYMBOLS, ALPHA_SYMBOLS

ALPHA_API_KEY = os.getenv("ALPHA_API_KEY")
PIPELINE_NAME = "crypto_pipeline"

MIN_BACKFILL_DAYS = 1
MAX_BACKFILL_DAYS = 30  

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def to_utc_date(timestamp_ms):
    
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).date()


def safe_get(url, params=None, retries=3, base_sleep_sec=2, max_sleep_sec=30):
    response = None

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=20)

            if response.status_code == 200:
                return response

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                sleep_for = (
                    float(retry_after) if retry_after
                    else min(max_sleep_sec, base_sleep_sec * (2 ** attempt))
                )
                log.warning(
                    f"Rate limited (429) on {url}. "
                    f"Sleeping {sleep_for:.1f}s before retry {attempt + 1}/{retries}."
                )
                time.sleep(sleep_for)
                continue

            log.warning(f"Retry {attempt + 1}/{retries} failed: {response.status_code}")

        except Exception as e:
            log.warning(f"Request error: {e}")

        time.sleep(min(max_sleep_sec, base_sleep_sec * (2 ** attempt)))

    return response


def setup_tables(cur):
    log.info("Setting up staging tables...")

    cur.execute("CREATE SCHEMA IF NOT EXISTS public_staging")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_staging.etl_control (
            pipeline_name varchar PRIMARY KEY,
            last_load_timestamp timestamptz,
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


def get_backfill_window(cur, min_days=MIN_BACKFILL_DAYS, max_days=MAX_BACKFILL_DAYS):
    """
    Decide how many days of history to (re)fetch this run.

    Instead of a hardcoded "30 on first load, 1 otherwise" (which permanently
    loses data if the pipeline misses a day - no run, a crashed container,
    a long weekend), this looks at etl_control.last_load_timestamp and asks
    for enough days to cover the actual gap, capped at max_days (API/history
    limits) and floored at min_days.
    """
    cur.execute("""
        SELECT last_load_timestamp, is_first_load
        FROM public_staging.etl_control
        WHERE pipeline_name = %s
    """, (PIPELINE_NAME,))

    result = cur.fetchone()

    if not result or result[1] or result[0] is None:
        # first load ever, or we don't have a reliable checkpoint - pull the
        # full lookback window rather than guessing
        return max_days

    last_load_ts = result[0]
    if last_load_ts.tzinfo is None:
        last_load_ts = last_load_ts.replace(tzinfo=timezone.utc)

    elapsed_days = (datetime.now(timezone.utc).date() - last_load_ts.date()).days

    # +1 as a small safety buffer in case a run finished very late in the day
    return max(min_days, min(elapsed_days + 1, max_days))


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


def ingest_coingecko(cur, backfill_days):
    log.info(f"Starting CoinGecko ingestion (backfill_days={backfill_days})...")

    inserted_total = 0
    failed_total = 0

    for coin in COINS:
        log.info(f"Fetching {coin}...")

        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": backfill_days,
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
            try:
                ts = price_entry[0]
                date_val = to_utc_date(ts)

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
                inserted_total += 1

            except Exception as e:
                # one malformed record shouldn't kill the whole source
                failed_total += 1
                log.warning(f"Skipping bad CoinGecko record for {coin}: {e}")

        log.info(f"{coin} ingested")
        time.sleep(3)

    log.info(f"CoinGecko done. inserted={inserted_total} skipped={failed_total}")


def ingest_binance(cur, backfill_days):
    log.info(f"Starting Binance ingestion (backfill_days={backfill_days})...")

    inserted_total = 0
    failed_total = 0

    for symbol in BINANCE_SYMBOLS:
        log.info(f"Fetching {symbol} daily kline...")

        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1d",
            "limit": backfill_days
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
            try:
                open_time_ms = int(kline[0])
                date_val = to_utc_date(open_time_ms)

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
                inserted_total += 1

            except Exception as e:
                failed_total += 1
                log.warning(f"Skipping bad Binance record for {symbol}: {e}")

        log.info(f"{symbol} ingested ({len(data)} day(s) received)")
        time.sleep(1)

    log.info(f"Binance done. inserted={inserted_total} skipped={failed_total}")


def ingest_alpha_vantage(cur, backfill_days):
    log.info(f"Starting Alpha Vantage ingestion (backfill_days={backfill_days})...")

    if not ALPHA_API_KEY:
        raise RuntimeError(
            "ALPHA_API_KEY is not set. Refusing to silently skip Alpha Vantage "
            "ingestion - fix the environment variable (see .env / docker-compose.yml) "
            "instead of letting this pass as a false SUCCESS."
        )

    inserted_total = 0
    failed_total = 0

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
            log.warning(
                f"No Alpha Vantage data for {symbol}: "
                f"{data.get('Note') or data.get('Information') or data}"
            )
            continue

        # Alpha Vantage uses trading days, not calendar days.
        # So we take the latest available N trading records.
        sorted_dates = sorted(time_series.keys(), reverse=True)
        dates_to_process = sorted_dates[:backfill_days]

        for date_str in dates_to_process:
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            except Exception:
                failed_total += 1
                log.warning(f"Skipping invalid Alpha Vantage date for {symbol}: {date_str}")
                continue

            values = time_series[date_str]

            try:
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

                inserted_total += 1

            except Exception as e:
                failed_total += 1
                log.warning(
                    f"Skipping bad Alpha Vantage record for {symbol} "
                    f"on {date_str}: {e}"
                )

        log.info(f"{symbol} ingested")

    log.info(f"Alpha Vantage done. inserted={inserted_total} skipped={failed_total}")

def main():
    log.info("=" * 50)
    log.info(f"Starting ingestion at {datetime.now(timezone.utc)}")
    log.info("=" * 50)

    conn = psycopg2.connect(**DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        setup_tables(cur)
        conn.commit()

        backfill_days = get_backfill_window(cur)
        log.info(f"backfill_days = {backfill_days}")

        truncate_staging_tables(cur)
        conn.commit()

        ingest_coingecko(cur, backfill_days)
        conn.commit()

        ingest_binance(cur, backfill_days)
        conn.commit()

        ingest_alpha_vantage(cur, backfill_days)
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