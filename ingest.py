import requests
import psycopg2
import time
import uuid
import logging
from datetime import datetime, timezone
import os

from config import DB_CONN

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
        CREATE TABLE IF NOT EXISTS public_staging.pipeline_logs (
            id               serial,
            run_id           uuid NOT NULL,
            source           varchar NOT NULL,
            status           varchar NOT NULL,
            records_inserted integer DEFAULT 0,
            duration_seconds float,
            error_message    text,
            run_timestamp    timestamptz DEFAULT now(),
            PRIMARY KEY (run_id, source)
        )
    """)

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


    cur.execute("CREATE SCHEMA IF NOT EXISTS raw")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.coingecko_daily
            (LIKE public_staging.airflow_coingecko_daily INCLUDING ALL)
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.binance_daily
            (LIKE public_staging.airflow_binance_daily INCLUDING ALL)
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.alpha_vantage
            (LIKE public_staging.airflow_alpha_vantage INCLUDING ALL)
    """)

    log.info("Staging and raw tables ready.")


def setup_config_tables(cur):
    log.info("Setting up config schema...")

    cur.execute("CREATE SCHEMA IF NOT EXISTS config")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS config.connections (
            id                 serial PRIMARY KEY,
            source_name        varchar UNIQUE NOT NULL,
            base_url           text NOT NULL,
            url_type           varchar NOT NULL DEFAULT 'query',
            auth_type          varchar NOT NULL DEFAULT 'none',
            api_key_env        varchar,
            api_key_param_name varchar,
            static_params      jsonb NOT NULL DEFAULT '{}'::jsonb,
            rate_limit_sec     numeric NOT NULL DEFAULT 1,
            is_active          boolean NOT NULL DEFAULT true,
            created_at         timestamptz DEFAULT now(),
            updated_at         timestamptz DEFAULT now()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS config.asset_mapping (
            id             serial PRIMARY KEY,
            connection_id  int NOT NULL REFERENCES config.connections(id),
            source_symbol  varchar NOT NULL,
            display_name   varchar,
            is_active      boolean NOT NULL DEFAULT true,
            UNIQUE (connection_id, source_symbol)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS config.column_mapping (
            id             serial PRIMARY KEY,
            connection_id  int NOT NULL REFERENCES config.connections(id),
            source_field   varchar NOT NULL,
            target_column  varchar NOT NULL,
            data_type      varchar NOT NULL DEFAULT 'float',
            is_active      boolean NOT NULL DEFAULT true,
            UNIQUE (connection_id, source_field)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS config.table_mapping (
            id                  serial PRIMARY KEY,
            connection_id       int NOT NULL UNIQUE REFERENCES config.connections(id),
            destination_schema  varchar NOT NULL DEFAULT 'public_staging',
            destination_table   varchar NOT NULL,
            pk_columns          text[] NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS config.load_errors (
            id             serial PRIMARY KEY,
            run_id         uuid NOT NULL,
            connection_id  int REFERENCES config.connections(id),
            source_symbol  varchar,
            error_message  text NOT NULL,
            occurred_at    timestamptz DEFAULT now()
        )
    """)

    _seed_config_tables(cur)

    log.info("Config schema ready.")


def _seed_config_tables(cur):
    """One-time seed matching the previous hardcoded COINS/BINANCE_SYMBOLS/
    ALPHA_SYMBOLS lists from config.py. Safe to re-run - every insert is
    ON CONFLICT DO NOTHING, so it never overwrites values you've since
    changed by hand (e.g. an is_active flag flipped off)."""

    connections = [
        (
            "coingecko",
            "https://api.coingecko.com/api/v3/coins/{symbol}/market_chart",
            "path", "none", None, None,
            '{"vs_currency": "usd", "interval": "daily", "days": "{backfill_days}"}',
            3,
        ),
        (
            "binance",
            "https://api.binance.com/api/v3/klines",
            "query", "none", None, None,
            '{"interval": "1d", "limit": "{backfill_days}"}',
            1,
        ),
        (
            "alpha_vantage",
            "https://www.alphavantage.co/query",
            "query", "api_key", "ALPHA_API_KEY", "apikey",
            '{"function": "TIME_SERIES_DAILY", "outputsize": "compact"}',
            1.2,
        ),
    ]

    for source_name, base_url, url_type, auth_type, api_key_env, api_key_param_name, static_params, rate_limit_sec in connections:
        cur.execute("""
            INSERT INTO config.connections
                (source_name, base_url, url_type, auth_type, api_key_env,
                 api_key_param_name, static_params, rate_limit_sec)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (source_name) DO NOTHING
        """, (source_name, base_url, url_type, auth_type, api_key_env,
              api_key_param_name, static_params, rate_limit_sec))

    asset_seed = {
        "coingecko": ["bitcoin", "ethereum", "solana", "binancecoin", "cardano"],
        "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT"],
        "alpha_vantage": ["AAPL", "TSLA"],
    }

    for source_name, symbols in asset_seed.items():
        for symbol in symbols:
            cur.execute("""
                INSERT INTO config.asset_mapping (connection_id, source_symbol)
                SELECT id, %s FROM config.connections WHERE source_name = %s
                ON CONFLICT (connection_id, source_symbol) DO NOTHING
            """, (symbol, source_name))

    column_seed = {
        "coingecko": [
            ("__symbol__", "coin_id", "text"),
            ("__date__", "date", "date"),
            ("price", "price", "float"),
            ("market_cap", "market_cap", "float"),
            ("total_volume", "total_volume", "float"),
        ],
        "binance": [
            ("__symbol__", "symbol", "text"),
            ("__date__", "date", "date"),
            ("open", "open_price", "float"),
            ("high", "high_price", "float"),
            ("low", "low_price", "float"),
            ("close", "close_price", "float"),
            ("volume", "volume", "float"),
            ("close_time", "close_time", "bigint"),
            ("quote_volume", "quote_volume", "float"),
            ("number_of_trades", "number_of_trades", "bigint"),
            ("taker_buy_base_vol", "taker_buy_base_vol", "float"),
            ("taker_buy_quote_vol", "taker_buy_quote_vol", "float"),
        ],
        "alpha_vantage": [
            ("__symbol__", "symbol", "text"),
            ("__date__", "date", "date"),
            ("open", "open", "float"),
            ("high", "high", "float"),
            ("low", "low", "float"),
            ("close", "close", "float"),
            ("volume", "volume", "bigint"),
        ],
    }

    for source_name, columns in column_seed.items():
        for source_field, target_column, data_type in columns:
            cur.execute("""
                INSERT INTO config.column_mapping
                    (connection_id, source_field, target_column, data_type)
                SELECT id, %s, %s, %s FROM config.connections WHERE source_name = %s
                ON CONFLICT (connection_id, source_field) DO NOTHING
            """, (source_field, target_column, data_type, source_name))

    table_seed = {
        "coingecko": ("public_staging", "airflow_coingecko_daily", ["coin_id", "date"]),
        "binance": ("public_staging", "airflow_binance_daily", ["symbol", "date"]),
        "alpha_vantage": ("public_staging", "airflow_alpha_vantage", ["symbol", "date"]),
    }

    for source_name, (dest_schema, dest_table, pk_columns) in table_seed.items():
        cur.execute("""
            INSERT INTO config.table_mapping
                (connection_id, destination_schema, destination_table, pk_columns)
            SELECT id, %s, %s, %s FROM config.connections WHERE source_name = %s
            ON CONFLICT (connection_id) DO NOTHING
        """, (dest_schema, dest_table, pk_columns, source_name))


def get_backfill_window(cur, min_days=MIN_BACKFILL_DAYS, max_days=MAX_BACKFILL_DAYS):
    """
    Decide how many days of history to (re)fetch this run.

    Instead of a hardcoded "30 on first load, 1 otherwise" (which permanently
    loses data if the pipeline misses a day - no run, a crashed container,
    a long weekend), this looks at etl_control.last_load_timestamp and asks
    for enough days to cover the actual gap, capped at max_days (API/history
    limits) and floored at min_days.

    BACKFILL_DAYS_OVERRIDE bypasses all of that for a one-off manual deep
    backfill (e.g. filling in months of missing history) without raising
    MAX_BACKFILL_DAYS itself, which would loosen the cap on every future
    daily run too.
    """
    override = os.getenv("BACKFILL_DAYS_OVERRIDE")
    if override:
        return int(override)

    cur.execute("""
        SELECT last_load_timestamp, is_first_load
        FROM public_staging.etl_control
        WHERE pipeline_name = %s
    """, (PIPELINE_NAME,))

    result = cur.fetchone()

    if not result or result[1] or result[0] is None:

        return max_days

    last_load_ts = result[0]
    if last_load_ts.tzinfo is None:
        last_load_ts = last_load_ts.replace(tzinfo=timezone.utc)

    elapsed_days = (datetime.now(timezone.utc).date() - last_load_ts.date()).days

    return max(min_days, min(elapsed_days + 1, max_days))


def truncate_staging_tables(cur):
    log.info("Truncating staging tables before latest load...")

    cur.execute("TRUNCATE TABLE public_staging.airflow_coingecko_daily")
    cur.execute("TRUNCATE TABLE public_staging.airflow_binance_daily")
    cur.execute("TRUNCATE TABLE public_staging.airflow_alpha_vantage")

    log.info("Staging tables truncated.")


def archive_coingecko_to_raw(cur):
    cur.execute("""
        INSERT INTO raw.coingecko_daily
            (coin_id, date, price, market_cap, total_volume, updated_at)
        SELECT coin_id, date, price, market_cap, total_volume, updated_at
        FROM public_staging.airflow_coingecko_daily
        ON CONFLICT (coin_id, date) DO UPDATE SET
            price = EXCLUDED.price,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            updated_at = EXCLUDED.updated_at
    """)
    log.info(f"Archived {cur.rowcount} CoinGecko rows to raw.")


def archive_binance_to_raw(cur):
    cur.execute("""
        INSERT INTO raw.binance_daily
            (symbol, date, open_price, high_price, low_price, close_price,
             volume, close_time, quote_volume, number_of_trades,
             taker_buy_base_vol, taker_buy_quote_vol, updated_at)
        SELECT symbol, date, open_price, high_price, low_price, close_price,
               volume, close_time, quote_volume, number_of_trades,
               taker_buy_base_vol, taker_buy_quote_vol, updated_at
        FROM public_staging.airflow_binance_daily
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
            updated_at = EXCLUDED.updated_at
    """)
    log.info(f"Archived {cur.rowcount} Binance rows to raw.")


def archive_alpha_vantage_to_raw(cur):
    cur.execute("""
        INSERT INTO raw.alpha_vantage
            (symbol, date, open, high, low, close, volume, updated_at)
        SELECT symbol, date, open, high, low, close, volume, updated_at
        FROM public_staging.airflow_alpha_vantage
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_at = EXCLUDED.updated_at
    """)
    log.info(f"Archived {cur.rowcount} Alpha Vantage rows to raw.")


ARCHIVE_FUNCS = {
    "coingecko": archive_coingecko_to_raw,
    "binance": archive_binance_to_raw,
    "alpha_vantage": archive_alpha_vantage_to_raw,
}


def mark_pipeline_status(cur, status):
    """SUCCESS | PARTIAL_SUCCESS both advance the watermark (we made
    progress). FAILED does not - handled separately in main() so a run
    that produced nothing doesn't move last_load_timestamp forward and
    silently skip the gap on the next run."""
    cur.execute("""
        UPDATE public_staging.etl_control
        SET
            is_first_load = false,
            last_load_timestamp = now(),
            last_run_status = %s,
            updated_at = now()
        WHERE pipeline_name = %s
    """, (status, PIPELINE_NAME))


def mark_pipeline_failed(cur):
    cur.execute("""
        UPDATE public_staging.etl_control
        SET
            last_run_status = 'FAILED',
            updated_at = now()
        WHERE pipeline_name = %s
    """, (PIPELINE_NAME,))


def log_run(cur, run_id, source, status, records=0, duration=0.0, error=None):
    cur.execute("""
        INSERT INTO public_staging.pipeline_logs
            (run_id, source, status, records_inserted, duration_seconds, error_message, run_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (run_id, source) DO UPDATE SET
            status = EXCLUDED.status,
            records_inserted = EXCLUDED.records_inserted,
            duration_seconds = EXCLUDED.duration_seconds,
            error_message = EXCLUDED.error_message
    """, (str(run_id), source, status, records, duration, error))
    log.info(f"[LOG] {source} -> {status} | records: {records} | duration: {duration:.2f}s")


def log_load_error(cur, run_id, connection_id, source_symbol, error_message):
    cur.execute("""
        INSERT INTO config.load_errors
            (run_id, connection_id, source_symbol, error_message)
        VALUES (%s, %s, %s, %s)
    """, (str(run_id), connection_id, source_symbol, str(error_message)[:2000]))


def get_active_connections(cur):
    cur.execute("""
        SELECT id, source_name, base_url, url_type, auth_type, api_key_env,
               api_key_param_name, static_params, rate_limit_sec
        FROM config.connections
        WHERE is_active = true
        ORDER BY id
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_active_asset_mappings(cur, connection_id):
    cur.execute("""
        SELECT source_symbol
        FROM config.asset_mapping
        WHERE connection_id = %s AND is_active = true
        ORDER BY id
    """, (connection_id,))
    return [row[0] for row in cur.fetchall()]


def get_column_mapping(cur, connection_id):
    cur.execute("""
        SELECT source_field, target_column, data_type
        FROM config.column_mapping
        WHERE connection_id = %s AND is_active = true
    """, (connection_id,))
    return cur.fetchall()


def get_table_mapping(cur, connection_id):
    cur.execute("""
        SELECT destination_schema, destination_table, pk_columns
        FROM config.table_mapping
        WHERE connection_id = %s
    """, (connection_id,))
    return cur.fetchone()


def build_request(connection, symbol, backfill_days):
    """Builds (url, params) from config.connections metadata - no
    source-specific branching here beyond url_type/auth_type, both of
    which are data, not code."""
    params = {}

    for key, value in connection["static_params"].items():
        if isinstance(value, str) and "{backfill_days}" in value:
            resolved = value.replace("{backfill_days}", str(backfill_days))
            params[key] = int(resolved) if resolved.isdigit() else resolved
        else:
            params[key] = value

    if connection["url_type"] == "path":
        url = connection["base_url"].format(symbol=symbol)
    else:
        url = connection["base_url"]
        params["symbol"] = symbol

    if connection["auth_type"] == "api_key":
        api_key = os.getenv(connection["api_key_env"])
        if not api_key:
            raise RuntimeError(
                f"{connection['api_key_env']} is not set. Refusing to silently "
                f"skip {connection['source_name']} ingestion - fix the "
                f"environment variable."
            )
        param_name = connection["api_key_param_name"] or "apikey"
        params[param_name] = api_key

    return url, params


def cast_value(value, data_type):
    if value is None:
        return None
    if data_type == "float":
        return float(value)
    if data_type in ("int", "bigint"):
        return int(value)
    return value


def apply_column_mapping(raw_record, column_mappings):
    row = {}
    for source_field, target_column, data_type in column_mappings:
        if source_field not in raw_record:
            continue
        row[target_column] = cast_value(raw_record[source_field], data_type)
    return row


def upsert_row(cur, schema, table, pk_columns, row):
    """Generic upsert built from config.table_mapping + whatever columns
    apply_column_mapping produced. schema/table/column names all come from
    config tables we control (not request input), so building the SQL
    string from them is safe - values themselves stay parameterized."""
    columns = list(row.keys())
    values = [row[c] for c in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    update_cols = [c for c in columns if c not in pk_columns]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    pk_list = ", ".join(pk_columns)

    sql = f"""
        INSERT INTO {schema}.{table} ({col_list}, updated_at)
        VALUES ({placeholders}, now())
        ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}, updated_at = now()
    """
    cur.execute(sql, values)


def parse_coingecko(symbol, raw_json):
    if "prices" not in raw_json:
        raise RuntimeError(f"No prices in CoinGecko response: {raw_json}")

    records = []
    for price_entry, market_entry, volume_entry in zip(
        raw_json.get("prices", []),
        raw_json.get("market_caps", []),
        raw_json.get("total_volumes", [])
    ):
        records.append({
            "__symbol__": symbol,
            "__date__": to_utc_date(price_entry[0]),
            "price": price_entry[1],
            "market_cap": market_entry[1],
            "total_volume": volume_entry[1],
        })

    if not records:
        raise RuntimeError("CoinGecko returned prices but zero usable records")

    return records


def parse_binance(symbol, raw_json):
    if not isinstance(raw_json, list):
        raise RuntimeError(f"Unexpected Binance response: {raw_json}")

    records = []
    for kline in raw_json:
        records.append({
            "__symbol__": symbol,
            "__date__": to_utc_date(int(kline[0])),
            "open": kline[1],
            "high": kline[2],
            "low": kline[3],
            "close": kline[4],
            "volume": kline[5],
            "close_time": int(kline[6]),
            "quote_volume": kline[7],
            "number_of_trades": int(kline[8]),
            "taker_buy_base_vol": kline[9],
            "taker_buy_quote_vol": kline[10],
        })

    if not records:
        raise RuntimeError("Binance returned an empty kline list")

    return records


def parse_alpha_vantage(symbol, raw_json, backfill_days):
    time_series = raw_json.get("Time Series (Daily)", {})

    if not time_series:
        note = raw_json.get("Note") or raw_json.get("Information") or raw_json
        raise RuntimeError(f"No Alpha Vantage data for {symbol}: {note}")

    records = []
    sorted_dates = sorted(time_series.keys(), reverse=True)[:backfill_days]

    for date_str in sorted_dates:
        values = time_series[date_str]
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            log.warning(f"Skipping invalid Alpha Vantage date for {symbol}: {date_str}")
            continue

        records.append({
            "__symbol__": symbol,
            "__date__": date_obj,
            "open": values["1. open"],
            "high": values["2. high"],
            "low": values["3. low"],
            "close": values["4. close"],
            "volume": values["5. volume"],
        })

    if not records:
        raise RuntimeError(f"No usable Alpha Vantage records for {symbol}")

    return records


PARSERS = {
    "coingecko": parse_coingecko,
    "binance": parse_binance,
    "alpha_vantage": parse_alpha_vantage,
}


def ingest_generic(cur, run_id, connection, backfill_days):
    """One engine for every source. Nothing here branches on source_name
    except picking which shape adapter to call - everything about which
    symbols, which columns, and which destination table is config."""
    source_name = connection["source_name"]
    parser = PARSERS.get(source_name)

    if not parser:
        raise RuntimeError(
            f"No parser registered for source '{source_name}'. Adding a new "
            f"connection still needs one small parse_* adapter for its "
            f"response shape - everything else is config."
        )

    column_mappings = get_column_mapping(cur, connection["id"])
    table_map = get_table_mapping(cur, connection["id"])

    if not table_map:
        raise RuntimeError(f"No table_mapping configured for '{source_name}'")

    dest_schema, dest_table, pk_columns = table_map

    symbols = get_active_asset_mappings(cur, connection["id"])
    log.info(f"Starting {source_name} ingestion (backfill_days={backfill_days}, symbols={len(symbols)})...")

    inserted_total = 0
    failed_symbols = []

    for symbol in symbols:
        log.info(f"[{source_name}] fetching {symbol}...")

        try:
            url, params = build_request(connection, symbol, backfill_days)
            response = safe_get(url, params)

            if not response or response.status_code != 200:
                status = response.status_code if response else "no response"
                raise RuntimeError(f"HTTP {status}")

            raw_json = response.json()

            if source_name == "alpha_vantage":
                records = parser(symbol, raw_json, backfill_days)
            else:
                records = parser(symbol, raw_json)

            symbol_inserted = 0
            symbol_skipped = 0

            for raw_record in records:
                try:
                    row = apply_column_mapping(raw_record, column_mappings)
                    upsert_row(cur, dest_schema, dest_table, pk_columns, row)
                    symbol_inserted += 1
                except Exception as e:
                    symbol_skipped += 1
                    log.warning(f"[{source_name}] skipping bad record for {symbol}: {e}")

            if symbol_inserted == 0:
                raise RuntimeError(f"0 rows inserted (skipped {symbol_skipped})")

            inserted_total += symbol_inserted
            log.info(f"[{source_name}] {symbol} ingested ({symbol_inserted} rows)")

        except Exception as e:
            failed_symbols.append(symbol)
            log_load_error(cur, run_id, connection["id"], symbol, e)
            log.error(f"[{source_name}] failed for {symbol}: {e}")

        time.sleep(float(connection["rate_limit_sec"]))

    log.info(f"{source_name} done. inserted={inserted_total} failed={failed_symbols}")
    return inserted_total, failed_symbols


def main():
    log.info("=" * 50)
    log.info(f"Starting ingestion at {datetime.now(timezone.utc)}")
    log.info("=" * 50)

    run_id = uuid.uuid4()
    log.info(f"Run ID: {run_id}")

    conn = psycopg2.connect(**DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        setup_tables(cur)
        setup_config_tables(cur)
        conn.commit()

        backfill_days = get_backfill_window(cur)
        log.info(f"backfill_days = {backfill_days}")

        truncate_staging_tables(cur)
        conn.commit()

        connections = get_active_connections(cur)
        if not connections:
            raise RuntimeError(
                "No active connections in config.connections - nothing to ingest."
            )

        results = []

        for connection in connections:
            source_name = connection["source_name"]
            t0 = time.time()

            count, failed = ingest_generic(cur, run_id, connection, backfill_days)
            conn.commit()

            archive_fn = ARCHIVE_FUNCS.get(source_name)
            if archive_fn:
                archive_fn(cur)
                conn.commit()

            log_run(
                cur, run_id, source_name,
                "failed" if (count == 0 or failed) else "success",
                count, time.time() - t0,
                f"failed symbols: {failed}" if failed else None
            )
            conn.commit()

            results.append((source_name, count, failed))

        problems = []
        for source_name, count, failed in results:
            if count == 0:
                problems.append(f"{source_name}: 0 rows inserted")
            elif failed:
                problems.append(f"{source_name}: failed symbols {failed}")

        any_success = any(count > 0 for _, count, _ in results)

        if not any_success:
            raise RuntimeError("Full ingestion failure: " + "; ".join(problems))

        if problems:
            mark_pipeline_status(cur, "PARTIAL_SUCCESS")
            conn.commit()
            log.info("=" * 50)
            log.info("PARTIAL SUCCESS: " + "; ".join(problems))
            log.info("=" * 50)
        else:
            mark_pipeline_status(cur, "SUCCESS")
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
