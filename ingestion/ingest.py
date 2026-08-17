import requests
import psycopg2
import json
import time
import logging
import uuid
import os
from datetime import datetime

from config import DB_CONN

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)


# ============================================================
# Фаза 2 - Config-driven helper функции
# Читаат од connections / column_mapping / table_mapping табелите
# наместо да имаат hardcoded вредности во кодот.
# ============================================================

def resolve_secret(secret_ref):
    """
    Ја разрешува вредноста на secret_ref од connections табелата.
    'env:ALPHA_API_KEY' -> ја чита ALPHA_API_KEY env variable-та.
    None -> нема тајна (public API).
    """
    if not secret_ref:
        return None
    if secret_ref.startswith("env:"):
        env_var_name = secret_ref.split("env:", 1)[1]
        return os.getenv(env_var_name)
    return secret_ref


def get_connection(cur, source_name):
    """Прочита host/port/base_path/secret за даден извор од config.connections табелата."""
    cur.execute("""
        SELECT host, port, base_path, secret_ref, is_active
        FROM config.connections
        WHERE source_name = %s
    """, (source_name,))
    row = cur.fetchone()

    if row is None:
        raise ValueError(f"Нема connection конфигурација за извор '{source_name}'")

    host, port, base_path, secret_ref, is_active = row

    if not is_active:
        raise ValueError(f"Конекцијата '{source_name}' е означена како is_active=false")

    return {
        "host": host,
        "port": port,
        "base_path": base_path,
        "secret": resolve_secret(secret_ref),
    }


def get_column_mapping(cur, source_name):
    """Прочита ја листата на field-mapping правила за даден извор, по редослед."""
    cur.execute("""
        SELECT source_field_path, destination_column, data_type, transform_note
        FROM config.column_mapping
        WHERE source_name = %s
        ORDER BY mapping_id
    """, (source_name,))
    rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Нема column_mapping редови за извор '{source_name}'")

    return [
        {
            "source_field_path": r[0],
            "destination_column": r[1],
            "data_type": r[2],
            "transform_note": r[3],
        }
        for r in rows
    ]


def get_table_mapping(cur, source_name):
    """Прочита во која destination табела оди даден извор, и кој е неговиот primary key."""
    cur.execute("""
        SELECT destination_schema, destination_table, primary_key_columns
        FROM config.table_mapping
        WHERE source_name = %s
    """, (source_name,))
    row = cur.fetchone()

    if row is None:
        raise ValueError(f"Нема table_mapping ред за извор '{source_name}'")

    schema, table, pk_columns = row

    return {
        "schema": schema,
        "table": table,
        "primary_key_columns": [c.strip() for c in pk_columns.split(",")],
    }


def get_active_identifiers(cur, source_name):
    """
    Прочита ги активните source_symbol вредности за даден извор од config.asset_mapping,
    наместо hardcoded COINS/BINANCE_SYMBOLS/ALPHA_SYMBOLS листи во config.py.
    """
    cur.execute("""
        SELECT am.source_symbol
        FROM config.asset_mapping am
        JOIN config.assets a ON am.asset_key = a.asset_key
        WHERE am.source_name = %s
          AND am.is_active = true
          AND a.is_active = true
        ORDER BY am.source_symbol
    """, (source_name,))
    rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Нема активни asset_mapping редови за извор '{source_name}'")

    return [r[0] for r in rows]


def safe_get(url, params=None, retries=3, sleep_sec=2):
    r = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)

            if r.status_code == 200:
                return r

            if r.status_code == 429:
                retry_after = r.headers.get('Retry-After')
                if retry_after and retry_after.isdigit():
                    wait = int(retry_after)
                else:
                    wait = sleep_sec * (2 ** i)
                log.warning(f"Rate limited (429). Чекам {wait}s пред retry {i+1}/{retries}...")
                time.sleep(wait)
                continue

            log.warning(f"Retry {i+1}/{retries} failed: {r.status_code}")

        except requests.exceptions.RequestException as e:
            log.warning(f"Request error: {e}")
            r = None

        time.sleep(sleep_sec * (2 ** i))

    return r


def setup_tables(cur):
    log.info("Setting up tables...")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.stg_coingecko (
            coin_id      varchar,
            ingested_at  date DEFAULT CURRENT_DATE,
            price_date   timestamptz,
            price        float,
            market_cap   float,
            volume       float,
            updated_at   timestamptz DEFAULT now(),
            PRIMARY KEY (coin_id, price_date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.stg_binance (
            symbol            varchar,
            date              date,
            open_price        float,
            high_price        float,
            low_price         float,
            close_price       float,
            volume            float,
            quote_volume      float,
            number_of_trades  integer,
            updated_at        timestamptz DEFAULT now(),
            PRIMARY KEY (symbol, date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.stg_alphavantage (
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.pipeline_logs (
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
        CREATE TABLE IF NOT EXISTS public.etl_control (
            table_name     varchar(100) PRIMARY KEY,
            last_load_type varchar(20),
            last_run_date  timestamptz,
            is_first_load  boolean DEFAULT TRUE,
            last_loaded_at timestamptz
        )
    """)

    cur.execute("""
        INSERT INTO public.etl_control (table_name, last_load_type, last_run_date, is_first_load)
        VALUES
            ('stg_coingecko', NULL, NULL, TRUE),
            ('stg_binance', NULL, NULL, TRUE),
            ('stg_alphavantage', NULL, NULL, TRUE),
            ('int_crypto_prices', NULL, NULL, TRUE)
        ON CONFLICT (table_name) DO NOTHING
    """)

    log.info("Tables ready.")


def get_days_to_fetch(cur):
    cur.execute("""
        SELECT last_loaded_at FROM public.etl_control
        WHERE table_name = 'int_crypto_prices'
    """)
    row = cur.fetchone()

    if row is None or row[0] is None:
        return 30  # прв пат, нема watermark

    last_date = row[0].date()
    days_gap = (datetime.utcnow().date() - last_date).days

    if days_gap < 1:
        days_gap = 1  # секогаш барем 1 ден

    return min(days_gap, 30)  # капирано на 30


def truncate_table(cur, table_name):
    log.info(f"[TRUNCATE] {table_name} -> бришење на стари податоци")
    cur.execute(f"TRUNCATE TABLE public.{table_name}")


def mark_load_complete(cur, table_name, load_type):
    cur.execute("""
        UPDATE public.etl_control
        SET last_load_type = %s,
            last_run_date  = now(),
            is_first_load  = FALSE
        WHERE table_name = %s
    """, (load_type, table_name))
    log.info(f"[CONTROL] {table_name} -> load_type={load_type}")


def log_run(cur, run_id, source, status, records=0, duration=0.0, error=None):
    cur.execute("""
        INSERT INTO public.pipeline_logs
            (run_id, source, status, records_inserted, duration_seconds, error_message, run_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (run_id, source) DO UPDATE SET
            status           = EXCLUDED.status,
            records_inserted = EXCLUDED.records_inserted,
            duration_seconds = EXCLUDED.duration_seconds,
            error_message    = EXCLUDED.error_message
    """, (str(run_id), source, status, records, duration, error))
    log.info(f"[LOG] {source} → {status} | records: {records} | duration: {duration:.2f}s")


def log_load_error(cur, run_id, source_name, identifier, error_message):
    """
    Фаза 5 - per-object failure log. Бележи КОНКРЕТНО кој (source, identifier)
    пар пропаднал, за разлика од pipeline_logs кој е ниво на цел извор.
    """
    cur.execute("""
        INSERT INTO config.load_errors (run_id, source_name, identifier, error_message)
        VALUES (%s, %s, %s, %s)
    """, (str(run_id), source_name, identifier, error_message))
    log.warning(f"[LOAD_ERROR] {source_name}/{identifier}: {error_message}")


# ============================================================
# Фаза 3 - Генеричка ingestion функција (config-driven)
# ============================================================

def build_request(source_name, connection, identifier, days):
    """
    Мал, неизбежен per-source dispatch за URL/params -
    самите API форми (path param vs query param vs auth) реално се разликуваат.
    Ова НЕ е делот што менторката сакаше generic - тој дел е подолу (parsing/mapping/insert).
    """
    if source_name == 'coingecko':
        url = f"https://{connection['host']}{connection['base_path'].format(id=identifier)}"
        params = {"vs_currency": "usd", "days": days, "interval": "daily"}
    elif source_name == 'binance':
        url = f"https://{connection['host']}{connection['base_path']}"
        params = {"symbol": identifier, "interval": "1d", "limit": days}
    elif source_name == 'alphavantage':
        url = f"https://{connection['host']}{connection['base_path']}"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": identifier,
            "outputsize": "compact",
            "apikey": connection['secret'],
        }
    else:
        raise ValueError(f"Непознат извор: {source_name}")
    return url, params


def apply_transform(value, transform_note, data_type):
    """Ги применува трансформациите означени во column_mapping табелата."""
    if value is None:
        return None
    if transform_note == 'unix_ms_to_timestamp':
        return datetime.utcfromtimestamp(value / 1000)
    if transform_note == 'unix_ms_to_date':
        return datetime.utcfromtimestamp(value / 1000).date()
    if transform_note == 'parse_yyyy_mm_dd':
        return datetime.strptime(value, "%Y-%m-%d").date()
    if data_type == 'float':
        return float(value)
    if data_type == 'integer':
        return int(float(value))
    return value


def extract_field(field_path, top_level_json, index, dict_key, record):
    """
    Ја чита вредноста според source_field_path од column_mapping.
    - '__dict_key__'  -> клучот на надворешниот dict (AlphaVantage датум)
    - 'row.N'         -> record[N] (Binance, record е самата свеќа-листа)
    - 'array[].N'     -> top_level_json[array][index][N] (CoinGecko, паралелни низи)
    - сѐ друго        -> record[field_path] (AlphaVantage вредности, литерален dict клуч)
    """
    if field_path == '__dict_key__':
        return dict_key
    if field_path.startswith('row.'):
        idx = int(field_path.split('.', 1)[1])
        return record[idx]
    if '[].' in field_path:
        array_name, idx_str = field_path.split('[].')
        idx = int(idx_str)
        arr = top_level_json.get(array_name, [])
        return arr[index][idx] if index < len(arr) else None
    return record.get(field_path) if record else None


def build_dynamic_insert(schema, table, row_values, pk_columns):
    """Гради ON CONFLICT UPSERT SQL динамички, според колоните што реално ги имаме."""
    columns = list(row_values.keys())
    update_columns = [c for c in columns if c not in pk_columns]

    col_list = ", ".join(columns + ["updated_at"])
    placeholders = ", ".join(["%s"] * len(columns)) + ", now()"
    conflict_cols = ", ".join(pk_columns)
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns) + ", updated_at = now()"

    sql = f"""
        INSERT INTO {schema}.{table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO UPDATE SET
            {update_clause}
    """
    return sql, list(row_values.values())


def ingest_generic(source_name, identifiers, cur, run_id):
    """
    Генеричка ingestion функција - работи за КОЈ И ДА Е извор,
    сѐ додека постојат редови за него во connections/column_mapping/table_mapping.
    """
    log.info(f"Starting {source_name} ingestion (generic)...")
    start = time.time()
    total_records = 0
    failed_identifiers = []

    connection = get_connection(cur, source_name)
    mapping = get_column_mapping(cur, source_name)
    table_info = get_table_mapping(cur, source_name)
    id_column = 'coin_id' if source_name == 'coingecko' else 'symbol'

    if source_name == 'alphavantage' and not connection['secret']:
        raise RuntimeError(
            "ALPHA_API_KEY is not set. Refusing to silently skip Alpha Vantage "
            "ingestion - fix the environment variable (see .env / docker-compose.yaml)."
        )

    days = get_days_to_fetch(cur)
    load_type = 'FULL' if days > 1 else 'INCREMENTAL'
    log.info(f"[{source_name.upper()}] Load type: {load_type} | Days: {days}")

    try:
        # СЕКОГАШ TRUNCATE - staging е секогаш свеж
        truncate_table(cur, table_info['table'])

        for identifier in identifiers:
            log.info(f"Fetching {identifier}...")

            url, params = build_request(source_name, connection, identifier, days)
            response = safe_get(url, params)

            if not response or response.status_code != 200:
                error_msg = f"HTTP failure (status={response.status_code if response else 'no response'})"
                log.error(f"Failed {identifier}")
                failed_identifiers.append(identifier)
                log_load_error(cur, run_id, source_name, identifier, error_msg)
                continue

            data = response.json()

            # Нормализирај го одговорот во листа од (index, dict_key, record) торки,
            # различно по извор бидејќи самите API одговори имаат различна форма.
            if source_name == 'coingecko':
                prices = data.get('prices', [])
                if not prices:
                    log.warning(f"No prices for {identifier}")
                    failed_identifiers.append(identifier)
                    log_load_error(cur, run_id, source_name, identifier, "No prices in response")
                    continue
                iterable = [(i, None, None) for i in range(len(prices))]
            elif source_name == 'binance':
                if not isinstance(data, list):
                    log.warning(f"Bad response for {identifier}: {data}")
                    failed_identifiers.append(identifier)
                    log_load_error(cur, run_id, source_name, identifier, f"Bad response shape: {data}")
                    continue
                iterable = [(i, None, row) for i, row in enumerate(data)]
            elif source_name == 'alphavantage':
                time_series = data.get('Time Series (Daily)', {})
                if not time_series:
                    log.warning(f"No Alpha Vantage data for {identifier} (rate limit or key issue)")
                    failed_identifiers.append(identifier)
                    log_load_error(cur, run_id, source_name, identifier, "No Time Series data (rate limit or key issue)")
                    continue
                iterable = [(None, date_str, values) for date_str, values in time_series.items()]
            else:
                raise ValueError(f"Непознат извор: {source_name}")

            for index, dict_key, record in iterable:
                try:
                    cur.execute("SAVEPOINT sp_generic_record")

                    row_values = {id_column: identifier}

                    for m in mapping:
                        raw_value = extract_field(
                            m['source_field_path'], data, index, dict_key, record
                        )
                        row_values[m['destination_column']] = apply_transform(
                            raw_value, m['transform_note'], m['data_type']
                        )

                    sql, values = build_dynamic_insert(
                        table_info['schema'], table_info['table'],
                        row_values, table_info['primary_key_columns']
                    )
                    cur.execute(sql, values)

                    cur.execute("RELEASE SAVEPOINT sp_generic_record")
                    total_records += 1

                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_generic_record")
                    log.error(f"Прескокнат лош запис за {identifier}: {e}")
                    continue

            log.info(f"{identifier} ingested ({source_name}, generic)")

        duration = time.time() - start

        if failed_identifiers and total_records > 0:
            status = 'partial_success'
            log.warning(
                f"[{source_name.upper()}] PARTIAL_SUCCESS - failed identifiers: {failed_identifiers}"
            )
        else:
            status = 'success'

        log_run(cur, run_id, source_name, status, total_records, duration)
        mark_load_complete(cur, table_info['table'], load_type)
        log.info(f"{source_name} (generic) done. Status: {status}")
        return total_records

    except Exception as e:
        duration = time.time() - start
        log_run(cur, run_id, source_name, 'failed', total_records, duration, str(e))
        log.error(f"{source_name} (generic) failed: {e}")
        raise


def main():
    run_id = uuid.uuid4()

    log.info("=" * 50)
    log.info(f"Starting ingestion at {datetime.now()}")
    log.info(f"Run ID: {run_id}")
    log.info("=" * 50)

    conn = psycopg2.connect(**DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        setup_tables(cur)
        conn.commit()

        ingest_generic('coingecko', get_active_identifiers(cur, 'coingecko'), cur, run_id)
        conn.commit()

        ingest_generic('binance', get_active_identifiers(cur, 'binance'), cur, run_id)
        conn.commit()

        ingest_generic('alphavantage', get_active_identifiers(cur, 'alphavantage'), cur, run_id)
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