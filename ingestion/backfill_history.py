"""One-off manual backfill: pulls 200 days of history for CoinGecko + Binance
(both sources together, so the int_crypto_prices join stays date-aligned)
instead of the watermark-limited 1-30 days get_days_to_fetch() normally
allows. Reuses ingest_generic() as-is via a temporary monkeypatch of
get_days_to_fetch, so the exact same tested fetch/parse/upsert logic runs --
only the day count differs. Not part of the DAG; run manually once, then
delete.

Usage: python ingestion/backfill_history.py
"""

import uuid

import ingest
from config import DB_CONN
import psycopg2

BACKFILL_DAYS = 200


def run():
    conn = psycopg2.connect(**DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()
    run_id = uuid.uuid4()

    original_get_days_to_fetch = ingest.get_days_to_fetch
    ingest.get_days_to_fetch = lambda cur: BACKFILL_DAYS

    try:
        ingest.setup_tables(cur)
        conn.commit()

        for source in ("coingecko", "binance"):
            identifiers = ingest.get_active_identifiers(cur, source)
            total = ingest.ingest_generic(source, identifiers, cur, run_id)
            conn.commit()
            print(f"{source}: {total} records backfilled.")

    finally:
        ingest.get_days_to_fetch = original_get_days_to_fetch
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()
