"""Model observability for forecast.py: scores every forecast in
fact_price_forecast that has a matching actual value in int_binance_metrics,
and aggregates MAE/RMSE per (target_name, model_name).

Re-scores everything available each run (not incremental) -- the table this
writes is small (one row per target/model combo) and cheap to recompute, and
recomputing from scratch means a fixed threshold or model change is reflected
immediately rather than blending with stale numbers from a previous run.

Only scores completed days (trade_date < current_date): today's Binance
candle is partial, so comparing a forecast against it would blame the model
for a target that hasn't finished happening yet.

Usage: python ingestion/score_forecasts.py
"""

import logging

import psycopg2

from config import DB_CONN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def setup_accuracy_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_marts.fact_forecast_accuracy (
            id serial PRIMARY KEY,
            target_name varchar NOT NULL,
            model_name varchar NOT NULL,
            n_predictions int NOT NULL,
            mae numeric NOT NULL,
            rmse numeric NOT NULL,
            scored_through date NOT NULL,
            computed_at timestamptz DEFAULT now(),
            UNIQUE (target_name, model_name)
        )
    """)
    log.info("fact_forecast_accuracy table ready.")


def score(cur):
    """Joins every forecast to its actual outcome (by symbol + target_date)
    and aggregates error per target/model. actual_value is picked per
    target_name since each target reads a different column from
    int_binance_metrics."""
    cur.execute("""
        with actuals as (
            select symbol, trade_date, close_price, volatility_pct, quote_volume
            from public_intermediate.int_binance_metrics
            where trade_date < current_date
        ),
        scored as (
            select
                f.target_name,
                f.model_name,
                f.target_date,
                f.predicted_value,
                case f.target_name
                    when 'price' then a.close_price
                    when 'volatility' then a.volatility_pct
                    when 'volume' then a.quote_volume
                end as actual_value
            from public_marts.fact_price_forecast f
            join actuals a
                on f.symbol = a.symbol
                and f.target_date = a.trade_date
        )
        select
            target_name,
            model_name,
            count(*) as n_predictions,
            round(avg(abs(predicted_value - actual_value))::numeric, 6) as mae,
            round(sqrt(avg(power(predicted_value - actual_value, 2)))::numeric, 6) as rmse,
            max(target_date) as scored_through
        from scored
        where actual_value is not null
        group by target_name, model_name
        order by target_name, model_name
    """)
    return cur.fetchall()


def save_accuracy(cur, rows):
    for target_name, model_name, n_predictions, mae, rmse, scored_through in rows:
        cur.execute("""
            INSERT INTO public_marts.fact_forecast_accuracy
                (target_name, model_name, n_predictions, mae, rmse, scored_through)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (target_name, model_name) DO UPDATE
            SET n_predictions = excluded.n_predictions,
                mae = excluded.mae,
                rmse = excluded.rmse,
                scored_through = excluded.scored_through,
                computed_at = now()
        """, (target_name, model_name, n_predictions, mae, rmse, scored_through))


def run():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    setup_accuracy_table(cur)
    conn.commit()

    rows = score(cur)
    if not rows:
        log.warning("No scoreable forecasts yet (no forecast has a matured actual value).")
        cur.close()
        conn.close()
        return

    save_accuracy(cur, rows)
    conn.commit()

    log.info("Scored %d (target, model) combinations.", len(rows))
    log.info("%-11s %-9s %10s %12s %12s", "target", "model", "n", "mae", "rmse")
    for target_name, model_name, n_predictions, mae, rmse, _ in rows:
        log.info("%-11s %-9s %10d %12s %12s", target_name, model_name, n_predictions, mae, rmse)

    cur.close()
    conn.close()
    log.info("Forecast scoring complete.")


if __name__ == "__main__":
    run()