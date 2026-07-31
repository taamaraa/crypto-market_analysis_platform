"""Daily price forecasting: naive, ETS and XGBoost, plus their ensemble.

Two modes:
    python scripts/forecast.py             predict tomorrow (the Airflow task)
    python scripts/forecast.py backtest    walk-forward over history

Trains only on completed days. Binance closes its daily candle at 00:00 UTC
while the DAG runs at 07:45 UTC, so the row for the current date is always a
partial candle -- its volume is roughly half a day and its close is just
"the price right now". Training on that teaches the model the ingest
schedule rather than the market.
"""

import logging
import sys

import numpy as np
import pandas as pd
import psycopg2

from config import DB_CONN

HORIZON_DAYS = 1
BACKTEST_MIN_TRAIN = 150      # days of history before the first backtest step
XGB_MIN_ROWS = 300            # below this a tree model is pure overfitting

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def setup_forecast_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_marts.fact_price_forecast (
            id serial PRIMARY KEY,
            symbol varchar NOT NULL,
            target_date date NOT NULL,
            model_name varchar NOT NULL,
            horizon_days int NOT NULL DEFAULT 1,
            predicted_price numeric NOT NULL,
            model_params varchar,
            generated_at timestamptz DEFAULT now(),
            UNIQUE (symbol, target_date, model_name, horizon_days)
        )
    """)
    log.info("fact_price_forecast table ready.")


def load_panel(conn):
    """All completed daily candles for the five Binance crypto pairs.

    quote_volume rather than volume: it is denominated in USDT, so it stays
    comparable across assets in the pooled model, while volume is in base
    asset units (BTC vs ADA).
    """
    panel = pd.read_sql("""
        select symbol, trade_date, close_price, quote_volume, volatility_pct
        from public_intermediate.binance_metrics
        where trade_date < current_date
        order by symbol, trade_date
    """, conn)

    for col in ("close_price", "quote_volume", "volatility_pct"):
        panel[col] = panel[col].astype(float)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])

    log.info(
        "Loaded %d rows, %d symbols, %s to %s.",
        len(panel), panel["symbol"].nunique(),
        panel["trade_date"].min().date(), panel["trade_date"].max().date(),
    )
    return panel


# ---------------------------------------------------------------- features

def make_features(panel):
    """One row per symbol/day. Every feature uses data up to and including
    that day; the target is the return from that day to the next.

    target_ret is the only column that looks forward, and it is the label.
    If anything else ever does, the backtest silently becomes meaningless.
    """
    frames = []

    for _, group in panel.groupby("symbol"):
        g = group.sort_values("trade_date").copy()

        g["ret"] = g["close_price"].pct_change()
        g["ret_lag1"] = g["ret"].shift(1)
        g["ret_lag2"] = g["ret"].shift(2)
        g["ret_lag6"] = g["ret"].shift(6)

        g["ma_7"] = g["close_price"].rolling(7).mean()
        g["ma_30"] = g["close_price"].rolling(30).mean()
        g["price_vs_ma7"] = g["close_price"] / g["ma_7"] - 1
        g["price_vs_ma30"] = g["close_price"] / g["ma_30"] - 1

        g["vol_ma_7"] = g["volatility_pct"].rolling(7).mean()
        g["volume_ratio"] = g["quote_volume"] / g["quote_volume"].rolling(30).mean()
        g["dow"] = g["trade_date"].dt.dayofweek

        g["target_ret"] = g["ret"].shift(-1)

        frames.append(g)

    return pd.concat(frames, ignore_index=True)


FEATURE_COLUMNS = [
    "ret", "ret_lag1", "ret_lag2", "ret_lag6",
    "price_vs_ma7", "price_vs_ma30",
    "volatility_pct", "vol_ma_7", "volume_ratio", "dow",
]


# ------------------------------------------------------------------ models

def forecast_naive(history):
    """Tomorrow equals today. On a random walk this is the optimal forecast,
    which is exactly why it is the yardstick for everything else."""
    return float(history["close_price"].iloc[-1]), None


def _ets_level(prices, alpha):
    level = prices[0]
    for price in prices[1:]:
        level = alpha * price + (1 - alpha) * level
    return level


def _fit_alpha(prices):
    """Grid search for the alpha with the smallest one-step-ahead error.

    An alpha near 1 means only yesterday carries information. That result is
    itself the finding -- it says the series has no usable memory -- rather
    than a failure of the model.
    """
    best_alpha, best_sse = 0.5, float("inf")

    for alpha in np.arange(0.05, 1.001, 0.05):
        level, sse = prices[0], 0.0
        for price in prices[1:]:
            sse += (price - level) ** 2
            level = alpha * price + (1 - alpha) * level
        if sse < best_sse:
            best_sse, best_alpha = sse, alpha

    return float(best_alpha)


def forecast_ets(history):
    """Simple exponential smoothing, written out rather than pulled from
    statsmodels: it is a dozen lines, it keeps scipy out of the image, and
    the fitted alpha stays visible instead of hiding inside a fitted object.
    """
    prices = history["close_price"].to_numpy(dtype=float)
    alpha = _fit_alpha(prices)
    return float(_ets_level(prices, alpha)), f"alpha={alpha:.2f}"


def forecast_xgboost(train_features, predict_rows):
    """One global model across all five assets. Returns are comparable
    between them, so pooling turns ~180 rows per asset into ~900 -- the
    single biggest win available on a series this short.

    Predicts the next-day return, never the price: tree models cannot
    extrapolate past the range they were trained on, so a price model would
    refuse to forecast above its training maximum.
    """
    try:
        import xgboost as xgb
    except ImportError:
        log.warning("xgboost is not installed, skipping this model.")
        return {}

    train = train_features.dropna(subset=FEATURE_COLUMNS + ["target_ret"])
    if len(train) < XGB_MIN_ROWS:
        log.warning("Only %d training rows, skipping xgboost.", len(train))
        return {}

    usable = predict_rows.dropna(subset=FEATURE_COLUMNS)
    if usable.empty:
        return {}

    # The native API rather than XGBRegressor: the sklearn-style wrapper pulls
    # in scikit-learn purely for syntax, and this needs no more code.
    params = {
        "objective": "reg:squarederror",
        "max_depth": 3,          # shallow on purpose -- little data
        "eta": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "seed": 42,
    }
    booster = xgb.train(
        params,
        xgb.DMatrix(train[FEATURE_COLUMNS], label=train["target_ret"]),
        num_boost_round=200,
    )
    predicted_returns = booster.predict(xgb.DMatrix(usable[FEATURE_COLUMNS]))

    out = {}
    for (_, row), predicted_return in zip(usable.iterrows(), predicted_returns):
        price = float(row["close_price"]) * (1 + float(predicted_return))
        out[row["symbol"]] = (price, f"ret={float(predicted_return):+.4f}")
    return out


# --------------------------------------------------------------- one round

def predict_for_cutoff(panel, features, cutoff_date, target_date):
    """Every prediction for one target date, using only data up to cutoff."""
    history = panel[panel["trade_date"] <= cutoff_date]
    train_features = features[features["trade_date"] < cutoff_date]
    predict_rows = features[features["trade_date"] == cutoff_date]

    results = []

    for symbol, group in history.groupby("symbol"):
        if len(group) < 30:
            continue
        for model_name, fn in (("naive", forecast_naive), ("ets", forecast_ets)):
            price, params = fn(group)
            results.append((symbol, target_date, model_name, price, params))

    for symbol, (price, params) in forecast_xgboost(train_features, predict_rows).items():
        results.append((symbol, target_date, "xgboost", price, params))

    # Averaging with naive shrinks each model's deviation from "no change".
    # On a near-random-walk series most of that deviation is noise, so the
    # ensemble is often the most accurate entry in the table.
    by_symbol = {}
    for symbol, _, model_name, price, _ in results:
        by_symbol.setdefault(symbol, {})[model_name] = price

    for symbol, prices in by_symbol.items():
        if len(prices) >= 2:
            results.append((
                symbol, target_date, "ensemble",
                float(np.mean(list(prices.values()))),
                "+".join(sorted(prices)),
            ))

    return results


def save_forecasts(cur, rows):
    for symbol, target_date, model_name, price, params in rows:
        cur.execute("""
            INSERT INTO public_marts.fact_price_forecast
                (symbol, target_date, model_name, horizon_days,
                 predicted_price, model_params)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, target_date, model_name, horizon_days)
            DO UPDATE SET predicted_price = excluded.predicted_price,
                          model_params = excluded.model_params,
                          generated_at = now()
        """, (symbol, target_date, model_name, HORIZON_DAYS,
              round(price, 8), params))


# ------------------------------------------------------------------- modes

def run_daily(conn, cur, panel, features):
    cutoff = panel["trade_date"].max()
    target = (cutoff + pd.Timedelta(days=1)).date()

    rows = predict_for_cutoff(panel, features, cutoff, target)
    save_forecasts(cur, rows)
    conn.commit()

    log.info("Wrote %d forecasts for %s.", len(rows), target)
    for symbol, _, model_name, price, params in sorted(rows):
        log.info("  %-9s %-9s %12.4f  %s", symbol, model_name, price, params or "")


def run_backtest(conn, cur, panel, features):
    """Re-forecast the past one day at a time, each step seeing only what was
    known then. Turns the history into ~60 honest predictions per asset
    instead of waiting two months to collect them."""
    dates = sorted(panel["trade_date"].unique())
    steps = dates[BACKTEST_MIN_TRAIN:-1]

    if not steps:
        raise RuntimeError(
            f"Only {len(dates)} days of history, need more than "
            f"{BACKTEST_MIN_TRAIN + 1} to backtest."
        )

    log.info("Backtesting %d steps (%s to %s).",
             len(steps), pd.Timestamp(steps[0]).date(), pd.Timestamp(steps[-1]).date())

    total = 0
    for i, cutoff in enumerate(steps, start=1):
        target = pd.Timestamp(dates[dates.index(cutoff) + 1]).date()
        rows = predict_for_cutoff(panel, features, cutoff, target)
        save_forecasts(cur, rows)
        total += len(rows)

        if i % 10 == 0 or i == len(steps):
            conn.commit()
            log.info("  step %d/%d, %d forecasts so far.", i, len(steps), total)

    conn.commit()
    log.info("Backtest complete: %d forecasts.", total)


def run():
    backtest = len(sys.argv) > 1 and sys.argv[1] == "backtest"

    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    setup_forecast_table(cur)
    conn.commit()

    panel = load_panel(conn)
    if panel.empty:
        raise RuntimeError("binance_metrics is empty -- run dbt before this task.")

    features = make_features(panel)

    if backtest:
        run_backtest(conn, cur, panel, features)
    else:
        run_daily(conn, cur, panel, features)

    cur.close()
    conn.close()
    log.info("Forecast run complete.")


if __name__ == "__main__":
    run()
