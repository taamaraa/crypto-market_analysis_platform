import json
import logging
import os
import time
import urllib.error
import urllib.request

import psycopg2

from config import DB_CONN, GEMINI_API_KEY

# A "-latest" alias avoids the model name going stale. Deliberately the lite
# variant: describing pre-computed numbers needs no reasoning, and the thinking
# models spend the whole output budget on thoughts and truncate the answer.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-flash-lite-latest")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "30"))
MAX_ATTEMPTS = 3

PERIOD_TYPE = "daily"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def setup_summaries_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_marts.market_summaries (
            id serial PRIMARY KEY,
            summary_date date NOT NULL,
            period_type varchar NOT NULL,
            summary_text text NOT NULL,
            generated_by varchar NOT NULL,
            created_at timestamptz DEFAULT now(),
            UNIQUE (summary_date, period_type)
        )
    """)
    log.info("market_summaries table ready.")


def get_as_of_date(cur):
    """Latest date with return data (int_asset_returns covers crypto + stock)."""
    cur.execute("select max(date) from public_intermediate.int_asset_returns")
    return cur.fetchone()[0]


def get_latest_prices(cur):
    """Most recent row per asset, joined to dim_asset (SCD2) for symbol/asset_type.
    daily_return in int_asset_returns is a fraction (0.025), multiplied by 100
    here so downstream formatting (fmt_pct) can stay identical to the original."""
    cur.execute("""
        select da.symbol, da.asset_type, ranked.price,
               ranked.daily_return * 100 as daily_return_pct, ranked.date
        from (
            select
                coin_id,
                date,
                price,
                daily_return,
                row_number() over (partition by coin_id order by date desc) as rn
            from public_intermediate.int_asset_returns
        ) ranked
        join public_marts.dim_asset da
            on ranked.coin_id = da.asset_id
            and ranked.date >= da.valid_from
            and (da.valid_to is null or ranked.date < da.valid_to)
        where ranked.rn = 1
        order by da.asset_type, da.symbol
    """)
    return cur.fetchall()


def get_alerts(cur, as_of_date):
    """market_alerts_log has no symbol column -- join dim_asset on asset_key
    (the surrogate key already pins the exact SCD2 version, so no date-range
    condition is needed here)."""
    cur.execute("""
        select
            mal.alert_type,
            count(*) as alert_count,
            string_agg(distinct da.symbol, ', ' order by da.symbol) as symbols
        from public.market_alerts_log mal
        join public_marts.dim_asset da on mal.asset_key = da.asset_key
        where mal.alert_date = %s
        group by mal.alert_type
        order by mal.alert_type
    """, (as_of_date,))
    return cur.fetchall()


def get_correlations(cur):
    """fact_asset_correlation has no pair_type column, so it's derived here via
    a CASE on the two joined dim_asset.asset_type values (only 'Crypto'/'Stock'
    exist, so a mismatch always means 'Crypto-Stock')."""
    cur.execute("""
        with latest as (
            select
                case
                    when da.asset_type = db.asset_type then da.asset_type || '-' || db.asset_type
                    else 'Crypto-Stock'
                end as pair_type,
                fac.corr_30d,
                row_number() over (
                    partition by fac.asset_key_a, fac.asset_key_b order by fac.date_key desc
                ) as rn
            from public_marts.fact_asset_correlation fac
            join public_marts.dim_asset da on fac.asset_key_a = da.asset_key
            join public_marts.dim_asset db on fac.asset_key_b = db.asset_key
            where fac.corr_30d is not null
        )
        select
            pair_type,
            round(avg(corr_30d)::numeric, 2) as avg_correlation,
            count(*) as pair_count
        from latest
        where rn = 1
        group by pair_type
        order by pair_type
    """)
    return cur.fetchall()


def gather_snapshot(cur):
    as_of_date = get_as_of_date(cur)
    if as_of_date is None:
        raise RuntimeError("int_asset_returns is empty -- run dbt before this task.")

    snapshot = {
        "as_of_date": as_of_date,
        "prices": get_latest_prices(cur),
        "alerts": get_alerts(cur, as_of_date),
        "correlations": get_correlations(cur),
    }
    log.info(
        "Snapshot for %s: %d assets, %d alert group(s), %d pair type(s).",
        as_of_date,
        len(snapshot["prices"]),
        len(snapshot["alerts"]),
        len(snapshot["correlations"]),
    )
    return snapshot


def _display_symbol(symbol):
    """Crypto assets are stored lowercase in dim_asset ('cardano', 'bitcoin');
    stock tickers are already uppercase ('AAPL', 'TSLA'). Capitalizing only
    the first character handles both without lowercasing a ticker."""
    return symbol[:1].upper() + symbol[1:]


def fmt_price(price):
    if price is None:
        return "n/a"
    price = float(price)
    return f"${price:,.2f}" if price >= 1 else f"${price:,.4f}"


def fmt_pct(pct):
    return "n/a" if pct is None else f"{float(pct):+.2f}%"


def build_prompt(snapshot):
    """Every number is pre-formatted here. The model is asked to describe
    them, never to compute anything, so it cannot produce a wrong figure."""
    lines = [f"Market data for {snapshot['as_of_date']}:", ""]

    for asset_type in ("Crypto", "Stock"):
        rows = [r for r in snapshot["prices"] if r[1] == asset_type]
        if not rows:
            continue
        lines.append(f"{asset_type.upper()}:")
        for symbol, _, price, return_pct, date in rows:
            suffix = "" if date == snapshot["as_of_date"] else f" [as of {date}]"
            lines.append(
                f"  {_display_symbol(symbol)}: {fmt_price(price)} ({fmt_pct(return_pct)}){suffix}"
            )
        lines.append("")

    if snapshot["alerts"]:
        total = sum(row[1] for row in snapshot["alerts"])
        lines.append(f"ACTIVE ALERTS ({total}):")
        for alert_type, count, symbols in snapshot["alerts"]:
            display_symbols = ", ".join(
                _display_symbol(s.strip()) for s in symbols.split(",")
            )
            lines.append(f"  {alert_type} x{count}: {display_symbols}")
    else:
        lines.append("ACTIVE ALERTS: none")
    lines.append("")

    if snapshot["correlations"]:
        lines.append("30-DAY RETURN CORRELATION (latest per pair, averaged):")
        for pair_type, avg_correlation, pair_count in snapshot["correlations"]:
            label = "pair" if pair_count == 1 else "pairs"
            lines.append(
                f"  {pair_type}: {float(avg_correlation):.2f} ({pair_count} {label})"
            )
        lines.append("")

    lines.extend([
        "Write a 3-4 sentence summary of this trading day for a dashboard card.",
        "",
        "Rules:",
        "- Use ONLY the numbers above. Do not calculate or infer new figures.",
        "- Plain text only. No markdown, no asterisks, no bullet points, no headings.",
        "- No disclaimers and no investment advice.",
        "- Maximum 80 words.",
        "- Mention the correlation pattern if it is notable.",
    ])
    return "\n".join(lines)


def call_gemini(prompt):
    """Returns the generated text, or None if the API could not be reached.
    Uses the REST endpoint so no extra pip package is needed, and passes the
    key as a header rather than a query parameter."""
    if not GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY not set, skipping the API call.")
        return None

    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 400,
        },
    }).encode("utf-8")

    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(
            GEMINI_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": GEMINI_API_KEY,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=GEMINI_TIMEOUT) as response:
                body = json.loads(response.read().decode("utf-8"))

            candidate = body["candidates"][0]
            finish_reason = candidate.get("finishReason")

            # A thinking model can burn the whole token budget on thoughts and
            # return a sentence cut off mid-word. Treat anything that did not
            # finish cleanly as a failure so the fallback is used instead of
            # storing a truncated summary.
            if finish_reason != "STOP":
                log.error(
                    "Gemini stopped with finishReason=%s (model '%s', "
                    "%s thinking tokens). Discarding the partial answer.",
                    finish_reason,
                    GEMINI_MODEL,
                    body.get("usageMetadata", {}).get("thoughtsTokenCount", 0),
                )
                return None

            # Skip thought parts and join the rest -- some models return the
            # answer split across several parts.
            text = "".join(
                part.get("text", "")
                for part in candidate["content"]["parts"]
                if not part.get("thought")
            ).strip()

            if not text:
                log.error("Gemini returned an empty answer.")
                return None

            return text

        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            # 429 and 5xx are worth retrying; 400/401/404 will not fix
            # themselves, so fail fast and let the fallback take over.
            if exc.code == 429 or exc.code >= 500:
                log.warning(
                    "Gemini returned %s (attempt %d/%d): %s",
                    exc.code, attempt, MAX_ATTEMPTS, detail,
                )
            else:
                log.error(
                    "Gemini returned %s for model '%s': %s",
                    exc.code, GEMINI_MODEL, detail,
                )
                return None

        except (urllib.error.URLError, TimeoutError) as exc:
            log.warning(
                "Could not reach Gemini (attempt %d/%d): %s",
                attempt, MAX_ATTEMPTS, exc,
            )

        except (KeyError, IndexError, ValueError) as exc:
            log.error("Unexpected Gemini response shape: %s", exc)
            return None

        if attempt < MAX_ATTEMPTS:
            time.sleep(2 ** attempt)

    log.error("Gemini unavailable after %d attempts.", MAX_ATTEMPTS)
    return None


def fallback_summary(snapshot):
    """Deterministic summary built straight from the snapshot, so the
    dashboard card is never empty when the API is unavailable."""
    ranked = [r for r in snapshot["prices"] if r[3] is not None]
    ranked.sort(key=lambda row: float(row[3]), reverse=True)

    parts = [f"Market summary for {snapshot['as_of_date']}."]

    if ranked:
        best_symbol, _, _, best_pct, _ = ranked[0]
        worst_symbol, _, _, worst_pct, _ = ranked[-1]
        parts.append(
            f"Best performer: {_display_symbol(best_symbol)} at {fmt_pct(best_pct)}. "
            f"Weakest: {_display_symbol(worst_symbol)} at {fmt_pct(worst_pct)}."
        )

    if snapshot["alerts"]:
        total = sum(row[1] for row in snapshot["alerts"])
        types = ", ".join(row[0] for row in snapshot["alerts"])
        parts.append(f"{total} alert(s) active ({types}).")
    else:
        parts.append("No alerts active.")

    correlations = {row[0]: float(row[1]) for row in snapshot["correlations"]}
    if "Crypto-Crypto" in correlations:
        detail = f"Average 30-day crypto correlation: {correlations['Crypto-Crypto']:.2f}"
        if "Crypto-Stock" in correlations:
            detail += f", crypto-to-stock: {correlations['Crypto-Stock']:.2f}"
        parts.append(detail + ".")

    return " ".join(parts)


def save_summary(cur, summary_date, summary_text, generated_by):
    cur.execute("""
        INSERT INTO public_marts.market_summaries
            (summary_date, period_type, summary_text, generated_by)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (summary_date, period_type) DO UPDATE
        SET summary_text = excluded.summary_text,
            generated_by = excluded.generated_by,
            created_at = now()
    """, (summary_date, PERIOD_TYPE, summary_text, generated_by))
    log.info("Saved %s summary for %s (source: %s).",
             PERIOD_TYPE, summary_date, generated_by)


def run():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    setup_summaries_table(cur)
    conn.commit()

    snapshot = gather_snapshot(cur)
    prompt = build_prompt(snapshot)

    summary_text = call_gemini(prompt)
    generated_by = "gemini"

    if not summary_text:
        summary_text = fallback_summary(snapshot)
        generated_by = "fallback"
        log.warning("Using the deterministic fallback summary.")

    save_summary(cur, snapshot["as_of_date"], summary_text, generated_by)
    conn.commit()

    cur.close()
    conn.close()
    log.info("AI summary complete.")
    log.info("Summary: %s", summary_text)


if __name__ == "__main__":
    run()