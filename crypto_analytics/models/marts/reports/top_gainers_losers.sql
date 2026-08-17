{{ config(materialized='table') }}

WITH daily_changes AS (
    SELECT
        a.asset_id,
        a.symbol,
        a.asset_type,
        d.full_date AS trade_date,
        f.price AS close_price,
        f.high_price,
        f.low_price,
        f.volume,
        LAG(f.price) OVER (
            PARTITION BY f.asset_key 
            ORDER BY d.full_date
        ) AS prev_price
    FROM {{ ref('fact_prices') }} f
    JOIN {{ ref('dim_asset') }} a ON f.asset_key = a.asset_key
    JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
),

ranked AS (
    SELECT
        asset_id,
        symbol,
        asset_type,
        trade_date,
        close_price,
        high_price,
        low_price,
        volume,
        prev_price,
        ROUND(((close_price - prev_price) / NULLIF(prev_price, 0) * 100)::numeric, 2) AS price_change_pct,
        ROUND((close_price - prev_price)::numeric, 2) AS price_change_abs,
        RANK() OVER (PARTITION BY trade_date ORDER BY ((close_price - prev_price) / NULLIF(prev_price, 0)) DESC) AS gainer_rank,
        RANK() OVER (PARTITION BY trade_date ORDER BY ((close_price - prev_price) / NULLIF(prev_price, 0)) ASC) AS loser_rank
    FROM daily_changes
    WHERE prev_price IS NOT NULL
)

SELECT
    asset_id,
    symbol,
    asset_type,
    trade_date,
    close_price,
    prev_price,
    price_change_abs,
    price_change_pct,
    volume,
    gainer_rank,
    loser_rank,
    CASE
        WHEN gainer_rank = 1 AND price_change_pct > 0 THEN 'TOP GAINER'
        WHEN loser_rank = 1 AND price_change_pct < 0  THEN 'TOP LOSER'
        WHEN price_change_pct > 0                      THEN 'GAINER'
        WHEN price_change_pct < 0                      THEN 'LOSER'
        ELSE 'NEUTRAL'
    END AS performance_label
FROM ranked
ORDER BY trade_date DESC, price_change_pct DESC