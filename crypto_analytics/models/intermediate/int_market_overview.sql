SELECT
    coin_id,
    ingested_at,
    COUNT(*)                    AS data_points,
    AVG(price)                  AS avg_price,
    MIN(price)                  AS min_price,
    MAX(price)                  AS max_price,
    AVG(market_cap)             AS avg_market_cap,
    SUM(cg_volume)              AS total_volume,
    AVG(price_change_percent)   AS avg_price_change_pct,
    MAX(high_price)             AS daily_high,
    MIN(low_price)              AS daily_low
FROM {{ ref('int_crypto_prices') }}
GROUP BY coin_id, ingested_at
ORDER BY ingested_at DESC, coin_id