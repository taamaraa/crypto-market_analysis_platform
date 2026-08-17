{{ config(materialized='materialized_view') }}

WITH daily_price AS (
    SELECT
        coin_id,
        date,
        price,
        asset_type
    FROM {{ ref('int_market_comparison') }}
)

SELECT
    coin_id,
    date,
    price,
    asset_type,
    AVG(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_price_7d,
    STDDEV(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS stddev_price_7d,
    AVG(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_price_30d,
    STDDEV(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS stddev_price_30d
FROM daily_price