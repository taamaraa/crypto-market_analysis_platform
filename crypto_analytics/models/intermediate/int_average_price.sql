{{ config(materialized='materialized_view') }}

WITH daily_prices AS (
    SELECT
        coin_id,
        price_date::date AS date,
        price
    FROM {{ ref('int_crypto_prices') }}
)

SELECT
    coin_id,
    date,
    price,
    AVG(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_price_7d,
    AVG(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS avg_price_14d,
    AVG(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_price_30d,
    MIN(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS min_price_30d,
    MAX(price) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS max_price_30d
FROM daily_prices