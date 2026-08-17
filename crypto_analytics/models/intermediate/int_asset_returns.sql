{{ config(materialized='materialized_view') }}

WITH daily_price AS (
    SELECT
        coin_id,
        date,
        price
    FROM {{ ref('int_market_comparison') }}
),

with_return AS (
    SELECT
        coin_id,
        date,
        price,
        LAG(price) OVER (PARTITION BY coin_id ORDER BY date) AS prev_price
    FROM daily_price
)

SELECT
    coin_id,
    date,
    price,
    CASE
        WHEN prev_price IS NULL OR prev_price = 0 THEN NULL
        ELSE (price - prev_price) / prev_price
    END AS daily_return
FROM with_return