{{ config(materialized='materialized_view') }}

WITH daily_volume AS (
    SELECT
        coin_id,
        date,
        volume,
        asset_type
    FROM {{ ref('int_market_comparison') }}
)

SELECT
    coin_id,
    date,
    volume,
    asset_type,
    AVG(volume) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_volume_7d,
    AVG(volume) OVER (
        PARTITION BY coin_id
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_volume_30d
FROM daily_volume