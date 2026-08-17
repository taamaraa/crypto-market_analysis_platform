{{ config(materialized='table') }}

WITH base AS (
    SELECT
        coin_id,
        date,
        price,
        avg_price_30d,
        stddev_price_30d
    FROM {{ ref('int_price_volatility') }}
)

SELECT
    da.asset_key,
    dd.date_key,
    base.price,
    base.avg_price_30d,
    base.stddev_price_30d,
    CASE
        WHEN base.avg_price_30d IS NULL OR base.avg_price_30d = 0 THEN NULL
        ELSE base.stddev_price_30d / base.avg_price_30d
    END AS coefficient_of_variation,
    CASE
        WHEN base.stddev_price_30d IS NULL OR base.stddev_price_30d = 0 THEN NULL
        ELSE (base.price - base.avg_price_30d) / base.stddev_price_30d
    END AS z_score,
    CASE
        WHEN base.stddev_price_30d IS NULL OR base.stddev_price_30d = 0 THEN false
        WHEN ABS((base.price - base.avg_price_30d) / base.stddev_price_30d) > 2 THEN true
        ELSE false
    END AS is_anomaly,
    CASE
        WHEN base.avg_price_30d IS NULL OR base.avg_price_30d = 0 THEN NULL
        WHEN base.stddev_price_30d / base.avg_price_30d < 0.02 THEN 'low'
        WHEN base.stddev_price_30d / base.avg_price_30d < 0.05 THEN 'medium'
        ELSE 'high'
    END AS risk_level
FROM base
JOIN {{ ref('dim_asset') }} da
    ON base.coin_id = da.asset_id
    AND base.date >= da.valid_from
    AND (da.valid_to IS NULL OR base.date < da.valid_to)
JOIN {{ ref('dim_date') }} dd ON base.date = dd.full_date