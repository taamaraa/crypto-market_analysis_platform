{{ config(materialized='table') }}

WITH base AS (
    SELECT
        coin_id,
        date,
        price,
        avg_price_7d,
        avg_price_14d,
        avg_price_30d,
        min_price_30d,
        max_price_30d
    FROM {{ ref('int_average_price') }}
)

SELECT
    da.asset_key,
    dd.date_key,
    base.price,
    base.avg_price_7d,
    base.avg_price_14d,
    base.avg_price_30d,
    base.min_price_30d,
    base.max_price_30d
FROM base
JOIN {{ ref('dim_asset') }} da
    ON base.coin_id = da.asset_id
    AND base.date >= da.valid_from
    AND (da.valid_to IS NULL OR base.date < da.valid_to)
JOIN {{ ref('dim_date') }} dd ON base.date = dd.full_date