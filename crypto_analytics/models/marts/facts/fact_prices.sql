{{ config(materialized='table') }}

WITH base AS (
    SELECT
        coin_id,
        date,
        price,
        volume,
        market_cap,
        high_price,
        low_price,
        open_price,
        asset_type,
        CASE
            WHEN asset_type = 'crypto' THEN 'CoinGecko'
            WHEN asset_type = 'stock' THEN 'AlphaVantage'
        END AS source_name
    FROM {{ ref('int_market_comparison') }}
)

SELECT
    da.asset_key,
    dd.date_key,
    ds.source_key,
    base.price,
    base.volume,
    base.market_cap,
    base.high_price,
    base.low_price,
    base.open_price
FROM base
JOIN {{ ref('dim_asset') }} da
    ON base.coin_id = da.asset_id
    AND base.date >= da.valid_from
    AND (da.valid_to IS NULL OR base.date < da.valid_to)
JOIN {{ ref('dim_date') }} dd ON base.date = dd.full_date
JOIN {{ ref('dim_source') }} ds ON base.source_name = ds.source_name