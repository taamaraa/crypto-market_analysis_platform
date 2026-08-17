WITH crypto AS (
    SELECT DISTINCT ON (coin_id, price_date::date)
        coin_id,
        price_date::date AS date,
        price,
        market_cap,
        cg_volume AS volume,
        price_change_percent,
        high_price,
        low_price,
        open_price,
        'crypto' AS asset_type
    FROM {{ ref('int_crypto_prices') }}
    ORDER BY coin_id, price_date::date, ingested_at DESC
),

stocks AS (
    SELECT
        symbol AS coin_id,
        date,
        close AS price,
        NULL::float AS market_cap,
        volume::float AS volume,
        NULL::float AS price_change_percent,
        high AS high_price,
        low AS low_price,
        open AS open_price,
        'stock' AS asset_type
    FROM {{ source('raw_data', 'stg_alphavantage') }}
)

SELECT * FROM crypto
UNION ALL
SELECT * FROM stocks