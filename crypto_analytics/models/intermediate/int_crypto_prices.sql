{{
    config(
        materialized='incremental',
        unique_key=['coin_id', 'price_date'],
        incremental_strategy='delete+insert',
        post_hook="
            INSERT INTO public.etl_control (table_name, last_loaded_at)
            VALUES ('int_crypto_prices', (SELECT MAX(ingested_at) FROM {{ this }}))
            ON CONFLICT (table_name) 
            DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at
        "
    )
}}

WITH coingecko AS (
    SELECT
        coin_id,
        ingested_at,
        price_date,
        price,
        market_cap,
        volume AS cg_volume
    FROM {{ source('raw_data', 'stg_coingecko') }}
),
binance AS (
    SELECT
        symbol,
        date,
        close_price AS last_price,
        volume AS binance_volume,
        high_price,
        low_price,
        open_price,
        quote_volume
    FROM {{ source('raw_data', 'stg_binance') }}
)
SELECT
    c.coin_id,
    c.ingested_at,
    c.price_date,
    c.price,
    c.market_cap,
    c.cg_volume,
    b.last_price AS binance_last_price,
    b.binance_volume,
    CASE
        WHEN b.open_price IS NOT NULL AND b.open_price != 0
        THEN ((b.last_price - b.open_price) / b.open_price) * 100
        ELSE NULL
    END AS price_change_percent,
    b.high_price,
    b.low_price,
    b.open_price,
    b.quote_volume
FROM coingecko c
LEFT JOIN binance b
    ON (
        (c.coin_id = 'bitcoin'      AND b.symbol = 'BTCUSDT') OR
        (c.coin_id = 'ethereum'     AND b.symbol = 'ETHUSDT') OR
        (c.coin_id = 'solana'       AND b.symbol = 'SOLUSDT') OR
        (c.coin_id = 'binancecoin'  AND b.symbol = 'BNBUSDT') OR
        (c.coin_id = 'cardano'      AND b.symbol = 'ADAUSDT')
    )
    AND c.price_date::date = b.date

{% if is_incremental() %}
WHERE c.ingested_at > COALESCE(
    (SELECT last_loaded_at::date FROM public.etl_control WHERE table_name = 'int_crypto_prices'),
    '1970-01-01'::date
)
{% endif %}