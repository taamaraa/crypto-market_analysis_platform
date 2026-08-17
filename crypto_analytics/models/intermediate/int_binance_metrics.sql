{{ config(materialized='materialized_view') }}

with source as (
    select
        coin_id as symbol,
        price_date::date as trade_date,
        open_price,
        high_price,
        low_price,
        binance_last_price as close_price,
        binance_volume as volume,
        quote_volume
    from {{ ref('int_crypto_prices') }}
    where binance_last_price is not null
),

metrics as (
    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_volume,
        round((close_price - open_price)::numeric, 6) as price_change,
        round(
            (((close_price - open_price) / nullif(open_price, 0)) * 100)::numeric, 2
        ) as price_change_percent,
        round(
            (((high_price - low_price) / nullif(open_price, 0)) * 100)::numeric, 2
        ) as volatility_pct,
        case
            when (((high_price - low_price) / nullif(open_price, 0)) * 100) < 2 then 'LOW'
            when (((high_price - low_price) / nullif(open_price, 0)) * 100) < 5 then 'MEDIUM'
            else 'HIGH'
        end as volatility_label
    from source
)

select * from metrics