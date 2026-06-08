{{ config(materialized='view') }}

with source as (
    select
        symbol,
        open_price,
        high_price,
        low_price,
        last_price,
        volume,
        quote_volume,
        price_change,
        price_change_percent,
        weighted_avg_price,
        open_time,
        close_time,
        updated_at
    from {{ ref('stg_binance') }}
),

metrics as (
    select
        symbol,
        open_price,
        high_price,
        low_price,
        last_price,
        volume,
        quote_volume,
        price_change,
        price_change_percent,
        weighted_avg_price,
        to_timestamp(open_time / 1000)::date  as trade_date,
        to_timestamp(close_time / 1000)::date as close_date,
        
        round(((high_price - low_price) / nullif(weighted_avg_price, 0) * 100)::numeric, 2) as volatility_pct,
        case
            when ((high_price - low_price) / nullif(weighted_avg_price, 0) * 100) < 2 then 'LOW'
            when ((high_price - low_price) / nullif(weighted_avg_price, 0) * 100) < 5 then 'MEDIUM'
            else 'HIGH'
        end as volatility_label,
        updated_at
    from source
)

select * from metrics