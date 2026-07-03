{{ config(
    materialized='incremental',
    unique_key=['symbol', 'trade_date'],
    incremental_strategy='delete+insert'
) }}

with source as (

    select
        symbol,
        date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_volume,
        updated_at
    from {{ source('raw_data', 'airflow_binance_daily') }}
    
    {% if is_incremental() %}
    where date > (
        select coalesce(max(trade_date), '1900-01-01'::date)
        from {{ this }}
    )
    {% endif %}

),

metrics as (

    select
        symbol,
        date as trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_volume,

        round((close_price - open_price)::numeric, 6) as price_change,

        round(
            (((close_price - open_price) / nullif(open_price, 0)) * 100)::numeric,
            2
        ) as price_change_percent,

        round(
            (((high_price - low_price) / nullif(open_price, 0)) * 100)::numeric,
            2
        ) as volatility_pct,

        case
            when (((high_price - low_price) / nullif(open_price, 0)) * 100) < 2 then 'LOW'
            when (((high_price - low_price) / nullif(open_price, 0)) * 100) < 5 then 'MEDIUM'
            else 'HIGH'
        end as volatility_label,

        updated_at

    from source

)

select *
from metrics