{{ config(materialized='view') }}

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
from {{ source('raw_data', 'airflow_binance') }}