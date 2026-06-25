{{ config(materialized='view') }}

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