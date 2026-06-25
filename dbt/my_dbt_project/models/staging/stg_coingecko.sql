{{ config(materialized='view') }}

select
    coin_id,
    date,
    price,
    market_cap,
    total_volume,
    updated_at
from {{ source('raw_data', 'airflow_coingecko_daily') }}