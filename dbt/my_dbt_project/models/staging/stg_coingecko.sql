{{ config(materialized='view') }}

select
    coin_id,
    prices,
    market_caps,
    total_volumes,
    updated_at
from {{ source('raw_data', 'airflow_coingecko') }}