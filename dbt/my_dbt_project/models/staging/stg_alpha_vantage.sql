{{ config(materialized='view') }}

select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    updated_at
from {{ source('raw_data', 'airflow_alpha_vantage') }}