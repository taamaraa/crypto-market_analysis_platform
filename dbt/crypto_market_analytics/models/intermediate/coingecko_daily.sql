{{ config(
    materialized='incremental',
    unique_key=['coin_id','date'],
    incremental_strategy='delete+insert'

) }}

select
    coin_id,
    date,
    price,
    market_cap,
    total_volume
from {{ source('raw_data', 'airflow_coingecko_daily') }}

{% if is_incremental() %}
where date >= (
    select coalesce(max(date), '1900-01-01'::date) - 3
    from {{ this }}
)
{% endif %}