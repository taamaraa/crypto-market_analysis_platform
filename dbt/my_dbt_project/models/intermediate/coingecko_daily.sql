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
from {{ ref('stg_coingecko') }}

{% if is_incremental() %}
where date >= (
    select coalesce(max(date), '1900-01-01'::date) 
    from {{ this }}
)
{% endif %}