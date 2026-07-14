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
-- lookback window instead of a strict `>` watermark: the newest days get
-- restated in raw (partial-day prices corrected by the next ingest run),
-- and delete+insert on the unique_key dedupes the overlap
where date >= (
    select coalesce(max(date), '1900-01-01'::date) - 3
    from {{ this }}
)
{% endif %}