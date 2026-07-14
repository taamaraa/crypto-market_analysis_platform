{{ config(
    materialized='incremental',
    unique_key=['asset_key', 'date_key', 'source_key'],
    incremental_strategy='delete+insert'
) }}

with base as (

    select
        case
            when source = 'binance' and symbol = 'BTCUSDT' then 'bitcoin'
            when source = 'binance' and symbol = 'ETHUSDT' then 'ethereum'
            when source = 'binance' and symbol = 'SOLUSDT' then 'solana'
            when source = 'binance' and symbol = 'BNBUSDT' then 'binancecoin'
            when source = 'binance' and symbol = 'ADAUSDT' then 'cardano'
            else symbol
        end as asset_id,
        symbol,
        date,
        price,
        total_volume,
        case
            when source = 'coingecko' then 'CoinGecko'
            when source = 'binance' then 'Binance'
        end as source_name
    from {{ ref('crypto_unified') }}

    union all

    select
        symbol as asset_id,
        symbol,
        date,
        close as price,
        volume::float as total_volume,
        'Alpha Vantage' as source_name
    from {{ source('raw_data', 'airflow_alpha_vantage') }}

),

filtered as (

    select *
    from base

    {% if is_incremental() %}
    -- 7-day lookback: the watermark is global across sources, and stock
    -- data (trading days only, published late) always lags crypto - a
    -- strict `>` filter would drop late-arriving stock rows forever
    where date >= (
        select coalesce(max(dd.full_date), '1900-01-01'::date) - 7
        from {{ this }} f
        join {{ ref('dim_date') }} dd on f.date_key = dd.date_key
    )
    {% endif %}

)

select
    da.asset_key,
    dd.date_key,
    ds.source_key,
    filtered.price,
    filtered.total_volume
from filtered
-- point-in-time join: match the dim_asset version that was active on the
-- fact's own date, NOT "whatever is current today" (that was the bug -
-- it silently re-pointed historical facts to today's asset_key on every run)
join {{ ref('dim_asset') }} da
    on filtered.asset_id = da.asset_id
   and filtered.date >= da.valid_from::date
   and (da.valid_to is null or filtered.date < da.valid_to::date)
join {{ ref('dim_date') }} dd
    on filtered.date = dd.full_date
join {{ ref('dim_source') }} ds
    on filtered.source_name = ds.source_name
