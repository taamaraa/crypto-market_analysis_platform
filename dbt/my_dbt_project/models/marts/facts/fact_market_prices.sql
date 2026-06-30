{{ config(materialized='table') }}

with base as (
    select
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
        symbol,
        date,
        close as price,
        volume::float as total_volume,
        'Alpha Vantage' as source_name
    from {{ ref('stg_alpha_vantage') }}
)

select
    da.asset_key,
    dd.date_key,
    ds.source_key,
    base.price,
    base.total_volume
from base
join {{ ref('dim_asset') }} da on base.symbol = da.symbol and da.is_current = true
join {{ ref('dim_date') }} dd on base.date = dd.full_date
join {{ ref('dim_source') }} ds on base.source_name = ds.source_name