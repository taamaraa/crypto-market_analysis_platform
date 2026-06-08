{{ config(materialized='view') }}

with crypto as (
    select
        symbol,
        date,
        price,
        total_volume,
        source
    from {{ ref('crypto_unified') }}
    where source = 'coingecko'
),

stocks as (
    select
        symbol,
        date,
        close                            as price,
        volume::float                    as total_volume,
        'alpha_vantage'                  as source
    from {{ ref('stg_alpha_vantage') }}
),

unified as (
    select * from crypto
    union all
    select * from stocks
)

select
    symbol,
    date,
    price,
    total_volume,
    source,
    case
        when source = 'alpha_vantage' then 'stock'
        else 'crypto'
    end as asset_type
from unified
order by symbol, date