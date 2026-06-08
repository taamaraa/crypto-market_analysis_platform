{{ config(materialized='view') }}

with coingecko as (
    select
        coin_id                          as symbol,
        date,
        price,
        market_cap,
        total_volume,
        'coingecko'                      as source
    from {{ ref('coingecko_daily') }}
),

binance as (
    select
        symbol,
        trade_date                       as date,
        last_price                       as price,
        null::float                      as market_cap,
        quote_volume                     as total_volume,
        'binance'                        as source
    from {{ ref('binance_metrics') }}
),

unified as (
    select * from coingecko
    union all
    select * from binance
)

select
    symbol,
    date,
    price,
    market_cap,
    total_volume,
    source
from unified
order by symbol, date