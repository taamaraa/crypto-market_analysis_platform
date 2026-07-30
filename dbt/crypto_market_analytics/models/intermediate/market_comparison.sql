{{ config(materialized='view') }}

with crypto_raw as (

    select
        case
            when source = 'binance' and symbol = 'BTCUSDT' then 'bitcoin'
            when source = 'binance' and symbol = 'ETHUSDT' then 'ethereum'
            when source = 'binance' and symbol = 'SOLUSDT' then 'solana'
            when source = 'binance' and symbol = 'BNBUSDT' then 'binancecoin'
            when source = 'binance' and symbol = 'ADAUSDT' then 'cardano'
            else symbol
        end as symbol,
        date,
        price,
        total_volume
    from {{ ref('crypto_unified') }}

),

crypto as (

    select
        symbol,
        date,
        avg(price) as price,
        sum(total_volume) as total_volume,
        'unified' as source
    from crypto_raw
    group by symbol, date

),

stocks as (

    select
        symbol,
        date,
        close as price,
        volume::float as total_volume,
        'alpha_vantage' as source
    from {{ source('raw_data', 'airflow_alpha_vantage') }}

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
        when source = 'alpha_vantage' then 'Stock'
        else 'Crypto'
    end as asset_type
from unified
order by symbol, date