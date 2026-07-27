{{ config(materialized='view') }}

with unified as (

    select
        case
            when source = 'binance' and symbol = 'BTCUSDT' then 'bitcoin'
            when source = 'binance' and symbol = 'ETHUSDT' then 'ethereum'
            when source = 'binance' and symbol = 'SOLUSDT' then 'solana'
            when source = 'binance' and symbol = 'BNBUSDT' then 'binancecoin'
            when source = 'binance' and symbol = 'ADAUSDT' then 'cardano'
            else symbol
        end as coin_id,
        date,
        price,
        market_cap,
        total_volume
    from {{ ref('crypto_unified') }}

),

base as (

    select
        coin_id,
        date,
        avg(price) as price,
        sum(market_cap) as market_cap,
        sum(total_volume) as total_volume
    from unified
    group by coin_id, date

),

averages as (
    select
        coin_id,
        date,
        price,
        market_cap,
        total_volume,

        round(avg(price) over (
            partition by coin_id
            order by date
            rows between 6 preceding and current row
        )::numeric, 2) as moving_avg_7d,

        round(avg(price) over (
            partition by coin_id
            order by date
            rows between 13 preceding and current row
        )::numeric, 2) as moving_avg_14d,

        round(avg(price) over (
            partition by coin_id
            order by date
            rows between 29 preceding and current row
        )::numeric, 2) as moving_avg_30d
    from base
)

select * from averages
order by coin_id, date
