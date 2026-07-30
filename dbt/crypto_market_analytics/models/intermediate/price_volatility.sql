{{ config(materialized='view') }}

with daily_price as (
    select symbol, date, price, asset_type
    from {{ ref('market_comparison') }}
)

select
    symbol,
    date,
    price,
    asset_type,
    avg(price) over (
        partition by symbol order by date
        rows between 6 preceding and current row
    ) as avg_price_7d,
    stddev(price) over (
        partition by symbol order by date
        rows between 6 preceding and current row
    ) as stddev_price_7d,
    avg(price) over (
        partition by symbol order by date
        rows between 29 preceding and current row
    ) as avg_price_30d,
    stddev(price) over (
        partition by symbol order by date
        rows between 29 preceding and current row
    ) as stddev_price_30d
from daily_price