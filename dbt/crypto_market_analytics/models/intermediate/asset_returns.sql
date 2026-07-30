{{ config(materialized='view') }}

with daily_price as (
    select symbol, date, price, asset_type
    from {{ ref('market_comparison') }}
),

with_prev as (
    select
        symbol,
        date,
        asset_type,
        price,
        lag(price) over (partition by symbol order by date) as prev_price
    from daily_price
)

select
    symbol,
    date,
    asset_type,
    price,
    prev_price,
    case
        when prev_price is null or prev_price = 0 then null
        else round(((price - prev_price) / prev_price * 100)::numeric, 4)
    end as daily_return_pct
from with_prev
