{{ config(materialized='table') }}

with sources as (

select 'CoinGecko' as source_name

union all

select 'Binance'

union all

select 'Alpha Vantage'

)

select

    row_number() over(order by source_name) as source_key,

    source_name

from sources