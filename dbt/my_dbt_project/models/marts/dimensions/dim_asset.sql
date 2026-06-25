{{ config(materialized='table') }}

with crypto as (

    select distinct

        symbol as coin_id,

        symbol,

        'Crypto' as asset_type

    from {{ ref('crypto_unified') }}
    where source = 'coingecko'

),

stocks as (

    select distinct

        symbol as coin_id,

        symbol,

        'Stock' as asset_type

    from {{ ref('stg_alpha_vantage') }}

),

assets as (

    select * from crypto

    union

    select * from stocks

)

select

    row_number() over(order by coin_id) as asset_key,

    coin_id as asset_id,

    symbol,

    asset_type

from assets
order by coin_id