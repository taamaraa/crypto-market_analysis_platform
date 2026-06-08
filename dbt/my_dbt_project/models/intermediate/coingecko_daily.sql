{{ config(materialized='view') }}

with source as (
    select
        coin_id,
        prices,
        market_caps,
        total_volumes
    from {{ ref('stg_coingecko') }}
),

unpacked as (
    select
        coin_id,
        (price_entry->0)::bigint                    as ts,
        (price_entry->1)::float                     as price,
        (market_entry->1)::float                    as market_cap,
        (volume_entry->1)::float                    as total_volume,
        to_timestamp((price_entry->0)::bigint / 1000)::date as date
    from source,
    jsonb_array_elements(prices)   with ordinality as t1(price_entry, idx),
    jsonb_array_elements(market_caps) with ordinality as t2(market_entry, idx2),
    jsonb_array_elements(total_volumes) with ordinality as t3(volume_entry, idx3)
    where t1.idx = t2.idx2 and t1.idx = t3.idx3
)

select
    coin_id,
    date,
    price,
    market_cap,
    total_volume
from unpacked
order by coin_id, date