{{ config(materialized='table') }}

select
    da.asset_key,
    dd.date_key,
    p.moving_avg_7d,
    p.moving_avg_14d,
    p.moving_avg_30d
from {{ ref('price_with_averages') }} p
join {{ ref('dim_asset') }} da
    on p.coin_id = da.symbol and da.is_current = true
join {{ ref('dim_date') }} dd
    on p.date = dd.full_date