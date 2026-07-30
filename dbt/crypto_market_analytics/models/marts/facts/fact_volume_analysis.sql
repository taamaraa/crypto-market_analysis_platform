{{ config(materialized='view') }}

with volume_calc as (
    select
        symbol,
        date,
        total_volume,
        avg_volume_7d,
        avg_volume_30d,
        case
            when avg_volume_7d = 0 or avg_volume_7d is null then null
            else round(((total_volume - avg_volume_7d) / avg_volume_7d * 100)::numeric, 2)
        end as volume_change_pct_7d,
        case
            when avg_volume_30d > 0 and total_volume > (avg_volume_30d * 2) then true
            else false
        end as is_volume_spike
    from {{ ref('volume_analysis') }}
)

select
    da.asset_key,
    dd.date_key,
    volume_calc.volume_change_pct_7d,
    volume_calc.is_volume_spike
from volume_calc
join {{ ref('dim_asset') }} da
    on volume_calc.symbol = da.asset_id
   and volume_calc.date >= da.valid_from::date
   and (da.valid_to is null or volume_calc.date < da.valid_to::date)
join {{ ref('dim_date') }} dd
    on volume_calc.date = dd.full_date