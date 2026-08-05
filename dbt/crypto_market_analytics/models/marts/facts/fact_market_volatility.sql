{{ config(materialized='table') }}

with volatility as (
    select
        symbol,
        date,
        price,
        avg_price_30d,
        stddev_price_30d,
        case
            when avg_price_30d = 0 or avg_price_30d is null then null
            else stddev_price_30d / avg_price_30d
        end as coefficient_of_variation,
        case
            when stddev_price_30d = 0 or stddev_price_30d is null then null
            else (price - avg_price_30d) / stddev_price_30d
        end as z_score
    from {{ ref('price_volatility') }}
),

flagged as (
    select
        *,
        case when abs(z_score) > 2 then true else false end as is_anomaly,
        case
            when coefficient_of_variation is null then null
            when coefficient_of_variation < 0.05 then 'low'
            when coefficient_of_variation < 0.15 then 'medium'
            else 'high'
        end as risk_level
    from volatility
)

select
    da.asset_key,
    dd.date_key,
    flagged.coefficient_of_variation,
    flagged.z_score,
    flagged.is_anomaly,
    flagged.risk_level
from flagged
join {{ ref('dim_asset') }} da
    on flagged.symbol = da.asset_id
   and flagged.date >= da.valid_from::date
   and (da.valid_to is null or flagged.date < da.valid_to::date)
join {{ ref('dim_date') }} dd
    on flagged.date = dd.full_date