{{ config(
    materialized='incremental',
    unique_key=['asset_key', 'date_key'],
    incremental_strategy='delete+insert'
) }}

with filtered as (

    select *
    from {{ ref('price_with_averages') }} p

    {% if is_incremental() %}
    where p.date >= (
        select coalesce(max(dd.full_date), '1900-01-01'::date) - 7
        from {{ this }} f
        join {{ ref('dim_date') }} dd on f.date_key = dd.date_key
    )
    {% endif %}

)

select
    da.asset_key,
    dd.date_key,
    filtered.moving_avg_7d,
    filtered.moving_avg_14d,
    filtered.moving_avg_30d
from filtered
join {{ ref('dim_asset') }} da
    on filtered.coin_id = da.symbol
   and filtered.date >= da.valid_from::date
   and (da.valid_to is null or filtered.date < da.valid_to::date)
join {{ ref('dim_date') }} dd
    on filtered.date = dd.full_date
