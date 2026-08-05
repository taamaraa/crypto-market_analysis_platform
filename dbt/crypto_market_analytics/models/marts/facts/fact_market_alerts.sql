{{ config(materialized='table') }}

with price_change as (
    select
        symbol,
        date,
        price,
        lag(price) over (partition by symbol order by date) as prev_price
    from {{ ref('market_comparison') }}
),

price_drop_alerts as (
    select
        symbol,
        date,
        price,
        prev_price,
        round(((price - prev_price) / prev_price * 100)::numeric, 2) as price_change_pct
    from price_change
    where prev_price is not null
      and (price - prev_price) / prev_price <= -0.10
),

combined as (

    select
        symbol,
        date,
        'price_drop' as alert_type,
        'Price dropped ' || price_change_pct || '% (from $'
            || round(prev_price::numeric, 2) || ' to $'
            || round(price::numeric, 2) || ')' as alert_message
    from price_drop_alerts

    union all

    select
        mc.symbol,
        dd.full_date as date,
        'volume_spike' as alert_type,
        'Volume spiked ' || fva.volume_change_pct_30d
            || '% above the 30-day average' as alert_message
    from {{ ref('fact_volume_analysis') }} fva
    join {{ ref('dim_asset') }} da on fva.asset_key = da.asset_key
    join {{ ref('dim_date') }} dd on fva.date_key = dd.date_key
    join {{ ref('market_comparison') }} mc on mc.symbol = da.asset_id and mc.date = dd.full_date
    where fva.is_volume_spike = true

    union all

    select
        mc.symbol,
        dd.full_date as date,
        'high_volatility' as alert_type,
        'High volatility: 30-day price swing at '
            || round((fmv.coefficient_of_variation * 100)::numeric, 1)
            || '% of the average price (z-score '
            || round(fmv.z_score::numeric, 2) || ')' as alert_message
    from {{ ref('fact_market_volatility') }} fmv
    join {{ ref('dim_asset') }} da on fmv.asset_key = da.asset_key
    join {{ ref('dim_date') }} dd on fmv.date_key = dd.date_key
    join {{ ref('market_comparison') }} mc on mc.symbol = da.asset_id and mc.date = dd.full_date
    where fmv.risk_level = 'high'

)

select * from combined
