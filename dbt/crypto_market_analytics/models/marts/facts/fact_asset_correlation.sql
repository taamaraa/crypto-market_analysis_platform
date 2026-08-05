{{ config(materialized='table') }}

-- Rolling correlation of daily returns for every unique asset pair.
--
-- The self-join uses a.symbol < b.symbol so each pair appears once and no
-- asset is paired with itself: 7 assets -> 21 pairs.
--
-- Joining on a.date = b.date keeps only dates where BOTH assets traded, so
-- weekends drop out of any pair that involves a stock. For a crypto-stock
-- pair "30 rows back" therefore spans roughly 6 calendar weeks, not 30 days.
-- That is intentional -- correlation needs aligned observations -- but it
-- means the 30d/90d windows are not calendar-comparable across pair types.
-- observations_30d / observations_90d expose the actual row count so short
-- windows can be filtered out downstream.

with returns as (
    select symbol, date, asset_type, daily_return_pct
    from {{ ref('asset_returns') }}
    where daily_return_pct is not null
),

pairs as (
    select
        a.symbol as symbol_a,
        b.symbol as symbol_b,
        a.asset_type as asset_type_a,
        b.asset_type as asset_type_b,
        a.date,
        a.daily_return_pct as return_a,
        b.daily_return_pct as return_b
    from returns a
    join returns b
        on a.date = b.date
       and a.symbol < b.symbol
),

rolling as (
    select
        symbol_a,
        symbol_b,
        asset_type_a,
        asset_type_b,
        date,
        corr(return_a, return_b) over (
            partition by symbol_a, symbol_b order by date
            rows between 29 preceding and current row
        ) as correlation_30d,
        corr(return_a, return_b) over (
            partition by symbol_a, symbol_b order by date
            rows between 89 preceding and current row
        ) as correlation_90d,
        count(*) over (
            partition by symbol_a, symbol_b order by date
            rows between 29 preceding and current row
        ) as observations_30d,
        count(*) over (
            partition by symbol_a, symbol_b order by date
            rows between 89 preceding and current row
        ) as observations_90d
    from pairs
),

classified as (
    select
        symbol_a,
        symbol_b,
        date,
        round(correlation_30d::numeric, 4) as correlation_30d,
        round(correlation_90d::numeric, 4) as correlation_90d,
        observations_30d,
        observations_90d,
        case
            when asset_type_a = 'Crypto' and asset_type_b = 'Crypto' then 'Crypto-Crypto'
            when asset_type_a = 'Stock' and asset_type_b = 'Stock' then 'Stock-Stock'
            else 'Crypto-Stock'
        end as pair_type,
        case
            when correlation_30d is null then null
            when correlation_30d >= 0.7 then 'strong positive'
            when correlation_30d >= 0.3 then 'moderate positive'
            when correlation_30d > -0.3 then 'weak / none'
            when correlation_30d > -0.7 then 'moderate negative'
            else 'strong negative'
        end as correlation_strength_30d
    from rolling
)

select
    da_a.asset_key as asset_key_a,
    da_b.asset_key as asset_key_b,
    dd.date_key,
    classified.symbol_a,
    classified.symbol_b,
    classified.pair_type,
    classified.correlation_30d,
    classified.correlation_90d,
    classified.correlation_strength_30d,
    classified.observations_30d,
    classified.observations_90d
from classified
join {{ ref('dim_asset') }} da_a
    on classified.symbol_a = da_a.asset_id
   and classified.date >= da_a.valid_from::date
   and (da_a.valid_to is null or classified.date < da_a.valid_to::date)
join {{ ref('dim_asset') }} da_b
    on classified.symbol_b = da_b.asset_id
   and classified.date >= da_b.valid_from::date
   and (da_b.valid_to is null or classified.date < da_b.valid_to::date)
join {{ ref('dim_date') }} dd
    on classified.date = dd.full_date
