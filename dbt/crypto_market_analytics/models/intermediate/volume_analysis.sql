{{ config(materialized='view') }}

with daily_volume as (
    select symbol, date, total_volume, asset_type
    from {{ ref('market_comparison') }}
)

select
    symbol,
    date,
    total_volume,
    asset_type,
    avg(total_volume) over (
        partition by symbol order by date
        rows between 6 preceding and current row
    ) as avg_volume_7d,
    avg(total_volume) over (
        partition by symbol order by date
        rows between 29 preceding and current row
    ) as avg_volume_30d
from daily_volume