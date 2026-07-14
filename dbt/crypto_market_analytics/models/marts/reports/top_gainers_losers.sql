{{ config(materialized='table') }}

with ranked as (
    select
        symbol,
        price_change,
        price_change_percent,
        close_price,
        volume,
        volatility_label,
        trade_date,
        rank() over (partition by trade_date order by price_change_percent desc) as gainer_rank,
        rank() over (partition by trade_date order by price_change_percent asc)  as loser_rank
    from {{ ref('binance_metrics') }}
),

final as (
    select
        symbol,
        close_price,
        price_change,
        price_change_percent,
        volume,
        volatility_label,
        trade_date,
        gainer_rank,
        loser_rank,
        case
            when gainer_rank = 1 and price_change_percent > 0 then 'TOP GAINER'
            when loser_rank  = 1 and price_change_percent < 0 then 'TOP LOSER'
            when price_change_percent > 0                     then 'GAINER'
            when price_change_percent < 0                     then 'LOSER'
            else 'NEUTRAL'
        end as performance_label
    from ranked
)

select * from final
order by price_change_percent desc