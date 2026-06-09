{{ config(materialized='view') }}

with base as (
    select
        coin_id,
        date,
        price,
        market_cap,
        total_volume
    from {{ ref('coingecko_daily') }}
),

averages as (
    select
        coin_id,
        date,
        price,
        market_cap,
        total_volume,

        round(avg(price) over (
            partition by coin_id
            order by date
            rows between 6 preceding and current row
        )::numeric, 2) as moving_avg_7d,

        round(avg(price) over (
            partition by coin_id
            order by date
            rows between 13 preceding and current row
        )::numeric, 2) as moving_avg_14d,

        round(avg(price) over (
            partition by coin_id
            order by date
            rows between 29 preceding and current row
        )::numeric, 2) as moving_avg_30d
    from base
)

select * from averages
order by coin_id, date