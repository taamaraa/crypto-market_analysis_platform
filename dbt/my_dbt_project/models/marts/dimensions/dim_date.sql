{{ config(materialized='table') }}

with dates as (

    select distinct date from {{ ref('crypto_unified') }}

    union

    select distinct date from {{ ref('stg_alpha_vantage') }}

)

select

    row_number() over(order by date) as date_key,

    date as full_date,

    extract(day from date)::int as day,

    extract(month from date)::int as month,

    extract(quarter from date)::int as quarter,

    extract(year from date)::int as year,

    extract(week from date)::int as week

from dates
order by date