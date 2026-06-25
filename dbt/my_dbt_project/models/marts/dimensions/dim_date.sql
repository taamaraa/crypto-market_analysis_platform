{{ config(materialized='table') }}

with dates as (

    select distinct date from {{ ref('crypto_unified') }}

    union

    select distinct date from {{ ref('stg_alpha_vantage') }}

)

select

    row_number() over(order by date) as date_key,

    date as full_date,

    extract(day from date) as day,

    extract(month from date) as month,

    extract(quarter from date) as quarter,

    extract(year from date) as year,

    extract(week from date) as week

from dates
order by date