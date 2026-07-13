{{ config(materialized='table') }}

with dates as (

    select distinct date
    from {{ ref('crypto_unified') }}

    union

    select distinct date
    from {{ source('raw_data', 'airflow_alpha_vantage') }}

)

select
    to_char(date, 'YYYYMMDD')::int as date_key,
    date as full_date,
    extract(day from date)::int as day,
    extract(month from date)::int as month,
    extract(quarter from date)::int as quarter,
    extract(year from date)::int as year,
    extract(week from date)::int as week,
    trim(to_char(date, 'Day')) as day_name,
    trim(to_char(date, 'Month')) as month_name,
    case
        when extract(dow from date) in (0, 6) then true
        else false
    end as is_weekend

from dates
order by date