{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT date
    FROM {{ ref('int_market_comparison') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS date_key,
    date AS full_date,
    EXTRACT(day FROM date) AS day,
    EXTRACT(month FROM date) AS month,
    EXTRACT(quarter FROM date) AS quarter,
    EXTRACT(year FROM date) AS year,
    EXTRACT(week FROM date) AS week,
    TO_CHAR(date, 'Day') AS day_name,
    TO_CHAR(date, 'Month') AS month_name,
    CASE WHEN EXTRACT(dow FROM date) IN (0, 6) THEN true ELSE false END AS is_weekend
FROM dates
ORDER BY date