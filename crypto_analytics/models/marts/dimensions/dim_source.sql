{{ config(materialized='table') }}

WITH sources AS (
    SELECT 'CoinGecko' AS source_name
    UNION ALL
    SELECT 'Binance'
    UNION ALL
    SELECT 'AlphaVantage'
)

SELECT
    ROW_NUMBER() OVER (ORDER BY source_name) AS source_key,
    source_name
FROM sources