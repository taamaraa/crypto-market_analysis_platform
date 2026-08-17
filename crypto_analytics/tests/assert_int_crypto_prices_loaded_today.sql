-- Failинг ако нема ниту еден нов ред за денес во int_crypto_prices
-- (штити нѐ од тивко скршена incremental логика, како проблемот од јули 2026)
SELECT 1
WHERE (
    SELECT COUNT(*)
    FROM {{ ref('int_crypto_prices') }}
    WHERE ingested_at = CURRENT_DATE
) = 0