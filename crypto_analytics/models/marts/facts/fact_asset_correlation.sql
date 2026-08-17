{{ config(materialized='table') }}

WITH returns AS (
    SELECT coin_id, date, daily_return
    FROM {{ ref('int_asset_returns') }}
    WHERE daily_return IS NOT NULL
),

paired AS (
    SELECT
        a.coin_id AS coin_id_a,
        b.coin_id AS coin_id_b,
        a.date,
        a.daily_return AS return_a,
        b.daily_return AS return_b
    FROM returns a
    JOIN returns b
        ON a.date = b.date
        AND a.coin_id < b.coin_id
),

with_corr AS (
    SELECT
        coin_id_a,
        coin_id_b,
        date,
        ROUND(CORR(return_a, return_b) OVER (
            PARTITION BY coin_id_a, coin_id_b
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::numeric, 6) AS corr_30d,
        ROUND(CORR(return_a, return_b) OVER (
            PARTITION BY coin_id_a, coin_id_b
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        )::numeric, 6) AS corr_90d
    FROM paired
)

SELECT
    da.asset_key AS asset_key_a,
    db.asset_key AS asset_key_b,
    dd.date_key,
    wc.corr_30d,
    wc.corr_90d
FROM with_corr wc
JOIN {{ ref('dim_asset') }} da
    ON wc.coin_id_a = da.asset_id
    AND wc.date >= da.valid_from
    AND (da.valid_to IS NULL OR wc.date < da.valid_to)
JOIN {{ ref('dim_asset') }} db
    ON wc.coin_id_b = db.asset_id
    AND wc.date >= db.valid_from
    AND (db.valid_to IS NULL OR wc.date < db.valid_to)
JOIN {{ ref('dim_date') }} dd ON wc.date = dd.full_date