{{ config(materialized='table') }}

WITH base AS (
    SELECT
        coin_id,
        date,
        volume,
        avg_volume_7d,
        avg_volume_30d
    FROM {{ ref('int_volume_analysis') }}
)

SELECT
    da.asset_key,
    dd.date_key,
    base.volume,
    base.avg_volume_7d,
    base.avg_volume_30d,
    CASE
        WHEN base.avg_volume_7d IS NULL OR base.avg_volume_7d = 0 THEN NULL
        ELSE (base.volume - base.avg_volume_7d) / base.avg_volume_7d
    END AS volume_change_pct_7d,
    CASE
        WHEN base.avg_volume_30d IS NULL OR base.avg_volume_30d = 0 THEN false
        ELSE base.volume > 2 * base.avg_volume_30d
    END AS is_volume_spike
FROM base
JOIN {{ ref('dim_asset') }} da
    ON base.coin_id = da.asset_id
    AND base.date >= da.valid_from
    AND (da.valid_to IS NULL OR base.date < da.valid_to)
JOIN {{ ref('dim_date') }} dd ON base.date = dd.full_date