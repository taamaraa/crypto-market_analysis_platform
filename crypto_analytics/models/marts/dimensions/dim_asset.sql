{{ config(
    materialized='incremental',
    unique_key='asset_key',
    post_hook="
        UPDATE {{ this }} AS t
        SET valid_to = sub.valid_to,
            is_active = sub.is_active
        FROM (
            SELECT
                asset_key,
                LEAD(valid_from) OVER (
                    PARTITION BY asset_id ORDER BY valid_from
                ) AS valid_to,
                CASE
                    WHEN LEAD(valid_from) OVER (
                        PARTITION BY asset_id ORDER BY valid_from
                    ) IS NULL THEN TRUE
                    ELSE FALSE
                END AS is_active
            FROM {{ this }}
        ) sub
        WHERE t.asset_key = sub.asset_key
    "
) }}

-- SCD2 следи промени на АТРИБУТИ на средството (symbol/asset_type),
-- не промени на цена (цената е fact-податок, живее во fact_prices)

WITH crypto AS (
    SELECT DISTINCT
        coin_id,
        coin_id AS symbol,
        'Crypto' AS asset_type,
        date AS attr_date
    FROM {{ ref('int_market_comparison') }}
    WHERE asset_type = 'crypto'
),
stocks AS (
    SELECT DISTINCT
        coin_id,
        coin_id AS symbol,
        'Stock' AS asset_type,
        date AS attr_date
    FROM {{ ref('int_market_comparison') }}
    WHERE asset_type = 'stock'
),
assets AS (
    SELECT * FROM crypto
    UNION ALL
    SELECT * FROM stocks
),

-- за секоја (asset_id, symbol, asset_type) комбинација, земи го најраниот
-- датум кога таа комбинација се појавила - тоа е valid_from за таа верзија
attribute_versions AS (
    SELECT
        coin_id AS asset_id,
        symbol,
        asset_type,
        MIN(attr_date) AS valid_from
    FROM assets
    GROUP BY coin_id, symbol, asset_type
),

{% if is_incremental() %}
new_versions AS (
    SELECT av.*
    FROM attribute_versions av
    LEFT JOIN {{ this }} t
        ON av.asset_id = t.asset_id
        AND av.symbol = t.symbol
        AND av.asset_type = t.asset_type
    WHERE t.asset_id IS NULL
),
max_key AS (
    SELECT COALESCE(MAX(asset_key), 0) AS current_max FROM {{ this }}
)
{% else %}
new_versions AS (
    SELECT * FROM attribute_versions
),
max_key AS (
    SELECT 0 AS current_max
)
{% endif %}

SELECT
    (SELECT current_max FROM max_key) + ROW_NUMBER() OVER (ORDER BY asset_id, valid_from) AS asset_key,
    asset_id,
    symbol,
    asset_type,
    valid_from,
    CAST(NULL AS date) AS valid_to,
    TRUE AS is_active
FROM new_versions