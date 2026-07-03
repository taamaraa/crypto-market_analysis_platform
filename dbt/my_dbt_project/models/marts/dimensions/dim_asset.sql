{% set close_old_versions %}
    {% if is_incremental() %}
        update {{ this }} as old
        set
            valid_to = current_timestamp,
            is_current = false
        where old.is_current = true
          and exists (
              select 1
              from {{ this }} as newer
              where newer.asset_id = old.asset_id
                and newer.asset_key > old.asset_key
          );
    {% endif %}
{% endset %}

{{ config(
    materialized='incremental',
    post_hook=close_old_versions
) }}

with latest_crypto as (
    select
        symbol as asset_id,
        symbol,
        'Crypto' as asset_type,
        price,
        row_number() over(partition by symbol order by date desc) as rn
    from {{ ref('crypto_unified') }}
    where source = 'coingecko'
),

latest_stocks as (
    select
        symbol as asset_id,
        symbol,
        'Stock' as asset_type,
        close as price,
        row_number() over(partition by symbol order by date desc) as rn
    from {{ source('raw_data', 'airflow_alpha_vantage') }}
),

current_assets as (
    select asset_id, symbol, asset_type, price
    from latest_crypto
    where rn = 1

    union all

    select asset_id, symbol, asset_type, price
    from latest_stocks
    where rn = 1
),

records_to_insert as (

    {% if is_incremental() %}

        select c.*
        from current_assets c
        left join {{ this }} d
            on c.asset_id = d.asset_id
           and d.is_current = true
        where d.asset_id is null
           or c.symbol <> d.symbol
           or c.asset_type <> d.asset_type
           or c.price <> d.price

    {% else %}

        select *
        from current_assets

    {% endif %}
),

numbered as (
    select
        *,
        row_number() over(order by asset_id) as rn
    from records_to_insert
)

select
    {% if is_incremental() %}
        (select coalesce(max(asset_key), 0) from {{ this }}) + rn as asset_key,
    {% else %}
        rn as asset_key,
    {% endif %}

    asset_id,
    symbol,
    asset_type,
    price,
    current_timestamp as valid_from,
    null::timestamp as valid_to,
    true as is_current

from numbered