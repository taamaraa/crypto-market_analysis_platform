{{ config(materialized='view') }}

-- One row per scored forecast: what each model predicted, what actually
-- happened, and the error between them.
--
-- This is the grain everything else builds on -- fact_forecast_accuracy is
-- just this aggregated -- so the join to the actuals and the Binance symbol
-- mapping live here once rather than in every downstream model.
--
-- Today's forecast is absent by design: its target date has no completed
-- candle yet, so there is nothing to score it against.

with forecasts as (

    select
        symbol,
        target_date,
        model_name,
        horizon_days,
        predicted_price::float as predicted_price,
        model_params
    from {{ source('python_marts', 'fact_price_forecast') }}

),

actuals as (

    select
        symbol,
        trade_date as target_date,
        close_price::float as actual_price
    from {{ ref('binance_metrics') }}
    -- Same exclusion the forecast script applies. Today's candle is still
    -- open, so its close is "the price right now" and would score a forecast
    -- against a number that is still moving.
    where trade_date < current_date

),

scored as (

    select
        -- Binance trades BTCUSDT while dim_asset keys on 'bitcoin'.
        case f.symbol
            when 'BTCUSDT' then 'bitcoin'
            when 'ETHUSDT' then 'ethereum'
            when 'SOLUSDT' then 'solana'
            when 'BNBUSDT' then 'binancecoin'
            when 'ADAUSDT' then 'cardano'
        end as asset_id,

        f.symbol,
        f.target_date,
        f.model_name,
        f.horizon_days,
        f.model_params,
        f.predicted_price,
        a.actual_price,
        a.actual_price - f.predicted_price as error,
        abs(a.actual_price - f.predicted_price) as abs_error
    from forecasts f
    join actuals a
        on a.symbol = f.symbol
       and a.target_date = f.target_date

)

select
    da.asset_key,
    dd.date_key,
    scored.asset_id,
    scored.symbol,
    scored.target_date,
    scored.model_name,
    scored.horizon_days,
    scored.model_params,
    round(scored.predicted_price::numeric, 8) as predicted_price,
    round(scored.actual_price::numeric, 8) as actual_price,
    round(scored.error::numeric, 8) as error,
    round(scored.abs_error::numeric, 8) as abs_error,
    round(
        (scored.abs_error / nullif(scored.actual_price, 0) * 100)::numeric, 4
    ) as abs_pct_error
from scored
join {{ ref('dim_asset') }} da
    on scored.asset_id = da.asset_id
   and scored.target_date >= da.valid_from::date
   and (da.valid_to is null or scored.target_date < da.valid_to::date)
join {{ ref('dim_date') }} dd
    on scored.target_date = dd.full_date
