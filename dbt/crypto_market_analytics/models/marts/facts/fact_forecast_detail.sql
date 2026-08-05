{{ config(materialized='table') }}

-- One row per scored forecast: what each model predicted, what actually
-- happened, and the error between them, across all three targets.
--
-- This is the grain everything else builds on -- fact_forecast_accuracy is
-- just this aggregated -- so the join to the actuals and the Binance symbol
-- mapping live here once rather than in every downstream model.
--
-- Today's forecast is deliberately absent: its target date has no completed
-- candle yet, so there is nothing to score it against.
--
-- vs_naive_pct is the number to read first. Positive means the model beat
-- doing nothing, negative means it lost to a one-line baseline.

with forecasts as (

    select
        symbol,
        target_date,
        target_name,
        model_name,
        horizon_days,
        predicted_value::float as predicted_value,
        model_params
    from {{ source('python_marts', 'fact_price_forecast') }}

),

actuals as (

    select
        symbol,
        trade_date as target_date,
        close_price::float as actual_price,
        volatility_pct::float as actual_volatility,
        quote_volume::float as actual_volume
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
        f.target_name,
        f.model_name,
        f.horizon_days,
        f.model_params,
        f.predicted_value,

        -- Each target is scored against its own actual: a price forecast
        -- against the closing price, a volatility forecast against the
        -- realized volatility_pct, a volume forecast against quote_volume.
        case f.target_name
            when 'price' then a.actual_price
            when 'volatility' then a.actual_volatility
            when 'volume' then a.actual_volume
        end as actual_value

    from forecasts f
    join actuals a
        on a.symbol = f.symbol
       and a.target_date = f.target_date

),

with_error as (

    select
        *,
        actual_value - predicted_value as error,
        abs(actual_value - predicted_value) as abs_error
    from scored

)

select
    da.asset_key,
    dd.date_key,
    with_error.asset_id,
    with_error.symbol,
    with_error.target_date,
    with_error.target_name,
    with_error.model_name,
    with_error.horizon_days,
    with_error.model_params,
    round(with_error.predicted_value::numeric, 8) as predicted_value,
    round(with_error.actual_value::numeric, 8) as actual_value,
    round(with_error.error::numeric, 8) as error,
    round(with_error.abs_error::numeric, 8) as abs_error,
    round(
        (with_error.abs_error / nullif(with_error.actual_value, 0) * 100)::numeric, 4
    ) as abs_pct_error
from with_error
join {{ ref('dim_asset') }} da
    on with_error.asset_id = da.asset_id
   and with_error.target_date >= da.valid_from::date
   and (da.valid_to is null or with_error.target_date < da.valid_to::date)
join {{ ref('dim_date') }} dd
    on with_error.target_date = dd.full_date
