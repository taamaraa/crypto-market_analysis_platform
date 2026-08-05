{{ config(materialized='table') }}

-- The leaderboard: one row per asset, target, model and horizon.
--
-- Built on fact_forecast_detail so the join to the actuals and the symbol
-- mapping are defined in exactly one place.
--
-- vs_naive_pct compares a model against the naive baseline for the SAME
-- target only (price vs price-naive, volatility vs volatility-naive, ...).
-- Comparing across targets would be meaningless -- a volatility MAE and a
-- price MAE are not on the same scale.

with per_model as (

    select
        asset_key,
        asset_id,
        symbol,
        target_name,
        model_name,
        horizon_days,
        count(*) as n_predictions,
        avg(abs_error) as mae,
        sqrt(avg(power(error, 2))) as rmse,
        avg(abs_pct_error) as mape
    from {{ ref('fact_forecast_detail') }}
    group by asset_key, asset_id, symbol, target_name, model_name, horizon_days

),

naive_baseline as (

    select symbol, target_name, horizon_days, mae as naive_mae
    from per_model
    where model_name = 'naive'

)

select
    per_model.asset_key,
    per_model.asset_id,
    per_model.symbol,
    per_model.target_name,
    per_model.model_name,
    per_model.horizon_days,
    per_model.n_predictions,

    round(per_model.mae::numeric, 6) as mae,
    round(per_model.rmse::numeric, 6) as rmse,
    round(per_model.mape::numeric, 4) as mape,

    round(
        ((naive_baseline.naive_mae - per_model.mae)
         / nullif(naive_baseline.naive_mae, 0) * 100)::numeric,
        2
    ) as vs_naive_pct

from per_model
left join naive_baseline
    on naive_baseline.symbol = per_model.symbol
   and naive_baseline.target_name = per_model.target_name
   and naive_baseline.horizon_days = per_model.horizon_days
order by per_model.target_name, per_model.symbol, per_model.mae
