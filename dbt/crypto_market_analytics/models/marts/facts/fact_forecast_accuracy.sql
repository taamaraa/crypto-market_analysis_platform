{{ config(materialized='view') }}

-- The leaderboard: one row per asset, model and horizon.
--
-- Built on fact_forecast_detail so the join to the actuals and the symbol
-- mapping are defined in exactly one place.
--
-- vs_naive_pct is the number to read first. Positive means the model beat
-- doing nothing; negative means it lost to a one-line baseline. Absolute
-- errors mean very little on their own -- a MAE of $970 is unreadable until
-- you know naive scores $974 on the same days.

with per_model as (

    select
        asset_key,
        asset_id,
        symbol,
        model_name,
        horizon_days,
        count(*) as n_predictions,
        avg(abs_error) as mae,
        sqrt(avg(power(error, 2))) as rmse,
        avg(abs_pct_error) as mape
    from {{ ref('fact_forecast_detail') }}
    group by asset_key, asset_id, symbol, model_name, horizon_days

),

naive_baseline as (

    select symbol, horizon_days, mae as naive_mae
    from per_model
    where model_name = 'naive'

)

select
    per_model.asset_key,
    per_model.asset_id,
    per_model.symbol,
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
   and naive_baseline.horizon_days = per_model.horizon_days
order by per_model.symbol, per_model.mae
