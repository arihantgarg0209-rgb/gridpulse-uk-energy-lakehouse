-- Gold: how clean was each UK day, and how good was the forecast? One row per uk_date.
-- A normal day has 48 periods; clock-change days have 46 or 50.
select
    uk_date,
    count(*)                                             as periods_total,
    count(actual_gco2_per_kwh)                           as periods_with_actual,
    count(actual_gco2_per_kwh) >= 46                     as is_complete_day,
    round(avg(actual_gco2_per_kwh), 1)                   as avg_actual_gco2_per_kwh,
    min(actual_gco2_per_kwh)                             as min_actual_gco2_per_kwh,
    max(actual_gco2_per_kwh)                             as max_actual_gco2_per_kwh,
    min_by(uk_time_slot, actual_gco2_per_kwh)            as cleanest_time_slot,
    max_by(uk_time_slot, actual_gco2_per_kwh)            as dirtiest_time_slot,
    round(avg(absolute_forecast_error_gco2_per_kwh), 1)  as forecast_mae_gco2_per_kwh,
    current_timestamp()                                  as gold_processed_at_utc
from {{ ref('stg_carbon_intensity') }}
group by uk_date
