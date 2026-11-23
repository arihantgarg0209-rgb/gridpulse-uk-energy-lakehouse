-- Fact table: one row per half-hour period (the grain), holding the measures.
-- Descriptive date attributes live in dim_date and are joined on uk_date.
select
    period_id,
    uk_date,
    uk_time_slot,
    period_start_utc,
    period_start_uk,
    forecast_gco2_per_kwh,
    actual_gco2_per_kwh,
    has_actual,
    intensity_index,
    forecast_error_gco2_per_kwh,
    absolute_forecast_error_gco2_per_kwh
from {{ ref('stg_carbon_intensity') }}
