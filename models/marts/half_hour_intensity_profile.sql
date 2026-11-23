-- Gold: when is the GB grid cleanest on average? One row per UK clock time (48 slots).
-- Same business logic as the old notebook, now reading the shared staging model.
with profile as (

    select
        uk_time_slot,
        count(*)                            as days_observed,
        round(avg(actual_gco2_per_kwh), 1)  as avg_actual_gco2_per_kwh,
        min(actual_gco2_per_kwh)            as min_actual_gco2_per_kwh,
        max(actual_gco2_per_kwh)            as max_actual_gco2_per_kwh
    from {{ ref('stg_carbon_intensity') }}
    where has_actual
    group by uk_time_slot

)

select
    uk_time_slot,
    days_observed,
    avg_actual_gco2_per_kwh,
    min_actual_gco2_per_kwh,
    max_actual_gco2_per_kwh,
    rank() over (order by avg_actual_gco2_per_kwh) as cleanliness_rank,
    current_timestamp()                            as gold_processed_at_utc
from profile
