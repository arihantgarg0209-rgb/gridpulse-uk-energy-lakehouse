-- Gold: is the grid cleaner at weekends? Joins the fact table to the date dimension (star schema).
select
    d.is_weekend,
    count(*)                                 as periods_observed,
    round(avg(f.actual_gco2_per_kwh), 1)     as avg_actual_gco2_per_kwh,
    current_timestamp()                      as gold_processed_at_utc
from {{ ref('fct_carbon_intensity') }} as f
join {{ ref('dim_date') }} as d
    on f.uk_date = d.uk_date
where f.has_actual
group by d.is_weekend
