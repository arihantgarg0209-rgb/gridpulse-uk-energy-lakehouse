-- Date dimension: one row per UK calendar day, with descriptive attributes.
-- Spark's dayofweek(): 1 = Sunday ... 7 = Saturday.
select distinct
    uk_date,
    year(uk_date)                          as year,
    month(uk_date)                         as month,
    date_format(uk_date, 'MMMM')           as month_name,
    date_format(uk_date, 'EEEE')           as day_name,
    dayofweek(uk_date)                     as day_of_week,
    dayofweek(uk_date) in (1, 7)           as is_weekend
from {{ ref('stg_carbon_intensity') }}
