-- Staging model: the ONE place where UTC is converted to UK local time.
-- Every Gold model reads from here with ref(), instead of repeating this logic.
with source as (

    select * from {{ source('silver', 'carbon_intensity') }}

),

renamed as (

    select
        period_id,
        period_start_utc,
        period_end_utc,
        from_utc_timestamp(period_start_utc, 'Europe/London')                      as period_start_uk,
        date(from_utc_timestamp(period_start_utc, 'Europe/London'))                as uk_date,
        date_format(from_utc_timestamp(period_start_utc, 'Europe/London'), 'HH:mm') as uk_time_slot,
        forecast_gco2_per_kwh,
        actual_gco2_per_kwh,
        actual_gco2_per_kwh is not null                                             as has_actual,
        intensity_index,
        forecast_error_gco2_per_kwh,
        absolute_forecast_error_gco2_per_kwh
    from source

)

select * from renamed
