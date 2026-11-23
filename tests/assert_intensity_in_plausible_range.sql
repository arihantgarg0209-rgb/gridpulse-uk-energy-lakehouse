-- Singular test: returns the rows that FAIL. dbt passes the test when this returns zero rows.
-- GB carbon intensity is never negative and has never come close to 1,000 gCO2/kWh.
select *
from {{ ref('fct_carbon_intensity') }}
where actual_gco2_per_kwh < 0
   or actual_gco2_per_kwh > 1000
   or forecast_gco2_per_kwh < 0
   or forecast_gco2_per_kwh > 1000
