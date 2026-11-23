-- Singular test: a day has 48 half-hour clock times (00:00 ... 23:30), so the profile
-- should have exactly 48 rows. Returns a row (= failure) if it doesn't.
select count(*) as slot_count
from {{ ref('half_hour_intensity_profile') }}
having count(*) != 48
