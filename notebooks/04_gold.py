# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GridPulse UK — Gold Carbon-Intensity Data Products
# MAGIC
# MAGIC This notebook turns the Silver table (one validated row per half-hour period)
# MAGIC into two business-ready Gold tables.
# MAGIC
# MAGIC | Gold table | One row per | Business question |
# MAGIC |---|---|---|
# MAGIC | `gold.half_hour_intensity_profile` | UK clock time (00:00 … 23:30) | When is the GB grid cleanest on average? |
# MAGIC | `gold.daily_intensity_summary` | UK calendar day | How clean was each day, and how good was the forecast? |
# MAGIC
# MAGIC **Time zone rule:** Silver stores UTC. People in the UK think in UK local time
# MAGIC (UTC+1 during British Summer Time), so every Gold time is converted to
# MAGIC `Europe/London` before grouping.

# COMMAND ----------

SILVER_TABLE = "gridpulse.silver.carbon_intensity"
PROFILE_TABLE = "gridpulse.gold.half_hour_intensity_profile"
DAILY_TABLE = "gridpulse.gold.daily_intensity_summary"

silver_count = spark.table(SILVER_TABLE).count()
print(f"Silver rows available: {silver_count}")

assert silver_count > 0, "Silver is empty. Run the Silver notebook first."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 1 — Half-hour intensity profile
# MAGIC
# MAGIC - Converts each period start from UTC to UK time and keeps the clock time (`HH:mm`).
# MAGIC - Uses only periods with an **actual** value (forecast-only periods are excluded).
# MAGIC - Averages across all days, then ranks: rank 1 = cleanest time of day.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gridpulse.gold.half_hour_intensity_profile AS
# MAGIC WITH local_periods AS (
# MAGIC     SELECT
# MAGIC         date_format(
# MAGIC             from_utc_timestamp(period_start_utc, 'Europe/London'),
# MAGIC             'HH:mm'
# MAGIC         ) AS uk_time_slot,
# MAGIC         actual_gco2_per_kwh
# MAGIC     FROM gridpulse.silver.carbon_intensity
# MAGIC     WHERE actual_gco2_per_kwh IS NOT NULL
# MAGIC ),
# MAGIC profile AS (
# MAGIC     SELECT
# MAGIC         uk_time_slot,
# MAGIC         COUNT(*)                           AS days_observed,
# MAGIC         ROUND(AVG(actual_gco2_per_kwh), 1) AS avg_actual_gco2_per_kwh,
# MAGIC         MIN(actual_gco2_per_kwh)           AS min_actual_gco2_per_kwh,
# MAGIC         MAX(actual_gco2_per_kwh)           AS max_actual_gco2_per_kwh
# MAGIC     FROM local_periods
# MAGIC     GROUP BY uk_time_slot
# MAGIC )
# MAGIC SELECT
# MAGIC     uk_time_slot,
# MAGIC     days_observed,
# MAGIC     avg_actual_gco2_per_kwh,
# MAGIC     min_actual_gco2_per_kwh,
# MAGIC     max_actual_gco2_per_kwh,
# MAGIC     RANK() OVER (ORDER BY avg_actual_gco2_per_kwh) AS cleanliness_rank,
# MAGIC     current_timestamp()                            AS gold_processed_at_utc
# MAGIC FROM profile;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 2 — Daily intensity summary
# MAGIC
# MAGIC - Groups by **UK calendar day**.
# MAGIC - `MIN_BY(a, b)` returns the value of `a` on the row where `b` is smallest,
# MAGIC   so `MIN_BY(uk_time_slot, actual)` = the cleanest half hour of that day.
# MAGIC - `is_complete_day`: a normal day has 48 periods; clock-change days have 46 or 50.
# MAGIC   Days with fewer than 46 actuals are partial (e.g. today, or the edge of a backfill).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gridpulse.gold.daily_intensity_summary AS
# MAGIC WITH local_periods AS (
# MAGIC     SELECT
# MAGIC         DATE(from_utc_timestamp(period_start_utc, 'Europe/London')) AS uk_date,
# MAGIC         date_format(
# MAGIC             from_utc_timestamp(period_start_utc, 'Europe/London'),
# MAGIC             'HH:mm'
# MAGIC         ) AS uk_time_slot,
# MAGIC         actual_gco2_per_kwh,
# MAGIC         absolute_forecast_error_gco2_per_kwh
# MAGIC     FROM gridpulse.silver.carbon_intensity
# MAGIC )
# MAGIC SELECT
# MAGIC     uk_date,
# MAGIC     COUNT(*)                                            AS periods_total,
# MAGIC     COUNT(actual_gco2_per_kwh)                          AS periods_with_actual,
# MAGIC     COUNT(actual_gco2_per_kwh) >= 46                    AS is_complete_day,
# MAGIC     ROUND(AVG(actual_gco2_per_kwh), 1)                  AS avg_actual_gco2_per_kwh,
# MAGIC     MIN(actual_gco2_per_kwh)                            AS min_actual_gco2_per_kwh,
# MAGIC     MAX(actual_gco2_per_kwh)                            AS max_actual_gco2_per_kwh,
# MAGIC     MIN_BY(uk_time_slot, actual_gco2_per_kwh)           AS cleanest_time_slot,
# MAGIC     MAX_BY(uk_time_slot, actual_gco2_per_kwh)           AS dirtiest_time_slot,
# MAGIC     ROUND(AVG(absolute_forecast_error_gco2_per_kwh), 1) AS forecast_mae_gco2_per_kwh,
# MAGIC     current_timestamp()                                 AS gold_processed_at_utc
# MAGIC FROM local_periods
# MAGIC GROUP BY uk_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold quality gate
# MAGIC The notebook stops here if either Gold table is wrong, so a scheduled
# MAGIC Workflow never publishes bad results.

# COMMAND ----------

from pyspark.sql.functions import col

profile_df = spark.table(PROFILE_TABLE)
daily_df = spark.table(DAILY_TABLE)

profile_rows = profile_df.count()
profile_null_avgs = profile_df.filter(col("avg_actual_gco2_per_kwh").isNull()).count()

daily_rows = daily_df.count()
daily_distinct_dates = daily_df.select("uk_date").distinct().count()

print(f"Profile time slots: {profile_rows} (expected 48)")
print(f"Profile slots with null average: {profile_null_avgs}")
print(f"Daily rows: {daily_rows}, distinct dates: {daily_distinct_dates}")

assert profile_rows == 48, "Profile should contain exactly 48 half-hour slots."
assert profile_null_avgs == 0, "Every profile slot needs an average."
assert daily_rows > 0, "Daily summary is empty."
assert daily_rows == daily_distinct_dates, "Daily summary has duplicate dates."

print("Gold validation passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE gridpulse.gold.half_hour_intensity_profile IS
# MAGIC 'Average GB grid carbon intensity by UK clock time, ranked from cleanest to dirtiest';
# MAGIC
# MAGIC ALTER TABLE gridpulse.gold.half_hour_intensity_profile
# MAGIC SET TBLPROPERTIES ('quality' = 'gold', 'data.owner' = 'GridPulse');
# MAGIC
# MAGIC COMMENT ON TABLE gridpulse.gold.daily_intensity_summary IS
# MAGIC 'Daily GB grid carbon-intensity summary in UK local time, with forecast accuracy';
# MAGIC
# MAGIC ALTER TABLE gridpulse.gold.daily_intensity_summary
# MAGIC SET TBLPROPERTIES ('quality' = 'gold', 'data.owner' = 'GridPulse');

# COMMAND ----------

# MAGIC %md
# MAGIC ## The answers
# MAGIC The headline results of GridPulse.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Five cleanest times of day
# MAGIC SELECT cleanliness_rank, uk_time_slot, avg_actual_gco2_per_kwh, days_observed
# MAGIC FROM gridpulse.gold.half_hour_intensity_profile
# MAGIC ORDER BY cleanliness_rank
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Five dirtiest times of day
# MAGIC SELECT cleanliness_rank, uk_time_slot, avg_actual_gco2_per_kwh, days_observed
# MAGIC FROM gridpulse.gold.half_hour_intensity_profile
# MAGIC ORDER BY cleanliness_rank DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily trend (complete days only)
# MAGIC SELECT uk_date, avg_actual_gco2_per_kwh, cleanest_time_slot, dirtiest_time_slot, forecast_mae_gco2_per_kwh
# MAGIC FROM gridpulse.gold.daily_intensity_summary
# MAGIC WHERE is_complete_day
# MAGIC ORDER BY uk_date;