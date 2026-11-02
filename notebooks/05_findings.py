# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GridPulse UK — Findings & Charts
# MAGIC
# MAGIC This notebook reads the Gold tables and answers the project's business question:
# MAGIC **when is the GB electricity grid cleanest, and how much does timing matter?**
# MAGIC
# MAGIC Run all cells. The printed findings at the top are the sentences for your README and interviews.

# COMMAND ----------

import matplotlib.pyplot as plt

PROFILE_TABLE = "gridpulse.gold.half_hour_intensity_profile"
DAILY_TABLE = "gridpulse.gold.daily_intensity_summary"
SILVER_TABLE = "gridpulse.silver.carbon_intensity"

# Gold tables are small (48 rows and ~100 rows), so converting to pandas for charting is safe
profile = spark.table(PROFILE_TABLE).orderBy("uk_time_slot").toPandas()
daily = (
    spark.table(DAILY_TABLE)
    .filter("is_complete_day")
    .orderBy("uk_date")
    .toPandas()
)

print(f"Time slots: {len(profile)} | Complete days: {len(daily)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key findings

# COMMAND ----------

avg_col = "avg_actual_gco2_per_kwh"

best = profile.nsmallest(1, avg_col).iloc[0]
worst = profile.nlargest(1, avg_col).iloc[0]
cleanest_5 = profile.nsmallest(5, avg_col)["uk_time_slot"].tolist()
dirtiest_5 = profile.nlargest(5, avg_col)["uk_time_slot"].tolist()
reduction_pct = (worst[avg_col] - best[avg_col]) / worst[avg_col] * 100

first_day = daily["uk_date"].min()
last_day = daily["uk_date"].max()
overall_avg = daily[avg_col].mean()
avg_mae = daily["forecast_mae_gco2_per_kwh"].mean()
cleanest_day = daily.nsmallest(1, avg_col).iloc[0]
dirtiest_day = daily.nlargest(1, avg_col).iloc[0]

print("GRIDPULSE UK — KEY FINDINGS")
print("=" * 60)
print(f"1. Period analysed: {first_day} to {last_day} ({len(daily)} complete days, UK local time).")
print(f"2. Average GB grid carbon intensity: {overall_avg:.0f} gCO2/kWh.")
print(f"3. Cleanest time of day: {best['uk_time_slot']} ({best[avg_col]:.0f} gCO2/kWh on average).")
print(f"4. Dirtiest time of day: {worst['uk_time_slot']} ({worst[avg_col]:.0f} gCO2/kWh on average).")
print(f"5. Moving a flexible load (e.g. EV charging) from {worst['uk_time_slot']} to "
      f"{best['uk_time_slot']} cuts its carbon intensity by about {reduction_pct:.0f}%.")
print(f"6. Five cleanest slots: {', '.join(cleanest_5)}")
print(f"7. Five dirtiest slots: {', '.join(dirtiest_5)}")
print(f"8. Cleanest day: {cleanest_day['uk_date']} ({cleanest_day[avg_col]:.0f}); "
      f"dirtiest day: {dirtiest_day['uk_date']} ({dirtiest_day[avg_col]:.0f}).")
print(f"9. NESO forecast accuracy: average absolute error of {avg_mae:.1f} gCO2/kWh per half hour.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 1 — Average carbon intensity by time of day
# MAGIC Green = 5 cleanest slots, red = 5 dirtiest.

# COMMAND ----------

colours = [
    "#2e7d32" if slot in cleanest_5 else "#c62828" if slot in dirtiest_5 else "#90a4ae"
    for slot in profile["uk_time_slot"]
]

fig, ax = plt.subplots(figsize=(14, 5))
ax.bar(profile["uk_time_slot"], profile[avg_col], color=colours)
ax.set_title("GB grid carbon intensity by time of day (UK local time)")
ax.set_xlabel("Half-hour slot")
ax.set_ylabel("Average actual intensity (gCO2/kWh)")
ax.set_xticks(range(0, 48, 2))
ax.set_xticklabels(profile["uk_time_slot"][::2], rotation=45)
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 2 — Daily average carbon intensity

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(daily["uk_date"], daily[avg_col], marker="o", markersize=3, label="Daily average")
ax.fill_between(daily["uk_date"], daily["min_actual_gco2_per_kwh"], daily["max_actual_gco2_per_kwh"],
                alpha=0.15, label="Daily min–max range")
ax.set_title("Daily GB grid carbon intensity")
ax.set_xlabel("Date")
ax.set_ylabel("gCO2/kWh")
ax.legend()
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 3 — Weekday vs weekend profile
# MAGIC Computed from Silver. `dayofweek` returns 1 = Sunday and 7 = Saturday.

# COMMAND ----------

day_type = spark.sql(f"""
    SELECT
        CASE WHEN dayofweek(from_utc_timestamp(period_start_utc, 'Europe/London')) IN (1, 7)
             THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        date_format(from_utc_timestamp(period_start_utc, 'Europe/London'), 'HH:mm') AS uk_time_slot,
        AVG(actual_gco2_per_kwh) AS avg_actual
    FROM {SILVER_TABLE}
    WHERE actual_gco2_per_kwh IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2
""").toPandas()

fig, ax = plt.subplots(figsize=(14, 5))
for label, group in day_type.groupby("day_type"):
    ax.plot(group["uk_time_slot"], group["avg_actual"], marker="o", markersize=3, label=label)
ax.set_title("Weekday vs weekend carbon intensity by time of day")
ax.set_xlabel("Half-hour slot (UK time)")
ax.set_ylabel("Average actual intensity (gCO2/kWh)")
ax.set_xticks(range(0, 48, 2))
ax.set_xticklabels(sorted(day_type["uk_time_slot"].unique())[::2], rotation=45)
ax.legend()
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

weekday_avg = day_type[day_type["day_type"] == "Weekday"]["avg_actual"].mean()
weekend_avg = day_type[day_type["day_type"] == "Weekend"]["avg_actual"].mean()
print(f"10. Weekday average: {weekday_avg:.0f} gCO2/kWh | Weekend average: {weekend_avg:.0f} gCO2/kWh.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 4 — How accurate is the NESO forecast?

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 4))
ax.bar(daily["uk_date"], daily["forecast_mae_gco2_per_kwh"], color="#5c6bc0")
ax.set_title("Daily forecast mean absolute error (actual vs forecast)")
ax.set_xlabel("Date")
ax.set_ylabel("MAE (gCO2/kWh)")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Suspiciously low readings
# MAGIC SELECT period_start_utc, forecast_gco2_per_kwh, actual_gco2_per_kwh
# MAGIC FROM gridpulse.silver.carbon_intensity
# MAGIC WHERE actual_gco2_per_kwh < 20
# MAGIC ORDER BY actual_gco2_per_kwh;

# COMMAND ----------

# MAGIC %sql
# MAGIC  DELETE FROM gridpulse.silver.carbon_intensity
# MAGIC    WHERE actual_gco2_per_kwh <= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH d AS (
# MAGIC   SELECT DATE(from_utc_timestamp(period_start_utc, 'Europe/London')) AS uk_date,
# MAGIC          forecast_gco2_per_kwh, actual_gco2_per_kwh, absolute_forecast_error_gco2_per_kwh
# MAGIC   FROM gridpulse.silver.carbon_intensity
# MAGIC )
# MAGIC SELECT uk_date,
# MAGIC        COUNT(*)                                            AS periods,
# MAGIC        ROUND(AVG(absolute_forecast_error_gco2_per_kwh), 1) AS mae,
# MAGIC        MAX(absolute_forecast_error_gco2_per_kwh)           AS worst_error
# MAGIC FROM d
# MAGIC WHERE uk_date BETWEEN '2026-08-28' AND '2026-09-08'
# MAGIC GROUP BY uk_date
# MAGIC ORDER BY uk_date;

# COMMAND ----------

