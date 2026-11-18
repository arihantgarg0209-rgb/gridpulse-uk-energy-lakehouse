# Databricks notebook source
# MAGIC %md
# MAGIC # Task 5 — Freshness and completeness check
# MAGIC
# MAGIC Fails the job (and so triggers the failure email) if the data is stale or yesterday
# MAGIC is incomplete. A pipeline that "succeeds" with old data is worse than one that fails.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

dbutils.widgets.text("max_staleness_hours", "3")
max_staleness = timedelta(hours=float(dbutils.widgets.get("max_staleness_hours")))

# COMMAND ----------

latest = spark.sql(
    "SELECT MAX(period_end_utc) AS latest FROM gridpulse.silver.carbon_intensity"
).first()["latest"]
latest_utc = latest.replace(tzinfo=timezone.utc)  # session time zone is UTC on Databricks by default
lag = datetime.now(timezone.utc) - latest_utc
print(f"Latest Silver period ends {latest_utc:%Y-%m-%d %H:%M} UTC ({lag} ago)")

# Yesterday in UK time should have a full day of periods (46 allows for clock-change days).
yesterday = spark.sql("""
    SELECT COUNT(*) AS periods
    FROM gridpulse.silver.carbon_intensity
    WHERE to_date(from_utc_timestamp(period_start_utc, 'Europe/London'))
          = date_sub(to_date(from_utc_timestamp(current_timestamp(), 'Europe/London')), 1)
""").first()["periods"]
print(f"Periods for yesterday (UK): {yesterday}")

# COMMAND ----------

problems = []
if lag > max_staleness:
    problems.append(f"Silver is stale: latest period is {lag} old (limit {max_staleness}).")
if yesterday < 46:
    problems.append(f"Yesterday has only {yesterday} of 48 half-hour periods.")

if problems:
    raise AssertionError(" ".join(problems))
print("Freshness and completeness checks passed.")
