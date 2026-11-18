# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 — Ingest raw NESO data (scheduled)
# MAGIC
# MAGIC Calls the NESO API for a **rolling window** (default: last 2 days) and saves the
# MAGIC response as an immutable JSON file in the landing volume. Re-fetching recent days
# MAGIC on every run captures **late-arriving actuals**; Silver keeps the best snapshot.
# MAGIC
# MAGIC Job parameter: `lookback_days` (default `2`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

import json
import time
from datetime import datetime, timezone

import requests

from gridpulse.ingestion import api_url, build_envelope, raw_file_path, rolling_window

# COMMAND ----------

dbutils.widgets.text("lookback_days", "2")
lookback_days = int(dbutils.widgets.get("lookback_days"))

start, end = rolling_window(datetime.now(timezone.utc), lookback_days)
url = api_url(start, end)
print(f"Requesting {url}")

# COMMAND ----------

# Retry transient API failures here; the job adds task-level retries on top.
payload = None
for attempt in range(1, 4):
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        payload = response.json()
        break
    except requests.RequestException as error:
        print(f"Attempt {attempt} failed: {error}")
        if attempt == 3:
            raise
        time.sleep(10 * attempt)

# COMMAND ----------

ingested_at = datetime.now(timezone.utc)
envelope = build_envelope(payload, url, start, end, ingested_at)  # raises on an empty payload
path = raw_file_path(start, end, ingested_at)

dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
dbutils.fs.put(path, json.dumps(envelope), overwrite=False)

print(f"Saved {envelope['record_count']} periods to {path}")
dbutils.jobs.taskValues.set(key="periods_ingested", value=envelope["record_count"])
