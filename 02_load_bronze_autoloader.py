# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 — Load Bronze with Auto Loader (incremental)
# MAGIC
# MAGIC Replaces "re-read every raw file" with **Auto Loader** (`cloudFiles`): a checkpoint
# MAGIC records which files are done, so each run processes only new files.
# MAGIC `trigger(availableNow=True)` drains the backlog and stops, which suits a batch job.
# MAGIC Each micro-batch is merged into Bronze on `record_id`, so a replay inserts nothing.
# MAGIC
# MAGIC The first run picks up the historical files too; the MERGE makes that harmless.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from delta.tables import DeltaTable

from gridpulse.bronze import RAW_ENVELOPE_SCHEMA, bronze_quality_issues, to_bronze_record

RAW_ROOT = "/Volumes/gridpulse/bronze/raw_landing/carbon_intensity"
CHECKPOINT = "/Volumes/gridpulse/bronze/checkpoints/carbon_intensity_bronze"
BRONZE_TABLE = "gridpulse.bronze.carbon_intensity"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS gridpulse.bronze.checkpoints
# MAGIC COMMENT 'Auto Loader / streaming checkpoints for GridPulse';

# COMMAND ----------

def merge_batch(batch_df, batch_id):
    bronze_df = to_bronze_record(batch_df)
    issues = bronze_quality_issues(bronze_df)
    if any(issues.values()):
        raise ValueError(f"Bronze quality gate failed in batch {batch_id}: {issues}")

    (
        DeltaTable.forName(batch_df.sparkSession, BRONZE_TABLE).alias("target")
        .merge(bronze_df.alias("source"), "target.record_id = source.record_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Batch {batch_id}: merged {bronze_df.count()} snapshot rows")

# COMMAND ----------

before = spark.table(BRONZE_TABLE).count()

query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("pathGlobFilter", "*.json")
    .schema(RAW_ENVELOPE_SCHEMA)
    .load(RAW_ROOT)
    .writeStream
    .foreachBatch(merge_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

after = spark.table(BRONZE_TABLE).count()
print(f"Bronze rows: {before} -> {after} (+{after - before})")
dbutils.jobs.taskValues.set(key="bronze_rows_added", value=after - before)
