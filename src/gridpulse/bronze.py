"""Pure PySpark logic for the Bronze layer: raw envelope -> one row per snapshot.

Shared by the historical notebook and the scheduled Auto Loader task, and
covered by tests/test_bronze.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

INTENSITY_SCHEMA = StructType([
    StructField("forecast", IntegerType(), True),
    StructField("actual", IntegerType(), True),
    StructField("index", StringType(), True),
])

PERIOD_SCHEMA = StructType([
    StructField("from", StringType(), True),
    StructField("to", StringType(), True),
    StructField("intensity", INTENSITY_SCHEMA, True),
])

RAW_ENVELOPE_SCHEMA = StructType([
    StructField("source_system", StringType(), True),
    StructField("dataset", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("request_start_utc", StringType(), True),
    StructField("request_end_utc", StringType(), True),
    StructField("ingested_at_utc", StringType(), True),
    StructField("record_count", IntegerType(), True),
    StructField("payload", StructType([
        StructField("data", ArrayType(PERIOD_SCHEMA), True),
    ]), True),
])

BRONZE_COLUMNS = [
    "record_id",
    "source_system",
    "ingested_at_utc",
    "period_start_utc",
    "period_end_utc",
    "forecast_gco2_per_kwh",
    "actual_gco2_per_kwh",
    "intensity_index",
    "loaded_at_utc",
]

API_TS = "yyyy-MM-dd'T'HH:mm'Z'"


def to_bronze_record(raw_df: DataFrame) -> DataFrame:
    """Explode each envelope into one typed row per period, with a deterministic record_id.

    explode_outer keeps an empty response visible as a null period, so the
    quality gate fails loudly instead of the data disappearing silently.
    """
    exploded = raw_df.withColumn("period", F.explode_outer(F.col("payload.data")))
    typed = exploded.select(
        F.col("source_system"),
        F.to_timestamp(F.col("ingested_at_utc")).alias("ingested_at_utc"),
        F.to_timestamp(F.col("period.from"), API_TS).alias("period_start_utc"),
        F.to_timestamp(F.col("period.to"), API_TS).alias("period_end_utc"),
        F.col("period.intensity.forecast").alias("forecast_gco2_per_kwh"),
        F.col("period.intensity.actual").alias("actual_gco2_per_kwh"),
        F.col("period.intensity.index").alias("intensity_index"),
    )
    return (
        typed.withColumn(
            "record_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("source_system"),
                    F.col("ingested_at_utc").cast("string"),
                    F.col("period_start_utc").cast("string"),
                    F.col("period_end_utc").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("loaded_at_utc", F.current_timestamp())
        .select(*BRONZE_COLUMNS)
    )


def bronze_quality_issues(bronze_df: DataFrame) -> dict:
    """Count rows that must never reach Bronze. All values should be 0."""
    counts = bronze_df.agg(
        F.sum(F.col("record_id").isNull().cast("int")).alias("missing_record_id"),
        F.sum((F.col("period_start_utc").isNull() | F.col("period_end_utc").isNull()).cast("int"))
        .alias("missing_period_time"),
        F.sum((F.col("period_end_utc") <= F.col("period_start_utc")).cast("int"))
        .alias("invalid_period_order"),
        (F.count("*") - F.countDistinct("record_id")).alias("duplicate_record_id"),
    ).first()
    return {k: int(v or 0) for k, v in counts.asDict().items()}
