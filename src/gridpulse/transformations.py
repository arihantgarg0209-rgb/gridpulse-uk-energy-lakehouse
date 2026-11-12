"""Pure PySpark transformations for the GridPulse Silver layer.

Each function takes a DataFrame and returns a new DataFrame (no reads or writes),
so the same logic runs in the Databricks Silver notebook and in local unit tests.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VALID_INTENSITY_INDEXES = ["very low", "low", "moderate", "high", "very high"]

SILVER_COLUMNS = [
    "period_id",
    "source_record_id",
    "period_start_utc",
    "period_end_utc",
    "forecast_gco2_per_kwh",
    "actual_gco2_per_kwh",
    "intensity_index",
    "forecast_error_gco2_per_kwh",
    "absolute_forecast_error_gco2_per_kwh",
    "selected_ingested_at_utc",
    "silver_processed_at_utc",
]


def add_rejection_reason(df: DataFrame) -> DataFrame:
    """Add `rejection_reason`; null means the row is valid.

    Rules are checked top to bottom and the first match wins. Null checks come
    before value checks because comparisons with null return null, not false.
    """
    index_clean = F.lower(F.trim(F.col("intensity_index")))
    return df.withColumn(
        "rejection_reason",
        F.when(F.col("record_id").isNull(), "missing_record_id")
        .when(F.col("source_system").isNull(), "missing_source_system")
        .when(F.col("period_start_utc").isNull(), "missing_period_start")
        .when(F.col("period_end_utc").isNull(), "missing_period_end")
        .when(F.col("period_end_utc") <= F.col("period_start_utc"), "invalid_period_order")
        .when(F.col("forecast_gco2_per_kwh").isNull(), "missing_forecast")
        .when(F.col("forecast_gco2_per_kwh") < 0, "negative_forecast")
        .when(F.col("actual_gco2_per_kwh") < 0, "negative_actual")
        .when(F.col("intensity_index").isNull(), "missing_intensity_index")
        .when(~index_clean.isin(VALID_INTENSITY_INDEXES), "invalid_intensity_index")
        .when(F.col("actual_gco2_per_kwh") <= 0, "implausible_actual")
        .otherwise(F.lit(None).cast("string")),
    )


def split_valid_rejected(df: DataFrame) -> tuple:
    """Return (valid_df, rejected_df). Rejected rows keep their rejection_reason."""
    checked = add_rejection_reason(df)
    valid = checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    rejected = checked.filter(F.col("rejection_reason").isNotNull())
    return valid, rejected


def select_best_snapshot(df: DataFrame) -> DataFrame:
    """One row per half hour: prefer a snapshot with an actual value, then the newest."""
    window = Window.partitionBy("period_start_utc", "period_end_utc").orderBy(
        F.col("actual_available").desc(),
        F.col("ingested_at_utc").desc(),
        F.col("loaded_at_utc").desc(),
    )
    return (
        df.withColumn(
            "actual_available",
            F.when(F.col("actual_gco2_per_kwh").isNotNull(), 1).otherwise(0),
        )
        .withColumn("selection_rank", F.row_number().over(window))
        .filter(F.col("selection_rank") == 1)
        .drop("actual_available", "selection_rank")
    )


def to_silver_record(df: DataFrame) -> DataFrame:
    """Shape canonical periods into the Silver table's columns."""
    error = F.col("actual_gco2_per_kwh") - F.col("forecast_gco2_per_kwh")
    return df.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.col("period_start_utc").cast("string"),
                F.col("period_end_utc").cast("string"),
            ),
            256,
        ).alias("period_id"),
        F.col("record_id").alias("source_record_id"),
        F.col("period_start_utc"),
        F.col("period_end_utc"),
        F.col("forecast_gco2_per_kwh"),
        F.col("actual_gco2_per_kwh"),
        F.lower(F.trim(F.col("intensity_index"))).alias("intensity_index"),
        error.alias("forecast_error_gco2_per_kwh"),
        F.abs(error).alias("absolute_forecast_error_gco2_per_kwh"),
        F.col("ingested_at_utc").alias("selected_ingested_at_utc"),
        F.current_timestamp().alias("silver_processed_at_utc"),
    )


def build_silver(bronze_df: DataFrame) -> tuple:
    """Full Silver transformation. Returns (silver_ready_df, rejected_df)."""
    valid, rejected = split_valid_rejected(bronze_df)
    return to_silver_record(select_best_snapshot(valid)), rejected
