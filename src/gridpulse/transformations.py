"""Pure PySpark transformations for the GridPulse Silver layer.

Every function takes a DataFrame and returns a new DataFrame, with no reads or
writes, so the same logic runs in Databricks notebooks and in local unit tests.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VALID_INTENSITY_INDEXES = ["very low", "low", "moderate", "high", "very high"]


def add_rejection_reason(df: DataFrame) -> DataFrame:
    """Add a `rejection_reason` column: null means the row is valid.

    Rules are checked top to bottom and the first match wins, so null checks
    must come before value checks (three-valued logic).
    """
    index_clean = F.lower(F.trim(F.col("intensity_index")))
    return df.withColumn(
        "rejection_reason",
        F.when(F.col("period_start_utc").isNull(), "missing_period_start")
        .when(F.col("period_end_utc").isNull(), "missing_period_end")
        .when(F.col("period_end_utc") <= F.col("period_start_utc"), "invalid_period_window")
        .when(F.col("intensity_index").isNull(), "missing_intensity_index")
        .when(~index_clean.isin(VALID_INTENSITY_INDEXES), "invalid_intensity_index")
        .when(F.col("actual_gco2_per_kwh") <= 0, "implausible_actual")
        .otherwise(F.lit(None).cast("string")),
    )


def split_valid_rejected(df: DataFrame) -> tuple:
    """Return (valid_df, rejected_df) based on `rejection_reason`."""
    checked = add_rejection_reason(df)
    valid = checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    rejected = checked.filter(F.col("rejection_reason").isNotNull())
    return valid, rejected


def select_best_snapshot(df: DataFrame) -> DataFrame:
    """Keep one row per half hour: prefer a snapshot with an actual value, then the newest."""
    window = Window.partitionBy("period_start_utc", "period_end_utc").orderBy(
        F.col("actual_gco2_per_kwh").isNotNull().cast("int").desc(),
        F.col("ingested_at_utc").desc(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_period_id(df: DataFrame) -> DataFrame:
    """Business key: one ID per half hour, independent of when it was fetched."""
    return df.withColumn(
        "period_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("period_start_utc").cast("string"),
                F.col("period_end_utc").cast("string"),
            ),
            256,
        ),
    )


def add_forecast_error(df: DataFrame) -> DataFrame:
    """Forecast error (actual - forecast) and its absolute value; null when no actual."""
    error = F.col("actual_gco2_per_kwh") - F.col("forecast_gco2_per_kwh")
    return df.withColumn("forecast_error_gco2_per_kwh", error).withColumn(
        "absolute_forecast_error_gco2_per_kwh", F.abs(error)
    )


def build_silver(bronze_df: DataFrame) -> tuple:
    """Full Silver transformation. Returns (silver_df, rejected_df)."""
    valid, rejected = split_valid_rejected(bronze_df)
    best = select_best_snapshot(valid)
    silver = add_forecast_error(add_period_id(best)).withColumnRenamed(
        "record_id", "source_record_id"
    )
    return silver, rejected
