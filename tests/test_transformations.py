from datetime import datetime

from gridpulse.transformations import (
    add_forecast_error,
    add_period_id,
    add_rejection_reason,
    select_best_snapshot,
)

COLUMNS = [
    "record_id",
    "period_start_utc",
    "period_end_utc",
    "forecast_gco2_per_kwh",
    "actual_gco2_per_kwh",
    "intensity_index",
    "ingested_at_utc",
]
SCHEMA = (
    "record_id string, period_start_utc timestamp, period_end_utc timestamp, "
    "forecast_gco2_per_kwh int, actual_gco2_per_kwh int, intensity_index string, "
    "ingested_at_utc timestamp"
)

START = datetime(2026, 8, 6, 11, 0)
END = datetime(2026, 8, 6, 11, 30)


def row(record_id="r1", start=START, end=END, forecast=62, actual=60,
        index="low", ingested=datetime(2026, 8, 6, 12, 0)):
    return (record_id, start, end, forecast, actual, index, ingested)


def reason(spark, *rows):
    df = add_rejection_reason(spark.createDataFrame(list(rows), SCHEMA))
    return [r["rejection_reason"] for r in df.orderBy("record_id").collect()]


# ---------- validation rules ----------

def test_valid_row_has_no_rejection_reason(spark):
    assert reason(spark, row()) == [None]


def test_null_intensity_index_is_rejected(spark):
    # Regression test for bug 1: isin() on null returns null, not false.
    assert reason(spark, row(index=None)) == ["missing_intensity_index"]


def test_unknown_intensity_index_is_rejected(spark):
    assert reason(spark, row(index="extreme")) == ["invalid_intensity_index"]


def test_index_is_trimmed_and_lowercased(spark):
    assert reason(spark, row(index="  Moderate ")) == [None]


def test_zero_actual_is_rejected(spark):
    # Regression test for bug 2: the 6 Aug 2026 11:00 UTC reading of 0.
    assert reason(spark, row(actual=0)) == ["implausible_actual"]


def test_forecast_only_row_is_valid(spark):
    # null <= 0 is null, so a missing actual must not be rejected.
    assert reason(spark, row(actual=None)) == [None]


def test_end_before_start_is_rejected(spark):
    assert reason(spark, row(start=END, end=START)) == ["invalid_period_window"]


def test_first_matching_rule_wins(spark):
    assert reason(spark, row(start=None, index=None)) == ["missing_period_start"]


# ---------- best snapshot selection ----------

def test_prefers_snapshot_with_actual_over_newer_forecast_only(spark):
    rows = [
        row("older_with_actual", actual=150, ingested=datetime(2026, 8, 6, 12, 0)),
        row("newer_forecast_only", actual=None, ingested=datetime(2026, 8, 6, 18, 0)),
    ]
    best = select_best_snapshot(spark.createDataFrame(rows, SCHEMA)).collect()
    assert [r["record_id"] for r in best] == ["older_with_actual"]


def test_takes_newest_when_both_have_actual(spark):
    rows = [
        row("older", actual=148, ingested=datetime(2026, 8, 6, 12, 0)),
        row("newer", actual=150, ingested=datetime(2026, 8, 6, 18, 0)),
    ]
    best = select_best_snapshot(spark.createDataFrame(rows, SCHEMA)).collect()
    assert [r["record_id"] for r in best] == ["newer"]


def test_one_row_per_period(spark):
    other_start, other_end = datetime(2026, 8, 6, 11, 30), datetime(2026, 8, 6, 12, 0)
    rows = [
        row("a1"), row("a2", ingested=datetime(2026, 8, 6, 13, 0)),
        row("b1", start=other_start, end=other_end),
    ]
    assert select_best_snapshot(spark.createDataFrame(rows, SCHEMA)).count() == 2


# ---------- keys and calculated columns ----------

def test_period_id_ignores_ingestion_time(spark):
    rows = [row("r1"), row("r2", ingested=datetime(2026, 8, 7, 9, 0))]
    ids = {r["period_id"] for r in add_period_id(spark.createDataFrame(rows, SCHEMA)).collect()}
    assert len(ids) == 1


def test_forecast_error(spark):
    out = add_forecast_error(spark.createDataFrame([row(forecast=62, actual=50)], SCHEMA)).first()
    assert out["forecast_error_gco2_per_kwh"] == -12
    assert out["absolute_forecast_error_gco2_per_kwh"] == 12


def test_forecast_error_is_null_without_actual(spark):
    out = add_forecast_error(spark.createDataFrame([row(actual=None)], SCHEMA)).first()
    assert out["forecast_error_gco2_per_kwh"] is None
