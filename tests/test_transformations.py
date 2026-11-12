from datetime import datetime

from gridpulse.transformations import (
    SILVER_COLUMNS,
    add_rejection_reason,
    build_silver,
    select_best_snapshot,
    to_silver_record,
)

SCHEMA = (
    "record_id string, source_system string, period_start_utc timestamp, "
    "period_end_utc timestamp, forecast_gco2_per_kwh int, actual_gco2_per_kwh int, "
    "intensity_index string, ingested_at_utc timestamp, loaded_at_utc timestamp"
)

START = datetime(2026, 8, 6, 11, 0)
END = datetime(2026, 8, 6, 11, 30)
INGESTED = datetime(2026, 8, 6, 12, 0)


def row(record_id="r1", source="neso_carbon_intensity_api", start=START, end=END,
        forecast=62, actual=60, index="low", ingested=INGESTED, loaded=INGESTED):
    return (record_id, source, start, end, forecast, actual, index, ingested, loaded)


def df(spark, *rows):
    return spark.createDataFrame(list(rows), SCHEMA)


def reason(spark, r):
    return add_rejection_reason(df(spark, r)).first()["rejection_reason"]


# ---------- validation rules ----------

def test_valid_row_has_no_rejection_reason(spark):
    assert reason(spark, row()) is None


def test_missing_record_id_is_rejected(spark):
    assert reason(spark, row(record_id=None)) == "missing_record_id"


def test_end_before_start_is_rejected(spark):
    assert reason(spark, row(start=END, end=START)) == "invalid_period_order"


def test_missing_forecast_is_rejected(spark):
    assert reason(spark, row(forecast=None)) == "missing_forecast"


def test_negative_forecast_is_rejected(spark):
    assert reason(spark, row(forecast=-5)) == "negative_forecast"


def test_negative_actual_is_rejected(spark):
    assert reason(spark, row(actual=-5)) == "negative_actual"


def test_null_intensity_index_is_rejected(spark):
    # Regression test: isin() on null returns null, so null needs its own rule.
    assert reason(spark, row(index=None)) == "missing_intensity_index"


def test_unknown_intensity_index_is_rejected(spark):
    assert reason(spark, row(index="extreme")) == "invalid_intensity_index"


def test_index_is_trimmed_and_lowercased_before_checking(spark):
    assert reason(spark, row(index="  Moderate ")) is None


def test_zero_actual_is_rejected(spark):
    # Regression test: the 6 Aug 2026 11:00 UTC reading of 0.
    assert reason(spark, row(actual=0)) == "implausible_actual"


def test_forecast_only_row_is_valid(spark):
    # null <= 0 evaluates to null, so a missing actual must not be rejected.
    assert reason(spark, row(actual=None)) is None


def test_first_matching_rule_wins(spark):
    assert reason(spark, row(record_id=None, index=None)) == "missing_record_id"


# ---------- best snapshot selection ----------

def test_prefers_snapshot_with_actual_over_newer_forecast_only(spark):
    best = select_best_snapshot(df(
        spark,
        row("older_with_actual", actual=150, ingested=datetime(2026, 8, 6, 12, 0)),
        row("newer_forecast_only", actual=None, ingested=datetime(2026, 8, 6, 18, 0)),
    )).collect()
    assert [r["record_id"] for r in best] == ["older_with_actual"]


def test_takes_newest_when_both_have_actual(spark):
    best = select_best_snapshot(df(
        spark,
        row("older", actual=148, ingested=datetime(2026, 8, 6, 12, 0)),
        row("newer", actual=150, ingested=datetime(2026, 8, 6, 18, 0)),
    )).collect()
    assert [r["record_id"] for r in best] == ["newer"]


def test_uses_loaded_at_to_break_ties(spark):
    best = select_best_snapshot(df(
        spark,
        row("loaded_first", loaded=datetime(2026, 8, 6, 12, 5)),
        row("loaded_later", loaded=datetime(2026, 8, 6, 12, 9)),
    )).collect()
    assert [r["record_id"] for r in best] == ["loaded_later"]


def test_keeps_one_row_per_period(spark):
    other_start, other_end = datetime(2026, 8, 6, 11, 30), datetime(2026, 8, 6, 12, 0)
    result = select_best_snapshot(df(
        spark,
        row("a1"), row("a2", ingested=datetime(2026, 8, 6, 13, 0)),
        row("b1", start=other_start, end=other_end),
    ))
    assert result.count() == 2


# ---------- Silver record shape ----------

def test_silver_has_expected_columns(spark):
    assert to_silver_record(df(spark, row())).columns == SILVER_COLUMNS


def test_period_id_ignores_ingestion_time(spark):
    out = to_silver_record(df(spark, row("r1"), row("r2", ingested=datetime(2026, 8, 7, 9, 0))))
    assert len({r["period_id"] for r in out.collect()}) == 1


def test_forecast_error_and_lowercased_index(spark):
    out = to_silver_record(df(spark, row(forecast=62, actual=50, index=" LOW "))).first()
    assert out["forecast_error_gco2_per_kwh"] == -12
    assert out["absolute_forecast_error_gco2_per_kwh"] == 12
    assert out["intensity_index"] == "low"


def test_forecast_error_is_null_without_actual(spark):
    out = to_silver_record(df(spark, row(actual=None))).first()
    assert out["forecast_error_gco2_per_kwh"] is None


def test_build_silver_end_to_end(spark):
    silver, rejected = build_silver(df(
        spark,
        row("forecast_only", actual=None, ingested=datetime(2026, 8, 6, 9, 0)),
        row("with_actual", actual=60, ingested=datetime(2026, 8, 6, 12, 0)),
        row("zero_reading", actual=0, ingested=datetime(2026, 8, 6, 13, 0)),
    ))
    assert [r["source_record_id"] for r in silver.collect()] == ["with_actual"]
    assert [r["rejection_reason"] for r in rejected.collect()] == ["implausible_actual"]
