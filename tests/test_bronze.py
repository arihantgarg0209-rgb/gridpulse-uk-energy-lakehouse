from gridpulse.bronze import (
    BRONZE_COLUMNS,
    RAW_ENVELOPE_SCHEMA,
    bronze_quality_issues,
    to_bronze_record,
)


def envelope(periods, ingested="2026-11-17T06:31:05+00:00"):
    return {
        "source_system": "neso_carbon_intensity_api",
        "dataset": "national_daily_carbon_intensity",
        "source_url": "u",
        "request_start_utc": "2026-11-15T06:30Z",
        "request_end_utc": "2026-11-17T06:30Z",
        "ingested_at_utc": ingested,
        "record_count": len(periods),
        "payload": {"data": periods},
    }


def period(start="2026-11-16T12:00Z", end="2026-11-16T12:30Z", actual=80):
    return {"from": start, "to": end, "intensity": {"forecast": 85, "actual": actual, "index": "low"}}


def raw(spark, *envelopes):
    return spark.createDataFrame(list(envelopes), RAW_ENVELOPE_SCHEMA)


def test_one_row_per_period_with_bronze_columns(spark):
    out = to_bronze_record(raw(spark, envelope([period(), period("2026-11-16T12:30Z", "2026-11-16T13:00Z")])))
    assert out.columns == BRONZE_COLUMNS
    assert out.count() == 2


def test_record_id_is_deterministic(spark):
    first = to_bronze_record(raw(spark, envelope([period()]))).first()["record_id"]
    second = to_bronze_record(raw(spark, envelope([period()]))).first()["record_id"]
    assert first == second


def test_same_period_fetched_twice_gives_two_snapshots(spark):
    out = to_bronze_record(raw(
        spark,
        envelope([period(actual=None)], ingested="2026-11-16T13:00:00+00:00"),
        envelope([period(actual=80)], ingested="2026-11-17T06:31:05+00:00"),
    ))
    assert out.select("record_id").distinct().count() == 2


def test_empty_payload_fails_quality_gate(spark):
    issues = bronze_quality_issues(to_bronze_record(raw(spark, envelope([]))))
    assert issues["missing_period_time"] == 1


def test_clean_batch_passes_quality_gate(spark):
    issues = bronze_quality_issues(to_bronze_record(raw(spark, envelope([period()]))))
    assert set(issues.values()) == {0}
