from datetime import datetime, timedelta, timezone

import pytest

from gridpulse.ingestion import (
    api_url,
    build_envelope,
    floor_to_half_hour,
    raw_file_path,
    rolling_window,
)

UTC = timezone.utc


def test_floor_to_half_hour_rounds_down():
    assert floor_to_half_hour(datetime(2026, 11, 17, 6, 47, 12, tzinfo=UTC)) == datetime(
        2026, 11, 17, 6, 30, tzinfo=UTC
    )
    assert floor_to_half_hour(datetime(2026, 11, 17, 6, 29, 59, tzinfo=UTC)) == datetime(
        2026, 11, 17, 6, 0, tzinfo=UTC
    )


def test_floor_to_half_hour_rejects_naive_datetime():
    with pytest.raises(ValueError):
        floor_to_half_hour(datetime(2026, 11, 17, 6, 47))


def test_rolling_window_covers_lookback_days():
    start, end = rolling_window(datetime(2026, 11, 17, 6, 47, tzinfo=UTC), 2)
    assert end == datetime(2026, 11, 17, 6, 30, tzinfo=UTC)
    assert end - start == timedelta(days=2)


def test_rolling_window_rejects_zero_days():
    with pytest.raises(ValueError):
        rolling_window(datetime(2026, 11, 17, tzinfo=UTC), 0)


def test_api_url_uses_neso_time_format():
    start = datetime(2026, 11, 15, 6, 30, tzinfo=UTC)
    end = datetime(2026, 11, 17, 6, 30, tzinfo=UTC)
    assert api_url(start, end) == (
        "https://api.carbonintensity.org.uk/intensity/2026-11-15T06:30Z/2026-11-17T06:30Z"
    )


def test_build_envelope_records_lineage():
    start = datetime(2026, 11, 15, 6, 30, tzinfo=UTC)
    end = datetime(2026, 11, 17, 6, 30, tzinfo=UTC)
    payload = {"data": [{"from": "a"}, {"from": "b"}]}
    env = build_envelope(payload, "u", start, end, end)
    assert env["record_count"] == 2
    assert env["request_start_utc"] == "2026-11-15T06:30Z"
    assert env["payload"] is payload


@pytest.mark.parametrize("payload", [{}, {"data": []}, None])
def test_build_envelope_rejects_empty_payload(payload):
    now = datetime(2026, 11, 17, tzinfo=UTC)
    with pytest.raises(ValueError):
        build_envelope(payload, "u", now, now, now)


def test_raw_file_path_is_partitioned_and_unique():
    start = datetime(2026, 11, 15, 6, 30, tzinfo=UTC)
    end = datetime(2026, 11, 17, 6, 30, tzinfo=UTC)
    ingested = datetime(2026, 11, 17, 6, 31, 5, tzinfo=UTC)
    path = raw_file_path(start, end, ingested)
    assert path.startswith(
        "/Volumes/gridpulse/bronze/raw_landing/carbon_intensity/national_daily/2026/11/17/"
    )
    assert path.endswith("_20261117T063105Z.json")
