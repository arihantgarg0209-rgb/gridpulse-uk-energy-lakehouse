"""Pure-Python helpers for daily NESO ingestion (no Spark, no network).

Kept separate from the notebooks so the window, envelope and path logic
can be unit-tested and reused by the scheduled Lakeflow Job.
"""

from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.carbonintensity.org.uk"
SOURCE_SYSTEM = "neso_carbon_intensity_api"
DAILY_DATASET = "national_daily_carbon_intensity"
RAW_ROOT = "/Volumes/gridpulse/bronze/raw_landing/carbon_intensity"
API_TIME_FORMAT = "%Y-%m-%dT%H:%MZ"


def floor_to_half_hour(moment: datetime) -> datetime:
    """Round a UTC datetime down to the start of its half-hour period."""
    if moment.tzinfo is None:
        raise ValueError("moment must be timezone-aware (UTC)")
    moment = moment.astimezone(timezone.utc)
    return moment.replace(minute=0 if moment.minute < 30 else 30, second=0, microsecond=0)


def rolling_window(now: datetime, lookback_days: int) -> tuple:
    """Return (start, end) for a rolling window ending at the current half hour.

    A window longer than one day re-fetches recent periods on every run, so
    actual values that NESO publishes late still reach Bronze; Silver then
    keeps the best snapshot per period.
    """
    if lookback_days < 1:
        raise ValueError("lookback_days must be at least 1")
    end = floor_to_half_hour(now)
    start = end - timedelta(days=lookback_days)
    return start, end


def api_url(start: datetime, end: datetime) -> str:
    return f"{BASE_URL}/intensity/{start.strftime(API_TIME_FORMAT)}/{end.strftime(API_TIME_FORMAT)}"


def build_envelope(payload: dict, url: str, start: datetime, end: datetime,
                   ingested_at: datetime, dataset: str = DAILY_DATASET) -> dict:
    """Wrap an API response with lineage metadata. Raises if the payload is empty."""
    periods = payload.get("data") if isinstance(payload, dict) else None
    if not periods:
        raise ValueError(f"NESO returned no periods for {url}")
    return {
        "source_system": SOURCE_SYSTEM,
        "dataset": dataset,
        "source_url": url,
        "request_start_utc": start.strftime(API_TIME_FORMAT),
        "request_end_utc": end.strftime(API_TIME_FORMAT),
        "ingested_at_utc": ingested_at.isoformat(),
        "record_count": len(periods),
        "payload": payload,
    }


def raw_file_path(start: datetime, end: datetime, ingested_at: datetime,
                  root: str = RAW_ROOT, folder: str = "national_daily") -> str:
    """Immutable landing path: one file per API call, partitioned by ingestion date."""
    return (
        f"{root}/{folder}/{ingested_at:%Y/%m/%d}/"
        f"carbon_intensity_{start:%Y%m%dT%H%M}_{end:%Y%m%dT%H%M}_{ingested_at:%Y%m%dT%H%M%SZ}.json"
    )
