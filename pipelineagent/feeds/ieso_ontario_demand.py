"""IESO hourly Ontario demand. The CSV is one growing file per year with
comment lines prefixed by a backslash and HOUR-ENDING values 1..24 in local
Toronto time, so the DST fall-back day has 25 wall-clock hours squeezed into
24 labels: event_ts is derived with the America/Toronto zone and the ambiguity
is recorded as an anomaly rather than papered over. Incremental: only rows
newer than the newest event_ts already stored are appended."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from ..schema import Column
from .base import Feed, FetchContext, FetchResult, to_float

TZ = ZoneInfo("America/Toronto")

COLUMNS = (
    Column("local_date", "DATE", True),
    Column("hour_ending", "INT64", True),
    Column("market_demand", "FLOAT64"),
    Column("ontario_demand", "FLOAT64"),
)


def hour_ending_to_utc(d: date, he: int, anomalies: list[str]) -> datetime:
    """Hour-ending HE (1..24) local -> the UTC instant at the END of that hour.
    Computed from the local midnight + HE hours so DST days keep 24 labels."""
    local_midnight = datetime(d.year, d.month, d.day, tzinfo=TZ)
    start_utc = local_midnight.astimezone(timezone.utc)
    end = start_utc + timedelta(hours=he)
    # A day with a DST transition is 23 or 25 hours long; flag it once.
    next_midnight = datetime.combine(d + timedelta(days=1), datetime.min.time(), tzinfo=TZ).astimezone(timezone.utc)
    day_len = (next_midnight - start_utc).total_seconds() / 3600
    if he == 24 and day_len != 24:
        anomalies.append(f"{d}: DST day is {day_len:g}h long but file has 24 hour-ending labels")
    return end


def parse(text: str, after: datetime | None) -> tuple[list[dict], list[str]]:
    anomalies: list[str] = []
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("\\")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    need = {"Date", "Hour", "Market Demand", "Ontario Demand"}
    if not reader.fieldnames or not need.issubset(set(reader.fieldnames)):
        raise ValueError(f"schema: expected {sorted(need)}, got {reader.fieldnames}")
    rows = []
    for rec in reader:
        try:
            d = date.fromisoformat(rec["Date"])
            he = int(rec["Hour"])
        except (ValueError, TypeError):
            anomalies.append(f"unparseable row {rec!r}")
            continue
        ts = hour_ending_to_utc(d, he, anomalies)
        if after is not None and ts <= after:
            continue
        rows.append({
            "local_date": d,
            "hour_ending": he,
            "event_ts": ts,
            "market_demand": to_float(rec["Market Demand"], anomalies, f"{d} HE{he} market"),
            "ontario_demand": to_float(rec["Ontario Demand"], anomalies, f"{d} HE{he} ontario"),
        })
    return rows, anomalies


def fetch(ctx: FetchContext) -> FetchResult:
    last = ctx.last_run
    r = ctx.http.get("https://reports-public.ieso.ca/public/Demand/PUB_Demand.csv", accept="text/csv,*/*", expect="csv",
                     etag=last.etag if last else None)
    if r.not_modified:
        return FetchResult([], last.content_sha256 if last else None, r.last_modified, 304, r.etag, True)
    # incremental-before-write: what do we already hold? (the data, not the job log)
    after = ctx.warehouse.max_event_ts(ctx.table, lookback_hours=24 * 45) if ctx.warehouse.table_exists(ctx.table) else None
    rows, anomalies = parse(r.text(), after)
    return FetchResult(rows, r.sha256, r.last_modified, r.status, r.etag, anomalies=anomalies,
                       note=f"incremental after {after.isoformat() if after else 'none'}", raw=r.body)


FEED = Feed("ieso_ontario_demand", COLUMNS, fetch, __doc__ or "")
