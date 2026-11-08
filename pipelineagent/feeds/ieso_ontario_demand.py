"""IESO hourly Ontario demand. The CSV is one growing file per year with comment lines
prefixed by a backslash and HOUR-ENDING labels 1..24. IESO publishes in fixed Eastern
Standard Time all year (UTC-5, no daylight saving): every day has exactly 24 labels,
including the March and November transition days (verified against the live file, which
carries 24 rows on both). Converting with a DST-aware zone would shift every summer row
by an hour, so event_ts is derived with a constant offset and a day with != 24 labels
is reported as an anomaly. Incremental: only rows newer than the newest event_ts already
stored are appended."""
from __future__ import annotations

import csv
import io
from collections import Counter
from datetime import date, datetime, timedelta, timezone

from ..schema import Column
from .base import Feed, FetchContext, FetchResult, to_float

EST = timezone(timedelta(hours=-5))  # fixed; IESO does not observe DST in its reports

COLUMNS = (
    Column("local_date", "DATE", True),
    Column("hour_ending", "INT64", True),
    Column("market_demand", "FLOAT64"),
    Column("ontario_demand", "FLOAT64"),
)


def hour_ending_to_utc(d: date, he: int) -> datetime:
    """Hour-ending HE (1..24) in fixed EST -> the UTC instant at the END of that hour."""
    return (datetime(d.year, d.month, d.day, tzinfo=EST) + timedelta(hours=he)).astimezone(timezone.utc)


def parse(text: str, after: datetime | None) -> tuple[list[dict], list[str]]:
    anomalies: list[str] = []
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("\\")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    need = {"Date", "Hour", "Market Demand", "Ontario Demand"}
    if not reader.fieldnames or not need.issubset(set(reader.fieldnames)):
        raise ValueError(f"schema: expected {sorted(need)}, got {reader.fieldnames}")
    rows = []
    labels_per_day: Counter = Counter()
    for rec in reader:
        try:
            d = date.fromisoformat(rec["Date"])
            he = int(rec["Hour"])
        except (ValueError, TypeError):
            anomalies.append(f"unparseable row {rec!r}")
            continue
        labels_per_day[d] += 1
        ts = hour_ending_to_utc(d, he)
        if after is not None and ts <= after:
            continue
        rows.append({
            "local_date": d,
            "hour_ending": he,
            "event_ts": ts,
            "market_demand": to_float(rec["Market Demand"], anomalies, f"{d} HE{he} market"),
            "ontario_demand": to_float(rec["Ontario Demand"], anomalies, f"{d} HE{he} ontario"),
        })
    if rows:
        newest_day = max(r["local_date"] for r in rows)
        for d, n in sorted(labels_per_day.items()):
            if n != 24 and d < newest_day:  # the current (still-filling) day is legitimately short
                anomalies.append(f"{d}: {n} hour-ending labels (expected 24)")
    return rows, anomalies


def fetch(ctx: FetchContext) -> FetchResult:
    last = ctx.last_run
    r = ctx.http.get("https://reports-public.ieso.ca/public/Demand/PUB_Demand.csv", accept="text/csv,*/*", expect="csv",
                     etag=ctx.etag)
    if r.not_modified:
        return FetchResult([], last.content_sha256 if last else None, r.last_modified, 304, r.etag, True)
    # incremental-before-write: what do we already hold? (the data, not the job log)
    after = ctx.warehouse.max_event_ts(ctx.table, lookback_hours=24 * 45) if ctx.warehouse.table_exists(ctx.table) else None
    rows, anomalies = parse(r.text(), after)
    return FetchResult(rows, r.sha256, r.last_modified, r.status, r.etag, anomalies=anomalies,
                       note=f"incremental after {after.isoformat() if after else 'none'}", raw=r.body)


FEED = Feed("ieso_ontario_demand", COLUMNS, fetch, __doc__ or "")
