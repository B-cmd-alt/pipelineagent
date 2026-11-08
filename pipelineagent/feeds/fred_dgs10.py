"""FRED series via the public fredgraph CSV (no key). Missing prints are '.',
which is exactly the "N/A in a numeric column" hazard: coerced to NULL and
recorded as an anomaly, never silently dropped. Vintage-first: the last N
observations are appended on every content change, so restatements are kept."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone

from ..schema import Column
from .base import Feed, FetchContext, FetchResult, to_float

COLUMNS = (
    Column("series_id", "STRING", True),
    Column("observation_date", "DATE", True),
    Column("value", "FLOAT64"),
    Column("raw_value", "STRING"),
)


def parse(text: str, series_id: str, keep_last_n: int) -> tuple[list[dict], list[str]]:
    anomalies: list[str] = []
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "observation_date" not in reader.fieldnames or series_id not in reader.fieldnames:
        raise ValueError(f"schema: expected columns observation_date,{series_id}; got {reader.fieldnames}")
    recs = list(reader)[-keep_last_n:]
    rows = []
    for rec in recs:
        d = date.fromisoformat(rec["observation_date"])
        raw = rec[series_id]
        rows.append({
            "series_id": series_id,
            "observation_date": d,
            "event_ts": datetime(d.year, d.month, d.day, tzinfo=timezone.utc),
            "value": None if raw == "." else to_float(raw, anomalies, f"{series_id}@{d}"),
            "raw_value": raw,
        })
    if any(r["raw_value"] == "." for r in rows):
        anomalies.append(f"{sum(1 for r in rows if r['raw_value'] == '.')} missing ('.') prints in last {keep_last_n}")
    return rows, anomalies


def fetch(ctx: FetchContext) -> FetchResult:
    sid = str(ctx.params.get("series_id", "DGS10"))
    n = int(ctx.params.get("keep_last_n", 30))
    r = ctx.http.get("https://fred.stlouisfed.org/graph/fredgraph.csv", params={"id": sid}, accept="text/csv,*/*", expect="csv")
    rows, anomalies = parse(r.text(), sid, n)
    return FetchResult(rows, r.sha256, r.last_modified, r.status, anomalies=anomalies, raw=r.body)


FEED = Feed("fred_dgs10", COLUMNS, fetch)
