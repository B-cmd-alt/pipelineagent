"""FRED series. Missing prints are '.', which is exactly the "N/A in a numeric
column" hazard: coerced to NULL and recorded as an anomaly, never silently dropped.
Vintage-first: the last N observations are appended on every content change, so
restatements are kept.

Two sources, one row shape. With FRED_API_KEY set, the official API
(api.stlouisfed.org, JSON) is used: it is the route FRED provides for automated
clients. Without a key the public fredgraph CSV is used; that is a website endpoint
and FRED stalls this client's requests to it (the 2026-11-08 incident), so it is a
fallback for local runs, not something to rely on. The parser is chosen from the
bytes, not from the route, so an archived payload of either kind replays."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from datetime import date, datetime, timezone

from ..http import SourceError, looks_like_html
from ..schema import Column
from .base import Feed, FetchContext, FetchResult, to_float

COLUMNS = (
    Column("series_id", "STRING", True),
    Column("observation_date", "DATE", True),
    Column("value", "FLOAT64"),
    Column("raw_value", "STRING"),
)

CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
API_URL = "https://api.stlouisfed.org/fred/series/observations"


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


def _observations(text: str) -> list[tuple[str, str]]:
    """(date, value) pairs from the API's JSON, oldest first."""
    try:
        obs = json.loads(text).get("observations")
    except (ValueError, AttributeError) as e:
        raise ValueError(f"schema: expected a JSON object with observations; {e}") from None
    if not isinstance(obs, list) or any(not isinstance(o, dict) or "date" not in o or "value" not in o for o in obs):
        raise ValueError("schema: expected observations[] of {date, value}")
    return sorted((str(o["date"]), str(o["value"])) for o in obs)


def parse_json(text: str, series_id: str, keep_last_n: int) -> tuple[list[dict], list[str]]:
    """The API's observations, restated as the CSV and handed to parse(): one row builder."""
    return parse(f"observation_date,{series_id}\n" + "".join(f"{d},{v}\n" for d, v in _observations(text)), series_id, keep_last_n)


def canonical(text: str) -> bytes:
    """The API echoes the request date (realtime_start/end) in every response, so the raw
    body changes daily even when no observation did. Only the observations are hashed and
    archived, which keeps 'identical content is a no_change' true on weekends."""
    return json.dumps({"observations": [{"date": d, "value": v} for d, v in _observations(text)]}, separators=(",", ":")).encode()


def fetch(ctx: FetchContext) -> FetchResult:
    sid = str(ctx.params.get("series_id", "DGS10"))
    n = int(ctx.params.get("keep_last_n", 30))
    key = os.environ.get("FRED_API_KEY", "").strip()
    if key:
        # The API takes the key only as a query parameter, and a transport error quotes the
        # full URL; errors end up in the runs table and on the public dashboard.
        try:
            r = ctx.http.get(API_URL, accept="application/json", expect="any",
                             params={"series_id": sid, "api_key": key, "file_type": "json", "sort_order": "desc", "limit": n})
        except SourceError as e:
            raise type(e)(str(e).replace(key, "***")) from None
    else:
        r = ctx.http.get(CSV_URL, params={"id": sid}, accept="text/csv,*/*", expect="any")
    if looks_like_html(r.body, r.content_type):
        raise SourceError(f"html_error_page: expected data, got HTML ({len(r.body)} bytes) from FRED")
    text = r.text()
    if text.lstrip()[:1] == "{":
        rows, anomalies = parse_json(text, sid, n)
        body = canonical(text)
        return FetchResult(rows, hashlib.sha256(body).hexdigest(), r.last_modified, r.status, anomalies=anomalies, raw=body)
    rows, anomalies = parse(text, sid, n)
    return FetchResult(rows, r.sha256, r.last_modified, r.status, anomalies=anomalies, raw=r.body)


FEED = Feed("fred_dgs10", COLUMNS, fetch)
