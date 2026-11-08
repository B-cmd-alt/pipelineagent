"""Open-Meteo current conditions, one request for all points."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from ..schema import Column
from .base import Feed, FetchContext, FetchResult, to_float

COLUMNS = (
    Column("location", "STRING", True),
    Column("latitude", "FLOAT64"),
    Column("longitude", "FLOAT64"),
    Column("temperature_2m", "FLOAT64"),
    Column("wind_speed_10m", "FLOAT64"),
    Column("generationtime_ms", "FLOAT64"),
)


def parse(payload, points: list[dict]) -> tuple[list[dict], list[str]]:
    anomalies: list[str] = []
    items = payload if isinstance(payload, list) else [payload]
    if len(items) != len(points):
        anomalies.append(f"expected {len(points)} locations, got {len(items)}")
    rows = []
    for pt, it in zip(points, items):
        cur = it.get("current") or {}
        t = cur.get("time")
        rows.append({
            "location": pt["name"],
            "latitude": it.get("latitude"),
            "longitude": it.get("longitude"),
            "event_ts": datetime.fromisoformat(t).replace(tzinfo=timezone.utc) if t else None,
            "temperature_2m": to_float(cur.get("temperature_2m"), anomalies, f"{pt['name']}.temperature_2m"),
            "wind_speed_10m": to_float(cur.get("wind_speed_10m"), anomalies, f"{pt['name']}.wind_speed_10m"),
            "generationtime_ms": it.get("generationtime_ms"),
        })
    return rows, anomalies


def fetch(ctx: FetchContext) -> FetchResult:
    points = list(ctx.params.get("points") or [])
    params = {
        "latitude": ",".join(str(p["latitude"]) for p in points),
        "longitude": ",".join(str(p["longitude"]) for p in points),
        "current": "temperature_2m,wind_speed_10m",
        "timezone": "UTC",
    }
    r = ctx.http.get("https://api.open-meteo.com/v1/forecast", params=params, accept="application/json", expect="json")
    rows, anomalies = parse(json.loads(r.text()), points)
    return FetchResult(rows, r.sha256, r.last_modified, r.status, anomalies=anomalies, raw=r.body)


FEED = Feed("open_meteo_current", COLUMNS, fetch, __doc__ or "")
