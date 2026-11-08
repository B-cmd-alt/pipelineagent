"""USGS all-earthquakes-past-hour GeoJSON. One summary row per capture; the
feed's `metadata.generated` is the data clock."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from ..schema import Column
from .base import Feed, FetchContext, FetchResult

COLUMNS = (
    Column("feature_count", "INT64"),
    Column("max_magnitude", "FLOAT64"),
    Column("newest_event_ts", "TIMESTAMP"),
    Column("api_version", "STRING"),
)


def parse(payload: dict) -> dict:
    md = payload.get("metadata") or {}
    feats = payload.get("features") or []
    mags = [f["properties"].get("mag") for f in feats if isinstance(f.get("properties"), dict) and f["properties"].get("mag") is not None]
    times = [f["properties"].get("time") for f in feats if isinstance(f.get("properties"), dict) and f["properties"].get("time")]
    gen = md.get("generated")
    return {
        "event_ts": datetime.fromtimestamp(gen / 1000, tz=timezone.utc) if gen else None,
        "feature_count": md.get("count", len(feats)),
        "max_magnitude": max(mags) if mags else None,
        "newest_event_ts": datetime.fromtimestamp(max(times) / 1000, tz=timezone.utc) if times else None,
        "api_version": md.get("api"),
    }


def fetch(ctx: FetchContext) -> FetchResult:
    r = ctx.http.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson", accept="application/json", expect="json")
    return FetchResult([parse(json.loads(r.text()))], r.sha256, r.last_modified, r.status, raw=r.body)


FEED = Feed("usgs_earthquakes_hour", COLUMNS, fetch)
