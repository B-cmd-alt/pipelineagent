"""Canary: the project's own published data.json, fetched over real HTTP like any other source.
It exists so the weekly self-test drills have a feed to break that is not somebody else's data.
`generated_at` is the data's own clock; the summary counts are the values."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from ..schema import Column
from .base import Feed, FetchContext, FetchResult

COLUMNS = (
    Column("ok_cells", "INT64"),
    Column("breach_cells", "INT64"),
    Column("unknown_cells", "INT64"),
    Column("open_findings", "INT64"),
)


def parse(payload: dict) -> tuple[list[dict], list[str]]:
    if "generated_at" not in payload:
        raise ValueError(f"schema: expected key 'generated_at'; got {sorted(payload)[:12]}")
    s = payload.get("summary") or {}
    ts = datetime.fromisoformat(str(payload["generated_at"]).replace("Z", "+00:00")).astimezone(timezone.utc)
    return [{"event_ts": ts, "ok_cells": s.get("ok"), "breach_cells": s.get("breach"), "unknown_cells": s.get("unknown"),
             "open_findings": s.get("open_findings")}], []


def fetch(ctx: FetchContext) -> FetchResult:
    r = ctx.http.get(str(ctx.params["url"]), accept="application/json", expect="json")
    rows, anomalies = parse(json.loads(r.text()))
    return FetchResult(rows, r.sha256, r.last_modified, r.status, anomalies=anomalies, raw=r.body)


FEED = Feed("canary_selfcheck", COLUMNS, fetch)
