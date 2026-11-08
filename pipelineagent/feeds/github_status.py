"""GitHub status page summary (Statuspage.io API)."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from ..schema import Column
from .base import Feed, FetchContext, FetchResult

COLUMNS = (
    Column("status_indicator", "STRING"),
    Column("status_description", "STRING"),
    Column("components_total", "INT64"),
    Column("components_not_operational", "INT64"),
    Column("incidents_open", "INT64"),
    Column("scheduled_maintenances", "INT64"),
)


def parse(payload: dict) -> dict:
    comps = payload.get("components") or []
    upd = (payload.get("page") or {}).get("updated_at")
    return {
        "event_ts": datetime.fromisoformat(upd.replace("Z", "+00:00")).astimezone(timezone.utc) if upd else None,
        "status_indicator": (payload.get("status") or {}).get("indicator"),
        "status_description": (payload.get("status") or {}).get("description"),
        "components_total": len(comps),
        "components_not_operational": sum(1 for c in comps if c.get("status") != "operational"),
        "incidents_open": len(payload.get("incidents") or []),
        "scheduled_maintenances": len(payload.get("scheduled_maintenances") or []),
    }


def fetch(ctx: FetchContext) -> FetchResult:
    r = ctx.http.get("https://www.githubstatus.com/api/v2/summary.json", accept="application/json", expect="json")
    return FetchResult([parse(json.loads(r.text()))], r.sha256, r.last_modified, r.status, raw=r.body)


FEED = Feed("github_status", COLUMNS, fetch, __doc__ or "")
