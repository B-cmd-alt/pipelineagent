"""Dead-man's switch. One row per completed loop run. The dashboard turns red
when the newest row is older than `heartbeat_max_age_hours`. The row carries the
runtime code hash so deploy drift is visible on the same screen."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from .schema import HEARTBEAT
from .version import code_sha256
from .warehouse.base import Warehouse


def beat(wh: Warehouse, run_id: str, *, feeds_attempted: int, feeds_ok: int, breaches: int, unknowns: int, duration_s: float,
         now: datetime | None = None, fleet_path: str | None = None) -> dict:
    row = {
        "ts": now or datetime.now(timezone.utc),
        "run_id": run_id,
        "code_sha256": code_sha256(fleet_path),
        "git_commit": os.environ.get("GIT_COMMIT") or None,
        "feeds_attempted": feeds_attempted,
        "feeds_ok": feeds_ok,
        "breaches": breaches,
        "unknowns": unknowns,
        "duration_s": round(duration_s, 2),
    }
    wh.append("heartbeat", [row], HEARTBEAT)
    return row


def status(last: dict | None, max_age_hours: float, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    if not last:
        return {"state": "MISSING", "age_hours": None, "last_ts": None, "max_age_hours": max_age_hours}
    age = (now - last["ts"]).total_seconds() / 3600
    return {"state": "STALE" if age > max_age_hours else "ALIVE", "age_hours": round(age, 2), "last_ts": last["ts"].isoformat(),
            "max_age_hours": max_age_hours, "code_sha256": last.get("code_sha256"), "git_commit": last.get("git_commit")}
