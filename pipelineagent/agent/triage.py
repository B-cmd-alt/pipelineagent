"""Deterministic triage. The only place a model is used is the patch step; everything a
finding needs to be classified is already in the ledger, _runs and the payload archive."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..config import FeedSpec


@dataclass
class Triage:
    failure_class: str   # green_job_dead_data | schema_drift | stale_source | flat_value | unknown
    action: str          # patch | escalate | benign | defer
    rationale: str
    evidence: dict[str, Any] = field(default_factory=dict)


def classify(finding: dict, runs: list[dict], spec: FeedSpec, now: datetime, first_seen: bool = True) -> Triage:
    axis, state = finding["axis"], finding.get("state")
    recent = [r for r in runs if r["status"] != "skipped"][:4]
    failed = [r for r in recent if r["status"] == "failed"]
    latest_fail = failed[0] if failed else None
    err = (latest_fail or {}).get("error") or ""
    reason = (latest_fail or {}).get("reason") or ""

    if latest_fail and ("schema:" in err or reason.startswith("exception:")) and "transport" not in err:
        return Triage("schema_drift", "patch", f"parser rejects the current payload: {err[:160]}",
                      {"content_sha256": latest_fail.get("content_sha256"), "error": err, "run_id": latest_fail.get("run_id")})
    if latest_fail and (reason in ("source_error", "rate_limited") or "transport" in err or "HTTP" in err):
        if first_seen:
            return Triage("stale_source", "defer", f"upstream failing ({reason}: {err[:120]}); waiting one run before escalating", {"error": err})
        return Triage("stale_source", "escalate", f"upstream keeps failing ({reason}: {err[:120]}); nothing in this repo can fix the source", {"error": err})
    if axis == "write_recency" and recent and all(r["status"] in ("ok", "no_change") for r in recent):
        return Triage("green_job_dead_data", "escalate", "job reports success but no rows land inside the SLA window", {"runs": [r["status"] for r in recent]})
    if axis == "flat_value" and state == "BREACH":
        return Triage("flat_value", "escalate", "values frozen upstream; the fetcher is faithfully recording a stuck source")
    if axis == "head_lag" and state == "BREACH" and recent and all(r["status"] in ("ok", "no_change") for r in recent):
        return Triage("stale_source", "escalate", "rows arrive but the data's own clock is behind the SLA; the publisher is late")
    if state == "UNKNOWN":
        return Triage("unknown", "escalate", f"detector could not look: {finding.get('detail','')[:160]}")
    return Triage("unknown", "escalate", f"no triage rule matched (axis={axis}, state={state}, runs={[r['status'] for r in recent]})")
