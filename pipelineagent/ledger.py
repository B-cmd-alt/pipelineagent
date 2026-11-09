"""Findings + dispositions, append-only.

A *finding* is one continuous stretch during which a (feed, axis) was not OK.
Its id is deterministic from (feed, axis, opened_at). The ledger records only
TRANSITIONS — OPENED when a (feed, axis) leaves OK, CLOSED when it returns —
because the hourly state already lives in `detections`; a known-open finding
costs nothing per run and is never re-diagnosed.

Every finding has exactly one *current* disposition (latest row wins):
FIXED | BENIGN | ESCALATED | DEFERRED | UNKNOWN. v0 auto-rules:
  OPENED             -> UNKNOWN  ("awaiting triage")
  CLOSED w/ auto UNKNOWN -> BENIGN ("self-healed without intervention"; an agent/human row is never overridden)
A human (or, later, the repair agent with a gate verdict) overrides via
`pipelineagent dispose`. A transient UNKNOWN (a feed still accruing the
history its flat-value window needs) is shown on the dashboard but opens no
finding.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime

from .detectors import Detection
from .schema import DISPOSITION_VALUES, DISPOSITIONS, FINDINGS
from .warehouse.base import Warehouse


def finding_id(feed: str, axis: str, opened_at: datetime) -> str:
    return hashlib.sha256(f"{feed}|{axis}|{opened_at.isoformat()}".encode()).hexdigest()[:16]


@dataclass
class LedgerDelta:
    opened: list[dict] = field(default_factory=list)
    closed: list[dict] = field(default_factory=list)
    dispositions: list[dict] = field(default_factory=list)

    @property
    def finding_rows(self) -> list[dict]:
        return self.opened + self.closed


def reconcile(detections: list[Detection], open_findings: list[dict], current_dispositions: dict[str, dict],
              run_id: str, now: datetime) -> LedgerDelta:
    """Pure: current detections + currently-open findings -> transition events."""
    delta = LedgerDelta()
    open_by_key = {(f["feed"], f["axis"]): f for f in open_findings}
    for d in detections:
        existing = open_by_key.get((d.feed, d.axis))
        if d.state == "OK" or d.transient:
            if existing:
                delta.closed.append({"finding_id": existing["finding_id"], "event": "CLOSED", "event_at": now, "run_id": run_id,
                                     "feed": d.feed, "axis": d.axis, "state": d.state, "observed": d.observed, "threshold": d.threshold,
                                     "detail": f"back to {d.state}: {d.detail}"})
                cur = current_dispositions.get(existing["finding_id"])
                # only an untriaged finding self-heals; an agent's "awaiting verification" UNKNOWN is the
                # repair loop's own state and the agent decides FIXED (with evidence) or not
                if cur is None or (cur["disposition"] == "UNKNOWN" and cur["decided_by"] == "auto"):
                    delta.dispositions.append({"finding_id": existing["finding_id"], "disposition": "BENIGN", "decided_at": now,
                                               "decided_by": "auto", "note": "self-healed without intervention", "evidence_ref": None})
        elif not existing:
            fid = finding_id(d.feed, d.axis, now)
            delta.opened.append({"finding_id": fid, "event": "OPENED", "event_at": now, "run_id": run_id, "feed": d.feed, "axis": d.axis,
                                 "state": d.state, "observed": d.observed, "threshold": d.threshold, "detail": d.detail})
            delta.dispositions.append({"finding_id": fid, "disposition": "UNKNOWN", "decided_at": now, "decided_by": "auto",
                                       "note": "awaiting triage", "evidence_ref": None})
        # else: known-open and still not OK -> nothing to write; detections carries this hour's state.
    # Open findings whose (feed, axis) produced no detection this run (axis undeclared now, feed removed)
    # stay open and untouched — removing a detector never silently closes its findings.
    return delta


def apply(delta: LedgerDelta, wh: Warehouse) -> None:
    wh.append("findings", delta.finding_rows, FINDINGS)
    wh.append("dispositions", delta.dispositions, DISPOSITIONS)


def dispose(wh: Warehouse, fid: str, disposition: str, who: str, note: str, now: datetime, evidence_ref: str | None = None) -> dict:
    if disposition not in DISPOSITION_VALUES:
        raise ValueError(f"disposition must be one of {DISPOSITION_VALUES}")
    if disposition == "FIXED" and not evidence_ref:
        raise ValueError("FIXED requires an evidence_ref (a gate verdict); a fix without evidence is a claim, not a fix")
    row = {"finding_id": fid, "disposition": disposition, "decided_at": now, "decided_by": who, "note": note, "evidence_ref": evidence_ref}
    wh.append("dispositions", [row], DISPOSITIONS)
    return row
