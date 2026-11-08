"""Findings + dispositions, append-only.

A *finding* is one continuous stretch during which a (feed, axis) was not OK.
Its id is deterministic from (feed, axis, opened_at). Every detector run
emits for each open finding exactly one event: OPENED (new), STILL_OPEN
(known; one ledger line, never a re-diagnosis), or CLOSED (back to OK).

Every finding has exactly one *current* disposition (latest row wins):
FIXED | BENIGN | ESCALATED | DEFERRED | UNKNOWN. v0 auto-rules:
  OPENED             -> UNKNOWN  ("awaiting triage")
  CLOSED w/ UNKNOWN  -> BENIGN   ("self-healed without intervention")
A human (or, later, the repair agent with a gate verdict) can override via
`pipelineagent dispose`.
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
    still_open: list[dict] = field(default_factory=list)
    closed: list[dict] = field(default_factory=list)
    dispositions: list[dict] = field(default_factory=list)

    @property
    def finding_rows(self) -> list[dict]:
        return self.opened + self.still_open + self.closed

    def healthlog_lines(self) -> list[str]:
        lines = []
        for r in self.opened:
            lines.append(f"- {r['event_at'].strftime('%Y-%m-%d %H:%M')}Z OPENED `{r['finding_id']}` {r['feed']}/{r['axis']} {r['state']} — {r['detail']} → disposition UNKNOWN (awaiting triage)")
        for r in self.closed:
            lines.append(f"- {r['event_at'].strftime('%Y-%m-%d %H:%M')}Z CLOSED `{r['finding_id']}` {r['feed']}/{r['axis']} — {r['detail']}")
        for d in self.dispositions:
            if d["decided_by"] == "auto" and d["disposition"] == "BENIGN":
                lines.append(f"- {d['decided_at'].strftime('%Y-%m-%d %H:%M')}Z DISPOSITION `{d['finding_id']}` → BENIGN (auto: {d['note']})")
        return lines


def reconcile(detections: list[Detection], open_findings: list[dict], current_dispositions: dict[str, dict],
              run_id: str, now: datetime) -> LedgerDelta:
    """Pure: current detections + currently-open findings -> ledger events."""
    delta = LedgerDelta()
    open_by_key = {(f["feed"], f["axis"]): f for f in open_findings}
    seen: set[tuple[str, str]] = set()
    for d in detections:
        key = (d.feed, d.axis)
        seen.add(key)
        existing = open_by_key.get(key)
        if d.state == "OK":
            if existing:
                delta.closed.append({"finding_id": existing["finding_id"], "event": "CLOSED", "event_at": now, "run_id": run_id,
                                     "feed": d.feed, "axis": d.axis, "state": "OK", "observed": d.observed, "threshold": d.threshold,
                                     "detail": f"back to OK: {d.detail}"})
                cur = current_dispositions.get(existing["finding_id"])
                if cur is None or cur["disposition"] == "UNKNOWN":
                    delta.dispositions.append({"finding_id": existing["finding_id"], "disposition": "BENIGN", "decided_at": now,
                                               "decided_by": "auto", "note": "self-healed without intervention", "evidence_ref": None})
            continue
        # not OK
        if existing:
            delta.still_open.append({"finding_id": existing["finding_id"], "event": "STILL_OPEN", "event_at": now, "run_id": run_id,
                                     "feed": d.feed, "axis": d.axis, "state": d.state, "observed": d.observed, "threshold": d.threshold,
                                     "detail": d.detail})
        else:
            fid = finding_id(d.feed, d.axis, now)
            delta.opened.append({"finding_id": fid, "event": "OPENED", "event_at": now, "run_id": run_id, "feed": d.feed, "axis": d.axis,
                                 "state": d.state, "observed": d.observed, "threshold": d.threshold, "detail": d.detail})
            delta.dispositions.append({"finding_id": fid, "disposition": "UNKNOWN", "decided_at": now, "decided_by": "auto",
                                       "note": "awaiting triage", "evidence_ref": None})
    # Open findings whose (feed, axis) produced no detection this run (axis undeclared now, feed removed):
    # leave them open and untouched — removing a detector never silently closes its findings.
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
