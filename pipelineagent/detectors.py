"""Out-of-band detectors. They read DATA tables through the warehouse and never
look at job exit codes: a job that exits 0 while writing nothing, or writing
yesterday's numbers again, is exactly what they exist to catch.

Each (feed, axis) yields one Detection with state OK | BREACH | UNKNOWN.
UNKNOWN means "we could not look" (query failed, table missing, not enough
history to evaluate) and must never be rendered as healthy.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .config import AXES, FeedSpec
from .warehouse.base import FeedSnapshot, Warehouse


@dataclass(frozen=True)
class Detection:
    feed: str
    axis: str
    state: str  # OK | BREACH | UNKNOWN
    observed: float | None
    threshold: float | None
    detail: str
    # True for an UNKNOWN that resolves by itself as history accrues (a feed younger than its
    # flat-value window). Rendered amber like any UNKNOWN, but it does not open a finding.
    transient: bool = False

    def as_row(self, run_id: str, at: datetime) -> dict:
        return {"run_id": run_id, "detected_at": at, "feed": self.feed, "axis": self.axis, "state": self.state,
                "observed": self.observed, "threshold": self.threshold, "detail": self.detail}


def _hours(a: datetime, b: datetime) -> float:
    return (a - b).total_seconds() / 3600.0


def lookback_hours(spec: FeedSpec) -> float:
    """Bound every read. Wide enough that 'no rows in window' is itself a breach,
    not a blind spot: 3x the slowest declared SLA, at least 3 days."""
    sla = spec.sla
    cands = [72.0]
    if sla.write_recency_max_hours:
        cands.append(3 * sla.write_recency_max_hours)
    if sla.head_lag_max_hours:
        cands.append(3 * sla.head_lag_max_hours)
    if sla.flat_value:
        cands.append(2 * sla.flat_value.max_consecutive * spec.cadence_hours + 6)
    return max(cands)


def evaluate_snapshot(spec: FeedSpec, snap: FeedSnapshot, now: datetime) -> list[Detection]:
    """Pure function: snapshot -> detections. Unit-testable without a warehouse."""
    out: list[Detection] = []
    sla = spec.sla

    if sla.write_recency_max_hours is not None:
        thr = sla.write_recency_max_hours
        if snap.last_captured_at is None:
            out.append(Detection(spec.id, "write_recency", "BREACH", None, thr, "no rows captured inside the lookback window"))
        else:
            age = _hours(now, snap.last_captured_at)
            out.append(Detection(spec.id, "write_recency", "BREACH" if age > thr else "OK", round(age, 2), thr,
                                 f"last captured_at {snap.last_captured_at.isoformat()} ({age:.1f}h ago)"))

    if sla.head_lag_max_hours is not None:
        thr = sla.head_lag_max_hours
        if snap.last_event_ts is None:
            state = "BREACH" if snap.last_captured_at is not None else "UNKNOWN"
            out.append(Detection(spec.id, "head_lag", state, None, thr,
                                 "rows exist but event_ts is NULL" if state == "BREACH" else "no rows to read a head from"))
        else:
            lag = _hours(now, snap.last_event_ts)
            out.append(Detection(spec.id, "head_lag", "BREACH" if lag > thr else "OK", round(lag, 2), thr,
                                 f"newest event_ts {snap.last_event_ts.isoformat()} ({lag:.1f}h behind now)"))

    if sla.flat_value is not None:
        fv = sla.flat_value
        n = fv.max_consecutive
        groups: dict[str, list[float | None]] = {}
        for g, v, _ in snap.recent_values:  # newest first
            groups.setdefault(g, []).append(v)
        if not groups:
            out.append(Detection(spec.id, "flat_value", "UNKNOWN", 0, n, f"no rows to evaluate {fv.column}"))
        else:
            flat, short, nulls, longest = [], [], [], 0
            for g, vals in groups.items():
                if len(vals) < n:
                    short.append(f"{g or fv.column}({len(vals)}<{n})")
                    continue
                window = vals[:n]
                if all(v is None for v in window):
                    nulls.append(g or fv.column)
                    continue
                run = 1
                for a, b in zip(window, window[1:]):
                    if a is not None and a == b:
                        run += 1
                    else:
                        break
                longest = max(longest, run)
                if run >= n:
                    flat.append(f"{g or fv.column}={window[0]!r}")
            if flat or nulls:
                parts = []
                if flat:
                    parts.append(f"{n} identical consecutive values: {', '.join(flat)}")
                if nulls:
                    parts.append(f"all-NULL window on {', '.join(nulls)}")
                out.append(Detection(spec.id, "flat_value", "BREACH", float(n if flat else 0), n, "; ".join(parts)))
            elif short and not any(len(v) >= n for v in groups.values()):
                out.append(Detection(spec.id, "flat_value", "UNKNOWN", float(max(len(v) for v in groups.values())), n,
                                     f"insufficient history: {', '.join(short)}", transient=True))
            else:
                out.append(Detection(spec.id, "flat_value", "OK", float(longest), n,
                                     f"longest identical run {longest} < {n} on {fv.column}" + (f"; thin groups: {', '.join(short)}" if short else "")))
    return out


def detect_feed(spec: FeedSpec, wh: Warehouse, now: datetime | None = None) -> list[Detection]:
    now = now or datetime.now(timezone.utc)
    declared = [a for a in AXES if spec.sla.declared(a)]
    if not declared:
        return []
    fv = spec.sla.flat_value
    try:
        snap = wh.feed_snapshot(
            spec.table,
            lookback_hours(spec),
            value_column=fv.column if fv else None,
            group_by=fv.group_by if fv else None,
            max_rows=(fv.max_consecutive * 8 + 50) if fv else 0,
        )
    except Exception as e:  # noqa: BLE001 — any read failure is UNKNOWN, loudly
        return [Detection(spec.id, a, "UNKNOWN", None, None, f"detector read failed: {e.__class__.__name__}: {str(e)[:200]}") for a in declared]
    return evaluate_snapshot(spec, snap, now)
