"""Benchmark (DESIGN §5): join the harness ground truth against the ledger.

An *episode* is one contiguous stretch of the same (feed, kind) injection. For each
episode: was it detected (first OPENED on that feed at/after the first injected run), how
long did that take (MTTD), what terminal disposition did the agent reach and how long after
detection (MTTR), how many gate rejections on the way, and did the outcome match what the
class demands (schema_drift → FIXED; everything else → ESCALATED). Findings opened on a feed
with no injection active are listed separately as *uninjected*: real incidents or false
alarms, the benchmark cannot tell and does not pretend to.

Baseline: watchdog-only has the same detectors, so the same MTTD, and no MTTR at all — a
finding waits for a person. That is the comparison the numbers make.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .warehouse.base import Warehouse

EXPECTED = {"schema_drift": "FIXED"}  # every other class: nothing to patch -> ESCALATED
EXPECTED_BY_KIND = {"moving_target": "ESCALATED"}  # patchable in form, unfixable in fact: must merge, fail, revert, escalate
TERMINAL = ("FIXED", "ESCALATED")
GAP = timedelta(hours=3)  # a hole longer than this between injected runs ends the episode
PENDING_WINDOW = timedelta(hours=48)  # from the FIRST injected run: longer than any SLA in the fleet, then it is a miss


@dataclass
class Episode:
    feed: str
    kind: str
    failure_class: str
    start: datetime
    end: datetime
    finding_ids: list[str] = field(default_factory=list)
    detected_at: datetime | None = None
    resolved_at: datetime | None = None
    outcome: str | None = None
    evidence_ref: str | None = None
    gate_rejections: int = 0
    prs: int = 0
    human_requeues: int = 0
    reverts: int = 0
    model_cost_usd: float = 0.0    # summed from the patch steps' recorded OpenRouter cost
    triaged_as: str | None = None  # the agent's FIRST classification of the primary finding, vs the injected truth
    pending: bool = False  # still inside the detection window; not yet a miss

    @property
    def expected(self) -> str:
        return EXPECTED_BY_KIND.get(self.kind) or EXPECTED.get(self.failure_class, "ESCALATED")

    @property
    def mttd_min(self) -> float | None:
        return None if self.detected_at is None else round((self.detected_at - self.start).total_seconds() / 60)

    @property
    def mttr_min(self) -> float | None:
        return None if self.resolved_at is None or self.detected_at is None else round((self.resolved_at - self.detected_at).total_seconds() / 60)

    @property
    def verdict(self) -> str:
        if self.detected_at is None:
            return "PENDING" if self.pending else "MISSED"
        if self.outcome is None:
            return "OPEN"
        return "CORRECT" if self.outcome == self.expected else f"WRONG ({self.outcome})"


def episodes(injections: list[dict[str, Any]]) -> list[Episode]:
    out: list[Episode] = []
    for row in sorted(injections, key=lambda r: r["ts"]):
        last = next((e for e in reversed(out) if e.feed == row["feed"] and e.kind == row["kind"]), None)
        if last and row["ts"] - last.end <= GAP:
            last.end = row["ts"]
        else:
            out.append(Episode(row["feed"], row["kind"], row["failure_class"], row["ts"], row["ts"]))
    return out


def run(wh: Warehouse, now: datetime | None = None) -> dict[str, Any]:
    eps = episodes(wh.injections())
    now = now or datetime.now(tz=eps[0].start.tzinfo) if eps else now
    events = wh.ledger_events(5000)
    opened = [e for e in events if e.get("kind") == "OPENED"]
    disps = [e for e in events if e.get("kind") == "DISPOSITION"]
    claimed: set[str] = set()
    for ep in eps:
        # findings this episode produced: opened on the feed after the first injected run, before the end + a day;
        # a finding belongs to exactly one episode (the earliest that can claim it)
        mine = sorted((o for o in opened if o["feed"] == ep.feed and o["finding_id"] not in claimed
                       and ep.start <= o["ts"] <= ep.end + timedelta(hours=36)), key=lambda o: o["ts"])
        ep.finding_ids = [o["finding_id"] for o in mine]
        claimed.update(ep.finding_ids)
        if not mine:
            ep.pending = now is not None and now - ep.start < PENDING_WINDOW
            continue
        ep.detected_at = mine[0]["ts"]
        # the LATEST disposition decides (latest row wins, as in the ledger): terminal -> the outcome,
        # MTTR runs to it; a later non-terminal row (a human re-queue) means the episode is open again
        # ... of the FIRST finding: a later finding on the same feed (another axis catching up, or the aftermath
        # of the drill) is listed with the episode but does not move its outcome or its MTTR
        rows = sorted((d for d in disps if d["finding_id"] == ep.finding_ids[0] and d["ts"] >= ep.detected_at), key=lambda d: d["ts"])
        if rows and rows[-1]["value"] in TERMINAL:
            ep.resolved_at, ep.outcome = rows[-1]["ts"], rows[-1]["value"]
        ep.human_requeues = sum(1 for d in disps if d["finding_id"] in ep.finding_ids and d["value"] == "DEFERRED" and str(d.get("actor") or "").startswith("human:"))
        steps = wh.agent_steps(ep.finding_ids)
        tri = [str(s["detail"]).split("->", 1)[0].strip() for s in steps if s["step"] == "triage" and "->" in str(s["detail"])]
        ep.triaged_as = tri[0] if tri else None
        ep.model_cost_usd = round(sum(float(m.group(1)) for s in steps if s["step"] == "patch"
                                      for m in [re.search(r"cost \$([0-9.]+)\]", str(s["detail"]))] if m), 6)
        ep.gate_rejections = sum(1 for s in steps if s["step"] == "gate" and s["outcome"] == "failed")
        ep.prs = sum(1 for s in steps if s["step"] == "pr" and s["outcome"] == "ok")
        ep.reverts = sum(1 for s in steps if s["step"] == "revert" and s["outcome"] == "ok")
        want = "pr" if ep.outcome == "FIXED" else "escalate"
        ep.evidence_ref = next((s["ref"] for s in reversed(steps) if s["step"] == want and s["outcome"] == "ok" and s.get("ref")), None)
    uninjected = [o for o in opened if o["finding_id"] not in claimed]
    detected = [e for e in eps if e.detected_at]
    resolved = [e for e in eps if e.resolved_at]
    return {
        "episodes": eps,
        "uninjected_findings": uninjected,
        "totals": {
            "episodes": len(eps),
            "detected": len(detected),
            "correct": sum(1 for e in eps if e.verdict == "CORRECT"),
            "wrong": sum(1 for e in eps if e.verdict.startswith("WRONG")),
            "open": sum(1 for e in eps if e.verdict == "OPEN"),
            "missed": sum(1 for e in eps if e.verdict == "MISSED"),
            "pending": sum(1 for e in eps if e.verdict == "PENDING"),
            "human_requeues": sum(e.human_requeues for e in eps),
            "triaged": sum(1 for e in eps if e.triaged_as),
            "model_cost_usd": round(sum(e.model_cost_usd for e in eps), 4),
            "reverts": sum(e.reverts for e in eps),
            "triage_exact": sum(1 for e in eps if e.triaged_as and e.triaged_as == e.failure_class),
            "median_mttd_min": _median([e.mttd_min for e in detected]),
            "median_mttr_min": _median([e.mttr_min for e in resolved]),
            "gate_rejections": sum(e.gate_rejections for e in eps),
            "uninjected_findings": len(uninjected),
        },
    }


def _median(xs: list[float | None]) -> float | None:
    v = sorted(x for x in xs if x is not None)
    if not v:
        return None
    mid = len(v) // 2
    return v[mid] if len(v) % 2 else (v[mid - 1] + v[mid]) / 2


def markdown(result: dict[str, Any]) -> str:
    t = result["totals"]
    lines = ["# Benchmark: injected failures vs. the ledger", "",
             f"Episodes {t['episodes']} · detected {t['detected']} · correct outcome {t['correct']} · wrong {t['wrong']} · still open {t['open']} · pending {t['pending']} · missed {t['missed']}",
             f"Median MTTD {t['median_mttd_min']} min · median MTTR {t['median_mttr_min']} min (detection → final FIXED/ESCALATED, human re-queues included: {t['human_requeues']}) · gate rejections {t['gate_rejections']} · automatic reverts {t['reverts']} · uninjected findings {t['uninjected_findings']}",
             f"Model spend on these episodes: ${t['model_cost_usd']} (recorded per patch from OpenRouter's billed usage; episodes before that was recorded show $0)",
             f"Triage classified {t['triage_exact']}/{t['triaged']} triaged episodes as the injected class (exact match; the baseline for any proposed triage rule)",
             "", "Baseline (watchdog-only): identical detectors, so identical MTTD; MTTR is undefined because nothing acts — every finding waits for a person.", "",
             "| feed | class | triaged as | injected (UTC) | detected | MTTD min | outcome | expected | MTTR min | gate rejects | PRs | verdict | evidence |", "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for e in result["episodes"]:
        lines.append(f"| {e.feed} | {e.failure_class} | {e.triaged_as or '—'} | {e.start:%Y-%m-%d %H:%M} | {e.detected_at:%H:%M} | {e.mttd_min} | {e.outcome or '—'} | {e.expected} | {e.mttr_min if e.mttr_min is not None else '—'} | {e.gate_rejections} | {e.prs} | {e.verdict} | {e.evidence_ref or ''} |"
                     if e.detected_at else f"| {e.feed} | {e.failure_class} | — | {e.start:%Y-%m-%d %H:%M} | — | — | — | {e.expected} | — | 0 | 0 | {e.verdict} | |")
    if result["uninjected_findings"]:
        lines += ["", "Findings opened with no injection active (real incidents or false alarms — not scored):", ""]
        lines += [f"- {o['ts']:%Y-%m-%d %H:%M} `{o['finding_id']}` {o['feed']}/{o['axis']}" for o in result["uninjected_findings"]]
    lines += ["", "UNKNOWN is never scored as a correct no-breach: during an injection it would be a miss; outside one it is blind time, shown per feed on the dashboard."]
    return "\n".join(lines) + "\n"
