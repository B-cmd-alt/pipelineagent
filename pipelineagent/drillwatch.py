"""Layer-2 watchdog: the scheduled drills are only worth something if a bad result makes noise.
After every run the drill feeds' benchmark episodes are read back; a WRONG or MISSED verdict, or
no drill at all for more than eight days, opens exactly one issue and turns the dashboard line red.

An alarm stays red until a person settles it: `pipelineagent dispose <alarm id> FIXED|BENIGN --who ...
--note ... --evidence-ref <the fixing PR>`. The episode keeps its WRONG verdict in the benchmark; what
changes is that the page shows the cause and the fix next to it instead of an unexplained warning.
Only a person can do this: the agent never writes a disposition for an alarm id."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from . import benchmark
from .schema import AGENT_STEPS
from .warehouse.base import Warehouse

STALE_AFTER = timedelta(days=8)
SETTLES = ("FIXED", "BENIGN")


def status(fleet, wh: Warehouse, now: datetime) -> list[dict[str, Any]]:
    """One row per drill feed: the latest episode, and every alarm condition that currently holds."""
    drills = getattr(fleet, "drills", ())
    if not drills:
        return []
    eps = benchmark.run(wh, now)["episodes"]
    disps = wh.latest_dispositions()
    out = []
    for d in drills:
        mine = [e for e in eps if e.feed == d.feed]
        last = mine[-1] if mine else None
        alarms = [{"id": f"drill:{d.feed}:{e.start:%Y%m%d%H}", "why": f"{e.failure_class} drill injected {e.start:%Y-%m-%d %H:%M}Z ended {e.verdict} (expected {e.expected})"}
                  for e in mine if e.verdict == "MISSED" or e.verdict.startswith("WRONG")]
        if last and now - last.end > STALE_AFTER:
            alarms.append({"id": f"drill:{d.feed}:stale:{now:%G%V}", "why": f"no drill has run since {last.end:%Y-%m-%d %H:%M}Z (more than {STALE_AFTER.days} days)"})
        settled = []
        for al in alarms:
            dd = disps.get(al["id"]) or {}
            if str(dd.get("decided_by") or "").startswith("human:") and dd.get("disposition") in SETTLES:
                settled.append({**al, "by": dd["decided_by"], "at": dd["decided_at"].isoformat(), "note": dd.get("note"), "evidence": dd.get("evidence_ref")})
        done = {x["id"] for x in settled}
        alarms = [al for al in alarms if al["id"] not in done]
        out.append({"feed": d.feed, "episodes": len(mine), "correct": sum(1 for e in mine if e.verdict == "CORRECT"), "settled": settled,
                    "last": None if not last else {"class": last.failure_class, "kind": last.kind, "injected": last.start.isoformat(), "verdict": last.verdict,
                                                   "outcome": last.outcome, "mttd_min": last.mttd_min, "mttr_min": last.mttr_min, "evidence": last.evidence_ref},
                    "alarms": alarms, "state": "BREACH" if alarms else ("OK" if last else "UNKNOWN")})
    return out


def check(fleet, wh: Warehouse, github, run_id: str, now: datetime) -> list[str]:
    """Open one issue per new alarm; the alarm id in agent_steps is the memory. Returns the ids raised now."""
    raised = []
    for row in status(fleet, wh, now):
        for a in row["alarms"]:
            if any(s["step"] == "drill_alarm" and s["outcome"] == "ok" for s in wh.agent_steps([a["id"]])):
                continue
            try:
                url = github.open_issue(a["id"], f"PipelineAgent self-test failed: {row['feed']}", f"The scheduled drill did not end as it must.\n\n- {a['why']}\n\n"
                                        "Run `python -m pipelineagent benchmark` for the episode table and `python -m pipelineagent eval` for the offline scenarios. "
                                        "Until this is understood, a real failure of the same class may be mishandled the same way.")
                outcome, detail, ref = "ok", a["why"], url
            except Exception as e:  # noqa: BLE001 — retried next run
                outcome, detail, ref = "failed", f"{a['why']} (issue not opened: {str(e)[:200]})", None
            wh.append("agent_steps", [{"finding_id": a["id"], "ts": now, "run_id": run_id, "attempt": 0, "step": "drill_alarm", "outcome": outcome,
                                       "detail": detail[:900], "ref": ref}], AGENT_STEPS)
            if outcome == "ok":
                raised.append(a["id"])
    return raised
