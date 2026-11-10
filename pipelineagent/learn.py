"""Learning from people, the bounded way. When a person settles a finding the agent could not
classify (triage `unknown`), or overrules the agent's own disposition, the evidence is turned into
a scenario DRAFT and opened as a pull request: the archived bytes of every run around the finding,
replayable offline, plus what the agent concluded and what the person concluded.

A draft is not a test yet. It lives in evals/drafts/; a person writes its `expect` (the right
answer) and moves it to evals/scenarios/, where it guards every future change. This module never
edits triage rules, SLAs, detectors or the gate: those stay human decisions, and the agent's own
linter refuses to touch evals/.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Callable

import yaml

from .schema import AGENT_STEPS
from .warehouse.base import Warehouse

log = logging.getLogger("pipelineagent.learn")
TERMINAL_HUMAN = ("BENIGN", "FIXED", "ESCALATED")
MAX_DRAFTS_PER_RUN = 2
MAX_STEPS = 60


def candidates(wh: Warehouse) -> list[dict[str, Any]]:
    """Findings a person has settled where the agent was blind (unknown) or the person disagreed."""
    disps = wh.latest_dispositions()
    human = {fid: d for fid, d in disps.items()
             if str(d.get("decided_by") or "").startswith("human:") and d["disposition"] in TERMINAL_HUMAN}
    if not human:
        return []
    events = sorted(wh.ledger_events(5000), key=lambda e: e["ts"])
    opened = {e["finding_id"]: e for e in events if e.get("kind") == "OPENED"}
    steps_by: dict[str, list[dict]] = {}
    for s in wh.agent_steps(sorted(human)):
        steps_by.setdefault(s["finding_id"], []).append(s)
    out = []
    for fid, d in human.items():
        steps = steps_by.get(fid, [])
        if fid not in opened or any(s["step"] == "scenario_draft" and s["outcome"] in ("ok", "dry_run", "skipped") for s in steps):
            continue
        triages = [str(s["detail"]) for s in steps if s["step"] == "triage"]
        agent_rows = [e for e in events if e.get("kind") == "DISPOSITION" and e["finding_id"] == fid and e.get("actor") == "agent"]
        agent_disp = [e["value"] for e in agent_rows]
        blind = any(t.startswith("unknown") for t in triages)
        overruled = bool(agent_disp) and agent_disp[-1] != d["disposition"] and agent_disp[-1] in TERMINAL_HUMAN
        if blind or overruled:
            out.append({"finding_id": fid, "feed": opened[fid]["feed"], "axis": opened[fid]["axis"], "opened_at": opened[fid]["ts"],
                        "triage": triages[0] if triages else None, "agent_disposition": agent_disp[-1] if agent_disp else None,
                        "agent_decided_at": agent_rows[-1]["ts"] if agent_rows else opened[fid]["ts"],
                        "human": d, "why": "agent could not classify it" if blind else f"person overruled the agent ({agent_disp[-1]} -> {d['disposition']})"})
    return sorted(out, key=lambda c: c["opened_at"])


def build_draft(fleet, wh: Warehouse, c: dict[str, Any], load_payload: Callable[[str, str], bytes | None]) -> tuple[str, dict[str, str | bytes]] | None:
    """(draft id, files to commit) or None when the runs around the finding cannot be reconstructed."""
    spec = fleet.feed(c["feed"])
    slas = [h for h in (spec.sla.write_recency_max_hours, spec.sla.head_lag_max_hours) if h]
    lookback = timedelta(hours=(max(slas) if slas else 24) + 6)
    runs = sorted(wh.runs_for_feed(c["feed"], 500), key=lambda r: r["started_at"])
    # through the agent's last decision, so the replay reaches the same conclusion (defer, then escalate ...)
    end = min(max(c["opened_at"], c.get("agent_decided_at") or c["opened_at"]), c["opened_at"] + timedelta(hours=48)) + timedelta(minutes=30)
    window = [r for r in runs if c["opened_at"] - lookback <= r["started_at"] <= end]
    earlier_ok = [r for r in runs if r["started_at"] < (window[0]["started_at"] if window else c["opened_at"]) and r["status"] == "ok"]
    if earlier_ok:
        window = [earlier_ok[-1]] + window
    if not window or window[0]["status"] != "ok":
        return None  # no healthy starting point: the replay could not tell a breach from a cold start
    window = window[-MAX_STEPS:] if len(window) > MAX_STEPS else window
    did = f"draft-{c['feed'].replace('_', '-')}-{c['finding_id']}"
    t0 = window[0]["started_at"]
    files: dict[str, str | bytes] = {}
    steps = []
    for i, r in enumerate(window):
        at_h = round((r["started_at"] - t0).total_seconds() / 3600, 3)
        sha = r.get("content_sha256")
        err = str(r.get("error") or "")
        if r["status"] == "skipped":
            steps.append({"at_h": at_h, "freeze": True})
        elif sha:
            raw = load_payload(c["feed"], sha)
            if raw is None:
                return None  # an archive gap: refuse to draft a replay that would not be the same bytes
            rel = f"drafts/{did}/{i:02d}.bin"
            files[f"evals/bodies/{rel}"] = raw
            steps.append({"at_h": at_h, "raw": rel})
        elif r["status"] == "failed":
            steps.append({"at_h": at_h, "fail": "html" if "html" in err else "timeout"})
        else:
            steps.append({"at_h": at_h, "freeze": True})
    h = c["human"]
    doc = {
        "id": did,
        "origin": f"finding {c['finding_id']} ({c['feed']}/{c['axis']}, opened {c['opened_at']:%Y-%m-%d %H:%M}Z): {c['why']}",
        "feed": c["feed"], "t0": t0.strftime("%Y-%m-%dT%H:%M:%S"), "steps": steps,
        "live": {"agent_triage": c["triage"], "agent_disposition": c["agent_disposition"],
                 "human_disposition": h["disposition"], "human": h.get("decided_by"), "human_note": h.get("note"), "evidence": h.get("evidence_ref")},
        "expect": {},
    }
    files[f"evals/drafts/{did}.yaml"] = ("# DRAFT: write `expect` (what the system SHOULD conclude), then move this file to evals/scenarios/.\n"
                                          "# `live` is what happened; the replay steps are the archived bytes of every run around the finding.\n"
                                          + yaml.safe_dump(doc, sort_keys=False, width=200, allow_unicode=True))
    return did, files


def draft_scenarios(fleet, wh: Warehouse, github, run_id: str, now: datetime, load_payload) -> list[str]:
    opened = []
    for c in candidates(wh)[:MAX_DRAFTS_PER_RUN]:
        built = build_draft(fleet, wh, c, load_payload)
        if built is None:
            outcome, detail, ref = "skipped", "runs or archived bytes around the finding are missing; nothing replayable to draft", None
        else:
            did, files = built
            body = (f"A person settled finding `{c['finding_id']}` on `{c['feed']}/{c['axis']}`: {c['why']}.\n\n"
                    f"- agent triage: `{c['triage']}`, agent disposition: `{c['agent_disposition']}`\n"
                    f"- person: **{c['human']['disposition']}** ({c['human'].get('decided_by')}): {c['human'].get('note') or ''}\n\n"
                    f"This PR adds `evals/drafts/{did}.yaml` with the archived bytes of {sum(1 for k in files if k.endswith('.bin'))} runs around the finding, "
                    "replayable offline. It is a draft, not a test: write its `expect` block (the right answer) and move it to `evals/scenarios/`; "
                    "if the answer needs a new triage rule, that rule goes in a separate PR and must keep every existing scenario green.\n\n"
                    "Opened by PipelineAgent; never merged automatically.")
            try:
                pr = github.open_pr(branch=f"learn/{did}", files=files, title=f"eval draft from {c['feed']} finding {c['finding_id']}", body=body, verdict=None)
                outcome, detail, ref = ("dry_run" if pr.dry_run else "ok"), f"draft {did} ({c['why']})", pr.url
            except Exception as e:  # noqa: BLE001 — retried next run
                outcome, detail, ref = "failed", f"draft PR not opened: {str(e)[:300]}", None
        wh.append("agent_steps", [{"finding_id": c["finding_id"], "ts": now, "run_id": run_id, "attempt": 0, "step": "scenario_draft",
                                   "outcome": outcome, "detail": detail[:900], "ref": ref}], AGENT_STEPS)
        if outcome in ("ok", "dry_run"):
            opened.append(c["finding_id"])
    return opened
