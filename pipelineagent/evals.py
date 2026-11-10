"""Offline scenario evals (layer 1): replay a scripted sequence of runs for ONE feed through the
real fetcher, detectors, ledger and agent (SQLite, no network, no model), then compare what the
system concluded with the scenario's recorded answer.

A scenario is a YAML file under evals/scenarios/:
  id, origin (where the answer comes from: a drill, a finding, a PR), feed, t0,
  steps:  [{at_h: 0, body: <file under evals/bodies>}, {at_h: 1, fail: timeout}, {at_h: 2, freeze: true}, ...]
  proposal: <dir under evals/recorded holding module.py + test.py + explanation.txt>   (optional)
  expect: {opened: [axes], triage: "class -> action", steps: {step: outcome}, absent_steps: [...],
           gate_codes: [...], disposition: {axis: DISPOSITION}, model_calls: n}

Bodies are templates: {{iso:-0.5}} / {{epoch:-0.5}} (hours from the step's clock) and {{date:-1}}
(days). `freeze: true` re-serves the bytes of the previous ok step (a publisher that stopped moving).
`observe()` runs it, `diff()` lists every mismatch; an empty list is a pass.
"""
from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from .http import Response, SourceError

ROOT = Path(__file__).resolve().parent.parent
SCEN_DIR = ROOT / "evals" / "scenarios"
_TPL = re.compile(r"\{\{(iso|epoch|date):(-?\d+(?:\.\d+)?)\}\}")


def render_body(text: str, now: datetime) -> bytes:
    def sub(m: re.Match) -> str:
        kind, off = m.group(1), float(m.group(2))
        if kind == "date":
            return (now + timedelta(days=off)).date().isoformat()
        t = now + timedelta(hours=off)
        return str(int(t.timestamp())) if kind == "epoch" else t.strftime("%Y-%m-%dT%H:%M:%SZ")
    return _TPL.sub(sub, text).encode()


class ScenarioHttp:
    """Stands in for http.Client for one step: serves the step's body, or fails the way it says."""

    def __init__(self):
        self.used, self.budget, self.last_body = 0, 40, None
        self.body: bytes | None = None
        self.bodies: list[bytes] = []  # raw steps: archived bodies served in request order (multi-request feeds)
        self.fail: str | None = None

    def get(self, url, *, accept="*/*", expect="json", params=None, retries=1):
        self.used += 1
        if self.fail == "timeout":
            raise SourceError("transport: ReadTimeout [scenario]")
        if self.fail == "html":
            self.last_body = b"<!DOCTYPE html><html><body>503</body></html>"
            raise SourceError(f"html_error_page: expected {expect}, got HTML [scenario]")
        body = self.bodies.pop(0) if len(self.bodies) > 1 else (self.bodies[0] if self.bodies else self.body)
        self.last_body = body
        return Response(200, body or b"", "application/json" if expect == "json" else "text/csv", None)


def load(path: Path) -> dict[str, Any]:
    sc = yaml.safe_load(path.read_text())
    sc["_path"] = str(path)
    return sc


def all_scenarios() -> list[dict[str, Any]]:
    return [load(p) for p in sorted(SCEN_DIR.glob("*.yaml"))]


def all_drafts() -> list[dict[str, Any]]:
    return [load(p) for p in sorted((ROOT / "evals" / "drafts").glob("*.yaml"))]


def observe(sc: dict[str, Any]) -> dict[str, Any]:
    """Run the scenario and return what the system concluded, in the same shape as `expect`."""
    from .agent import Agent, AgentContext  # noqa: F401 — Agent is used through the runner
    from .agent.github import DryRunGitHub
    from .agent.patch import PatchProposal
    from .config import load_fleet
    from .runner import ensure_warehouse, load_payload, run
    from .warehouse.sqlite import SQLiteWarehouse

    fleet = load_fleet(ROOT / "fleet.yaml")
    feed = sc["feed"]
    t0 = datetime.fromisoformat(sc["t0"]).replace(tzinfo=timezone.utc)
    calls = {"n": 0}
    proposal = None
    if sc.get("proposal"):
        d = ROOT / "evals" / "recorded" / sc["proposal"]
        proposal = PatchProposal((d / "module.py").read_text(), f"test_eval_{sc['id'].replace('-', '_')}.py",
                                 (d / "test.py").read_text(), (d / "explanation.txt").read_text().strip())

    def proposer(**kw):
        calls["n"] += 1
        return proposal

    saved = {k: os.environ.get(k) for k in ("PIPELINEAGENT_PAYLOAD_DIR", "PIPELINEAGENT_PAYLOAD_BUCKET", "PIPELINEAGENT_INJECT")}
    with tempfile.TemporaryDirectory(prefix="pa-eval-") as tmp:
        os.environ["PIPELINEAGENT_PAYLOAD_DIR"] = str(Path(tmp) / "payloads")
        os.environ.pop("PIPELINEAGENT_PAYLOAD_BUCKET", None)
        os.environ.pop("PIPELINEAGENT_INJECT", None)
        try:
            wh = SQLiteWarehouse(":memory:")
            ensure_warehouse(fleet, wh)
            ctx = AgentContext(fleet=fleet, package_root=ROOT, github=DryRunGitHub(Path(tmp) / "site"), payload_loader=load_payload,
                               deployer=lambda sha: None, proposer=proposer, auto_merge=True)
            http, frozen = ScenarioHttp(), None
            bodies = Path(sc.get("_bodies_root") or ROOT / "evals" / "bodies")
            for step in sc["steps"]:
                now = t0 + timedelta(hours=float(step["at_h"]))
                wh.now_override = now
                http.fail = step.get("fail")
                if step.get("freeze"):
                    http.body, http.bodies = frozen if isinstance(frozen, bytes) else None, list(frozen) if isinstance(frozen, list) else []
                elif step.get("body"):
                    http.body, http.bodies = render_body((bodies / step["body"]).read_text(), now), []
                    frozen = http.body
                elif step.get("raw"):  # archived production bytes, served verbatim (no templating)
                    from .agent.replay import load_payload_bodies
                    http.body, http.bodies = None, load_payload_bodies((bodies / step["raw"]).read_bytes())
                    frozen = list(http.bodies)
                run(fleet, wh, feeds=[feed], http=http, now=now, agent_ctx=ctx)
            events = sorted(wh.ledger_events(5000), key=lambda e: e["ts"])
            opened = [e for e in events if e.get("kind") == "OPENED"]
            ids = [o["finding_id"] for o in opened]
            steps = wh.agent_steps(ids) if ids else []
            disps = wh.latest_dispositions()
            triages = [s["detail"].split(":", 1)[0].strip() for s in steps if s["step"] == "triage"]
            gate = [s["detail"].split(":", 1)[0] for s in steps if s["step"] == "gate"]
            return {
                "opened": sorted({o["axis"] for o in opened}),
                "opened_at_h": {o["axis"]: round((o["ts"] - t0).total_seconds() / 3600) for o in opened},
                "triage": triages[-1] if triages else None,
                "triage_first": triages[0] if triages else None,
                "steps": {s["step"]: s["outcome"] for s in steps},
                "gate_codes": sorted(set(",".join(gate).split(","))) if gate else [],
                "disposition": {o["axis"]: (disps.get(o["finding_id"]) or {}).get("disposition") for o in opened},
                "model_calls": calls["n"],
                "run_statuses": [r["status"] for r in reversed(wh.runs_for_feed(feed, 500))],
            }
        finally:
            for k, v in saved.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)


def diff(observed: dict[str, Any], expect: dict[str, Any]) -> list[str]:
    """Every way the observation disagrees with the recorded answer. Empty = pass.
    `steps` and `gate_codes` are subset checks; everything else is exact."""
    out: list[str] = []
    for key, want in expect.items():
        got = observed.get(key)
        if key == "steps":
            out += [f"step {k}: expected {v}, got {got.get(k)}" for k, v in want.items() if got.get(k) != v]
        elif key == "absent_steps":
            out += [f"step {k} must not happen, but it did ({observed['steps'][k]})" for k in want if k in observed["steps"]]
        elif key == "gate_codes":
            out += [f"gate code {c} expected, got {got}" for c in want if c not in got]
        elif key == "opened":
            if sorted(want) != got:
                out.append(f"opened: expected {sorted(want)}, got {got}")
        elif got != want:
            out.append(f"{key}: expected {want!r}, got {got!r}")
    return out


def run_all() -> list[dict[str, Any]]:
    results = []
    for sc in all_scenarios():
        try:
            mism = diff(observe(sc), sc["expect"])
        except Exception as e:  # noqa: BLE001 — a scenario that cannot run has not passed
            mism = [f"scenario crashed: {e.__class__.__name__}: {str(e)[:200]}"]
        results.append({"id": sc["id"], "origin": sc.get("origin", ""), "passed": not mism, "mismatches": mism})
    return results


def markdown(results: list[dict[str, Any]]) -> str:
    ok = sum(1 for r in results if r["passed"])
    lines = [f"# Scenario evals: {ok}/{len(results)} passed", "", "| scenario | origin | result |", "|---|---|---|"]
    lines += [f"| {r['id']} | {r['origin']} | {'PASS' if r['passed'] else 'FAIL: ' + '; '.join(r['mismatches'])} |" for r in results]
    return "\n".join(lines) + "\n"
