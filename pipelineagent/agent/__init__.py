"""The repair agent. One code path for "notified" and "patrols": the hourly runner calls
`run()` right after the ledger with this run's OPENED findings plus the DEFERRED ones.

Depth budget 2 per run. Truth lives in the ledger (dispositions + agent_steps) and GitHub;
everything here is append-only and idempotent per (finding, attempt).
"""
from __future__ import annotations

import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ..config import Fleet
from ..ledger import dispose
from ..schema import AGENT_STEPS
from ..warehouse.base import Warehouse
from . import triage as triage_mod
from .gate import check as gate_check
from .github import GitHub, GitHubError
from .patch import PatchProposal, apply_proposal, propose
from .replay import load_payload_bodies

log = logging.getLogger("pipelineagent.agent")

DEPTH_BUDGET = 2
MAX_ATTEMPTS = 3
VERIFY_RUNS = 2  # detector runs after a deploy before a non-clearing finding is reverted


@dataclass
class AgentContext:
    fleet: Fleet
    package_root: Path                      # directory that contains the `pipelineagent` package + fleet.yaml + tests
    github: GitHub
    payload_loader: Callable[[str, str], bytes | None]   # (feed_id, content_sha256) -> raw bytes or None
    deployer: Callable[[str], str | None]   # (merge sha) -> build id / ref, or None when not configured
    proposer: Callable[..., PatchProposal | None] = propose
    auto_merge: bool = True
    current_commit: str | None = None       # GIT_COMMIT of the running job
    extra: dict[str, Any] = field(default_factory=dict)


class Agent:
    def __init__(self, wh: Warehouse, ctx: AgentContext, run_id: str, now: datetime):
        self.wh, self.ctx, self.run_id, self.now = wh, ctx, run_id, now
        self.open_now: dict[tuple[str, str], dict] = {}

    # ---- bookkeeping -------------------------------------------------------------------
    def step(self, fid: str, attempt: int, step: str, outcome: str, detail: str = "", ref: str | None = None) -> None:
        self.wh.append("agent_steps", [{"finding_id": fid, "ts": self.now, "run_id": self.run_id, "attempt": attempt, "step": step,
                                        "outcome": outcome, "detail": detail[:2000], "ref": ref}], AGENT_STEPS)
        log.info("agent %s attempt %d %-8s %-7s %s", fid, attempt, step, outcome, detail[:120])

    def disposition(self, fid: str, disp: str, note: str, evidence_ref: str | None = None) -> None:
        dispose(self.wh, fid, disp, "agent", note, self.now, evidence_ref)

    def steps_for(self, fid: str) -> list[dict]:
        return self.wh.agent_steps([fid])

    def _last_human_requeue(self, fid: str):
        """Timestamp of the latest human DEFERRED disposition on this finding, if any."""
        ts = None
        for e in self.wh.ledger_events(1000):
            if e.get("kind") == "DISPOSITION" and e.get("finding_id") == fid and e.get("value") == "DEFERRED" and str(e.get("actor") or "").startswith("human:"):
                if ts is None or e["ts"] > ts:
                    ts = e["ts"]
        return ts

    # ---- entry -------------------------------------------------------------------------
    AXIS_PRIORITY = {"write_recency": 0, "head_lag": 1, "flat_value": 2}

    def run(self, opened: list[dict], deferred: list[dict], open_now: dict[tuple[str, str], dict]) -> None:
        self.open_now = open_now
        self.verify_pending(open_now)
        self.close_recovered_issues(open_now)
        queue = list(opened) + [f for f in deferred if f["finding_id"] not in {o["finding_id"] for o in opened}]
        # The unit of work is a FEED, not a finding: several axes breaching on one feed are one
        # upstream problem. One triage, one patch, one PR per feed; the other axes JOIN the primary
        # (the most informative axis: write_recency, then head_lag, then flat_value) and inherit its
        # outcome. The depth budget counts distinct feeds.
        by_feed: dict[str, list[dict]] = {}
        for f in queue:
            by_feed.setdefault(f["feed"], []).append(f)
        for i, (feed, group) in enumerate(by_feed.items()):
            group.sort(key=lambda g: self.AXIS_PRIORITY.get(g["axis"], 9))
            primary, siblings = group[0], group[1:]
            if i >= DEPTH_BUDGET:
                for f in group:
                    self.step(f["finding_id"], 0, "defer", "ok", f"depth budget {DEPTH_BUDGET} feeds/run exhausted; re-queued next run")
                    self.disposition(f["finding_id"], "DEFERRED", "depth_budget")
                continue
            try:
                self.handle(primary)
            except Exception as e:  # noqa: BLE001 — one finding must not take the loop down
                log.exception("agent failed on %s", primary["finding_id"])
                self.step(primary["finding_id"], 0, "escalate", "failed", f"agent crashed: {e.__class__.__name__}: {str(e)[:300]}")
                self.disposition(primary["finding_id"], "DEFERRED", f"agent error {e.__class__.__name__}; retry next run")
            if siblings:
                outcome = self.wh.latest_dispositions().get(primary["finding_id"]) or {}
                disp = outcome.get("disposition", "DEFERRED")
                for f in siblings:
                    self.step(f["finding_id"], 0, "join", "ok", f"follows sibling finding {primary['finding_id']} (same feed): {disp}", primary["finding_id"])
                    if disp in ("ESCALATED", "BENIGN"):
                        self.disposition(f["finding_id"], disp, f"follows {primary['finding_id']}: {outcome.get('note', '')}"[:900], outcome.get("evidence_ref"))
                    else:
                        self.disposition(f["finding_id"], "DEFERRED", f"follows {primary['finding_id']}")

    # ---- one finding -------------------------------------------------------------------
    def handle(self, f: dict) -> None:
        fid, feed_id = f["finding_id"], f["feed"]
        spec = self.ctx.fleet.feed(feed_id)
        prior_all = self.steps_for(fid)
        # A human re-queue (`dispose ... DEFERRED` by a human) resets the attempt budget and discards
        # stale gate feedback: it means "the conditions changed, try again from scratch".
        reset_at = self._last_human_requeue(fid)
        prior = [s for s in prior_all if reset_at is None or s["ts"] > reset_at]
        attempt = 1 + sum(1 for s in prior if s["step"] == "patch" and s["outcome"] in ("ok", "failed"))
        if attempt > MAX_ATTEMPTS:
            return self.escalate(fid, attempt, f, "max attempts reached", prior)
        runs = self.wh.runs_for_feed(feed_id, 30)  # daily feeds are mostly 'skipped' rows; triage filters those out
        t = triage_mod.classify(f, runs, spec, self.now, first_seen=not prior)
        self.step(fid, attempt, "triage", "ok", f"{t.failure_class} -> {t.action}: {t.rationale}")
        if t.action == "benign":
            return self.disposition(fid, "BENIGN", t.rationale)
        if t.action == "defer":
            return self.disposition(fid, "DEFERRED", t.rationale)
        if t.action == "escalate":
            return self.escalate(fid, attempt, f, t.rationale, prior, t.failure_class)
        # ---- patch path
        failing_sha = t.evidence.get("content_sha256")
        payload = self.ctx.payload_loader(feed_id, failing_sha) if failing_sha else None
        if payload is None:
            self.step(fid, attempt, "replay", "skipped", "no archived payload for the failing run; cannot reproduce")
            return self.escalate(fid, attempt, f, "no archived payload to replay (seamfulness: not guessing)", prior, t.failure_class)
        healthy = [b for b in (self.ctx.payload_loader(feed_id, r["content_sha256"]) for r in runs if r["status"] == "ok" and r.get("content_sha256")) if b][:5]
        prev_codes = [s["detail"] for s in prior if s["step"] == "gate" and s["outcome"] == "failed"][-1:]
        with tempfile.TemporaryDirectory(prefix="pa-agent-") as tmp:
            base = Path(tmp) / "base"
            head = Path(tmp) / "head"
            shutil.copytree(self.ctx.package_root, base, ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache", "site", "*.db"))
            shutil.copytree(base, head)
            module_rel = Path("pipelineagent") / "feeds" / f"{feed_id}.py"
            proposal = self.ctx.proposer(spec=spec, module_source=(base / module_rel).read_text(), error=t.evidence.get("error", ""),
                                          payload_sample=load_payload_bodies(payload)[0][:4000].decode("utf-8", "replace"),
                                          prior_gate_feedback="\n".join(prev_codes), existing_tests=(base / "tests" / "test_feeds_parse.py").read_text())
            if proposal is None:
                self.step(fid, attempt, "patch", "dry_run", "no model credentials or model declined; nothing proposed")
                return self.disposition(fid, "DEFERRED", "patch step needs a model key (dry run)")
            apply_proposal(proposal, head, module_rel)
            self.step(fid, attempt, "patch", "ok", proposal.explanation[:500])
            verdict = gate_check(base, head, feed_id, [payload], healthy, fleet_rel="fleet.yaml", finding_id=fid)
            codes = ",".join(verdict["reason_codes"])
            if verdict["verdict"] != "PASS":
                self.step(fid, attempt, "gate", "failed", f"{codes}: {verdict['evidence'].get('diagnosis','')[:400]}")
                if attempt >= MAX_ATTEMPTS:
                    return self.escalate(fid, attempt, f, f"gate rejected {attempt} attempts ({codes})", prior + self.steps_for(fid), t.failure_class)
                return self.disposition(fid, "DEFERRED", f"attempt {attempt} rejected by gate ({codes}); retry next run")
            self.step(fid, attempt, "replay", "ok", verdict["evidence"].get("diagnosis", "")[:400])
            self.step(fid, attempt, "gate", "ok", codes)
            # ---- PR
            changed = {str(module_rel): (head / module_rel).read_text()}
            test_path = Path("tests") / proposal.test_filename
            changed[str(test_path)] = proposal.test_source
            title = f"agent: repair {feed_id} ({t.failure_class}, finding {fid}, attempt {attempt})"
            body = _pr_body(f, t, proposal, verdict, attempt)
            try:
                pr = self.ctx.github.open_pr(branch=f"agent/{fid}-{attempt}", files=changed, title=title, body=body, verdict=verdict)
            except GitHubError as e:
                self.step(fid, attempt, "pr", "failed", str(e)[:400])
                return self.disposition(fid, "DEFERRED", f"PR could not be opened ({e.__class__.__name__}); retry next run")
            self.step(fid, attempt, "pr", "dry_run" if pr.dry_run else "ok", title, pr.url)
            if pr.dry_run or not self.ctx.auto_merge:
                return self.disposition(fid, "DEFERRED", f"PR {'drafted (dry run)' if pr.dry_run else 'open, auto-merge off'}: {pr.url}")
            try:
                sha = self.ctx.github.merge_pr(pr)
            except GitHubError as e:
                self.step(fid, attempt, "merge", "failed", str(e)[:400], pr.url)
                return self.disposition(fid, "DEFERRED", f"merge failed ({e}); retry next run")
            self.step(fid, attempt, "merge", "ok", f"squash-merged into {self.ctx.github.base_branch}", sha)
            build = self.ctx.deployer(sha)
            self.step(fid, attempt, "deploy", "ok" if build else "dry_run", f"deploy of {sha[:10]} {'triggered' if build else 'not configured'}", build or sha)
            self.disposition(fid, "UNKNOWN", f"attempt {attempt} merged as {sha[:10]}; awaiting verification by the next {VERIFY_RUNS} detector runs", pr.url)

    # ---- verification of earlier merges ------------------------------------------------
    def _deploy_landed(self, merged_sha: str, running_sha: str) -> bool:
        """The running job carries the merge when it IS that commit or any later one on main
        (an unrelated merge+deploy in between must not stall verification forever)."""
        if running_sha.startswith(merged_sha[:10]) or merged_sha.startswith(running_sha[:10]):
            return True
        try:
            return self.ctx.github.contains(merged_sha, running_sha)
        except Exception:  # noqa: BLE001 — GitHubError or a transport failure: unknown is "not yet", never a crash
            return False

    def close_recovered_issues(self, open_now: dict[tuple[str, str], dict]) -> None:
        """A 'needs-human' issue whose finding is no longer open (source recovered, or a later repair
        fixed it) is closed with one comment, so the open-issue list stays the list of what still
        needs a person. One issue may serve a feed's sibling findings; it is closed once."""
        still_open = {f["finding_id"] for f in open_now.values()}
        disps = self.wh.latest_dispositions()
        # one batched read for every finding that has ever been escalated (only those can own an issue)
        candidates = [fid for fid, d in disps.items() if d["disposition"] == "ESCALATED" or "/issues/" in str(d.get("evidence_ref") or "")
                      or fid in still_open]
        by_fid: dict[str, list[dict]] = {}
        for s in self.wh.agent_steps(candidates) if candidates else []:
            by_fid.setdefault(s["finding_id"], []).append(s)

        def issue_of(steps: list[dict]) -> str | None:
            return next((str(s["ref"]) for s in reversed(steps) if s["step"] == "escalate" and s["outcome"] == "ok" and s.get("ref")
                         and "/issues/" in str(s["ref"]) and not str(s["ref"]).startswith("dry-run://")), None)

        # an issue is shared by a feed's sibling findings: while any of them is still open it stays open
        held_open = {issue_of(by_fid.get(fid, [])) for fid in still_open} - {None}
        done_urls: set[str] = set()
        for fid in candidates:
            d = disps.get(fid) or {}
            if fid in still_open:
                continue
            steps = by_fid.get(fid, [])
            issue = issue_of(steps)
            if not issue or issue in done_urls or issue in held_open:
                continue
            if any(s["step"] == "issue_closed" and s.get("ref") == issue and s["outcome"] == "ok" for s in steps):
                continue  # a FAILED close attempt is retried next run
            done_urls.add(issue)
            comment = (f"Finding `{fid}` is no longer open on the live detectors. Current disposition: **{d['disposition']}** — "
                       f"{d.get('note') or ''}" + (f" (evidence: {d['evidence_ref']})" if d.get("evidence_ref") else "") +
                       "\n\nClosed automatically by PipelineAgent; reopen if a person still needs to look.")
            try:
                closed = self.ctx.github.close_issue(issue, comment)
                self.step(fid, 0, "issue_closed", "ok", "closed: finding no longer open" if closed else "already closed", issue)
            except GitHubError as e:
                self.step(fid, 0, "issue_closed", "failed", str(e)[:400], issue)

    def verify_pending(self, open_now: dict[tuple[str, str], dict]) -> None:
        disps = self.wh.latest_dispositions()
        for fid, d in disps.items():
            awaiting = d["decided_by"] == "agent" and "awaiting verification" in (d["note"] or "")
            # a finding the auto-close rule stamped "self-healed" although a repair had been merged and
            # never verified belongs to the agent: reclaim it so the record says FIXED with its evidence
            self_healed = d["decided_by"] == "auto" and d["disposition"] == "BENIGN"
            if not (awaiting or self_healed):
                continue
            steps = self.steps_for(fid)
            merges = [s for s in steps if s["step"] == "merge" and s["outcome"] == "ok"]
            if not merges:
                continue
            last = merges[-1]
            attempt = last["attempt"]
            if self_healed and any(s["step"] == "verify" and s["attempt"] == attempt and s["outcome"] in ("ok", "failed") for s in steps):
                continue
            if self_healed:
                d = dict(d, evidence_ref=next((s["ref"] for s in steps if s["step"] == "pr" and s["attempt"] == attempt and s["outcome"] == "ok"), None))
            deployed_sha = last["ref"]
            runs_since = sum(1 for s in steps if s["step"] == "verify" and s["attempt"] == attempt)
            still_open = any(f["finding_id"] == fid for f in open_now.values())
            if self.ctx.current_commit and deployed_sha and not self._deploy_landed(deployed_sha, self.ctx.current_commit):
                self.step(fid, attempt, "verify", "skipped", f"running commit {self.ctx.current_commit[:10]} is not the merged {deployed_sha[:10]} yet (deploy pending)")
                if runs_since + 1 > VERIFY_RUNS + 2:
                    self.step(fid, attempt, "verify", "failed", "deploy never landed", deployed_sha)
                    self.disposition(fid, "ESCALATED", f"attempt {attempt} merged but the deploy did not land after {runs_since + 1} runs")
                continue
            if not still_open:
                self.step(fid, attempt, "verify", "ok", "finding cleared on the live detectors after deploy", deployed_sha)
                self.disposition(fid, "FIXED", f"attempt {attempt}: merged {deployed_sha[:10]}, deployed, detector cleared", d.get("evidence_ref"))
                for sid, sd in disps.items():
                    if str(sd.get("note") or "").startswith(f"follows {fid}") and not any(g["finding_id"] == sid for g in open_now.values()):
                        self.step(sid, attempt, "verify", "ok", f"cleared with sibling {fid}", deployed_sha)
                        self.disposition(sid, "FIXED", f"fixed by the repair of sibling finding {fid}", d.get("evidence_ref"))
            elif runs_since + 1 >= VERIFY_RUNS:
                self.step(fid, attempt, "verify", "failed", f"still open {runs_since + 1} runs after deploy; reverting", deployed_sha)
                try:
                    rev = self.ctx.github.revert(deployed_sha)
                    self.step(fid, attempt, "revert", "ok", "revert merged", rev)
                    build = self.ctx.deployer(rev)
                    self.step(fid, attempt, "deploy", "ok" if build else "dry_run", "redeploy after revert", build or rev)
                except GitHubError as e:
                    self.step(fid, attempt, "revert", "failed", str(e)[:400])
                if attempt >= MAX_ATTEMPTS:
                    self.disposition(fid, "ESCALATED", f"{attempt} attempts merged and reverted without clearing the finding")
                    self.ctx.github.open_issue(fid, f"PipelineAgent: {fid} not fixed after {attempt} attempts", _issue_body(fid, steps))
                else:
                    self.disposition(fid, "DEFERRED", f"attempt {attempt} reverted; next attempt next run")
            else:
                self.step(fid, attempt, "verify", "skipped", f"still open; run {runs_since + 1}/{VERIFY_RUNS} after deploy", deployed_sha)

    def escalate(self, fid: str, attempt: int, f: dict, reason: str, prior: list[dict], failure_class: str = "unknown") -> None:
        """ESCALATED + exactly one GitHub issue per FEED: three axes of one feed breaching is one
        upstream problem, so sibling findings reuse the issue already opened for the feed."""
        sibling_ids = [g["finding_id"] for g in self.open_now.values() if g["feed"] == f["feed"]] + [fid]
        # a dry-run ref is not an issue anybody can see: once GitHub is live, open the real one
        def counts(ref: str) -> bool:
            return not ref.startswith("dry-run://") or getattr(self.ctx.github, "dry_run", False)

        prior_issue = next((s for s in self.wh.agent_steps(sorted(set(sibling_ids)))
                            if s["step"] == "escalate" and s["outcome"] == "ok" and s.get("ref") and counts(str(s["ref"]))), None)
        if prior_issue:
            if prior_issue["finding_id"] != fid:
                self.step(fid, attempt, "escalate", "ok", f"{reason} (joined the feed's open issue)", prior_issue["ref"])
            self.disposition(fid, "ESCALATED", reason, prior_issue["ref"])
            return
        try:
            url = self.ctx.github.open_issue(fid, f"PipelineAgent: {f['feed']} needs a human ({failure_class}: {f['axis']} {f.get('state','')})",
                                             _issue_body(fid, prior, f=f, reason=reason))
            self.step(fid, attempt, "escalate", "ok", reason, url)
            self.disposition(fid, "ESCALATED", reason, url)
        except GitHubError as e:
            self.step(fid, attempt, "escalate", "failed", f"{reason} (issue not opened: {e})")
            self.disposition(fid, "ESCALATED", reason)


def _pr_body(f: dict, t: triage_mod.Triage, proposal: PatchProposal, verdict: dict, attempt: int) -> str:
    import json
    return (f"## Repair for finding `{f['finding_id']}`\n\n**Feed:** `{f['feed']}` · **axis:** {f['axis']} · **state:** {f.get('state')}\n"
            f"**Triage:** `{t.failure_class}` — {t.rationale}\n\n**Attempt:** {attempt}\n\n### Patch\n{proposal.explanation}\n\n"
            f"### Gate\n`{verdict['verdict']}` {verdict['reason_codes']}\n\n{verdict['evidence'].get('diagnosis','')}\n\n"
            f"<details><summary>verdict.json</summary>\n\n```json\n{json.dumps(verdict, indent=2, default=str)}\n```\n</details>\n\n"
            "Opened by PipelineAgent. Merge is automatic on gate PASS; the next live detector runs verify or revert it.")


def _issue_body(fid: str, steps: list[dict], f: dict | None = None, reason: str = "") -> str:
    lines = [f"PipelineAgent could not resolve finding `{fid}` without a human.", ""]
    if f:
        lines += [f"- feed: `{f['feed']}` axis: {f['axis']} state: {f.get('state')}", f"- evidence: {f.get('detail')}", f"- reason: {reason}", ""]
    lines.append("### What was tried")
    for s in steps:
        lines.append(f"- attempt {s['attempt']} {s['step']} {s['outcome']}: {(s.get('detail') or '')[:200]}" + (f" ({s['ref']})" if s.get('ref') else ""))
    if not steps:
        lines.append("- nothing (escalated at triage)")
    lines += ["", "Resolve with `pipelineagent dispose <id> FIXED|BENIGN|DEFERRED --who <you> --note ...` or by changing the SLA/detector in a human PR."]
    return "\n".join(lines)
