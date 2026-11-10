"""The repair agent, end to end on SQLite with a dry-run GitHub and stubbed patchers.
The replay and the gate run for real (subprocesses) against copies of this tree."""
import shutil
from datetime import timedelta
from pathlib import Path

import pytest
from conftest import NOW, ROOT, fixture_text

from pipelineagent.agent import Agent, AgentContext, DEPTH_BUDGET, MAX_ATTEMPTS

# The gate re-runs the suite inside a patched copy of this tree; these tests spawn the gate
# themselves, so they are excluded there (-m "not agent_e2e") to avoid recursion.
pytestmark = pytest.mark.agent_e2e
from pipelineagent.agent.gate import check, lint, sla_widened
from pipelineagent.agent.github import DryRunGitHub, GitHubError
from pipelineagent.agent.patch import PatchProposal
from pipelineagent.agent.replay import load_payload_bodies, replay
from pipelineagent.agent.triage import classify
from pipelineagent.ledger import apply as apply_ledger, reconcile
from pipelineagent.detectors import Detection

FRED_OK = fixture_text("fredgraph_tail.csv")
FRED_RENAMED = FRED_OK.replace("observation_date,DGS10", "DATE,DGS10", 1)  # upstream renamed the date column


def run_row(status, reason=None, error=None, sha=None, hours_ago=0):
    return {"run_id": "r", "feed": "fred_dgs10", "status": status, "reason": reason, "error": error, "content_sha256": sha,
            "started_at": NOW - timedelta(hours=hours_ago), "finished_at": NOW - timedelta(hours=hours_ago)}


# ---------------------------------------------------------------- triage
def test_triage_rules(fleet):
    spec = fleet.feed("fred_dgs10")
    f = {"finding_id": "f1", "feed": "fred_dgs10", "axis": "write_recency", "state": "BREACH", "detail": ""}
    t = classify(f, [run_row("failed", "exception:ValueError", "schema: expected columns observation_date,DGS10; got ['DATE','DGS10']", "abc")], spec, NOW)
    assert (t.failure_class, t.action, t.evidence["content_sha256"]) == ("schema_drift", "patch", "abc")
    t = classify(f, [run_row("failed", "source_error", "transport: ReadTimeout")], spec, NOW, first_seen=True)
    assert (t.failure_class, t.action) == ("stale_source", "defer")
    t = classify(f, [run_row("failed", "source_error", "transport: ReadTimeout")], spec, NOW, first_seen=False)
    assert (t.failure_class, t.action) == ("stale_source", "escalate")
    t = classify(f, [run_row("ok"), run_row("no_change")], spec, NOW)
    assert (t.failure_class, t.action) == ("green_job_dead_data", "escalate")
    t = classify({**f, "axis": "flat_value"}, [run_row("ok")], spec, NOW)
    assert (t.failure_class, t.action) == ("flat_value", "escalate")
    t = classify({**f, "state": "UNKNOWN", "axis": "head_lag"}, [], spec, NOW)
    assert t.action == "escalate" and t.failure_class == "unknown"


# ---------------------------------------------------------------- replay
def test_replay_reproduces_schema_drift_and_clean_payload_loads():
    bad = replay(ROOT, "fred_dgs10", FRED_RENAMED.encode())
    assert not bad.fetch_ok and "schema" in (bad.error or "")
    good = replay(ROOT, "fred_dgs10", FRED_OK.encode())
    assert good.fetch_ok and good.rows > 0 and good.loads_cleanly and good.rows_digest
    assert load_payload_bodies(b'["a","b"]') == [b"a", b"b"] and load_payload_bodies(b"x,y") == [b"x,y"]


def test_replay_blocks_network():
    # a feed that goes to the network gets no body back and fails loudly instead of fetching
    r = replay(ROOT, "github_status", b"{}")
    assert r.fetch_ok or "replay" in (r.error or "")  # the stub answers once with {}; a second request would raise


# ---------------------------------------------------------------- gate
def _copy_tree(dst: Path) -> Path:
    shutil.copytree(ROOT, dst, ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache", "site", "*.db", "payloads"))
    return dst


def good_patch(head: Path) -> None:
    m = head / "pipelineagent" / "feeds" / "fred_dgs10.py"
    src = m.read_text()
    src = src.replace('''    if not reader.fieldnames or "observation_date" not in reader.fieldnames or series_id not in reader.fieldnames:
        raise ValueError(f"schema: expected columns observation_date,{series_id}; got {reader.fieldnames}")
    recs = list(reader)[-keep_last_n:]
    rows = []
    for rec in recs:
        d = date.fromisoformat(rec["observation_date"])''', '''    date_col = next((c for c in ("observation_date", "DATE") if reader.fieldnames and c in reader.fieldnames), None)
    if date_col is None or series_id not in (reader.fieldnames or []):
        raise ValueError(f"schema: expected columns observation_date,{series_id}; got {reader.fieldnames}")
    if date_col != "observation_date":
        anomalies.append(f"date column renamed upstream: {date_col}")
    recs = list(reader)[-keep_last_n:]
    rows = []
    for rec in recs:
        d = date.fromisoformat(rec[date_col])''')
    m.write_text(src)
    (head / "tests" / "test_agent_fix_fred.py").write_text(
        "from pipelineagent.feeds import fred_dgs10\n\ndef test_renamed_date_column_parses():\n"
        "    rows, anomalies = fred_dgs10.parse('DATE,DGS10\\n2026-11-05,4.1\\n2026-11-06,4.2\\n', 'DGS10', 5)\n"
        "    assert len(rows) == 2 and any('renamed' in a for a in anomalies)\n")


def test_lint_rules(tmp_path):
    base = _copy_tree(tmp_path / "base"); head = _copy_tree(tmp_path / "head")
    assert lint(base, head, "fred_dgs10")[0] == []
    good_patch(head)
    assert lint(base, head, "fred_dgs10")[0] == []
    (head / "pipelineagent" / "detectors.py").write_text("# gutted\n")
    (head / "tests" / "test_detectors.py").unlink()
    (head / "fleet.yaml").write_text((head / "fleet.yaml").read_text().replace("head_lag_max_hours: 120", "head_lag_max_hours: 999"))
    (head / "pipelineagent" / "feeds" / "fred_dgs10.py").write_text((head / "pipelineagent" / "feeds" / "fred_dgs10.py").read_text() + '\nAPI_KEY = "sk-ant-abcdefghijklmnop"\n')
    codes, notes = lint(base, head, "fred_dgs10")
    assert {"DETECTOR_DISABLED", "TEST_WEAKENED", "SLA_WIDENED", "POLICY_FILE_TOUCHED", "SCOPE_EXCEEDED", "SECRET_IN_DIFF"} <= set(codes)
    assert notes["sla_widened"] == ["fred_dgs10/head_lag_max_hours 120.0->999.0"] or notes["sla_widened"] == ["fred_dgs10/head_lag_max_hours 120->999"]


def test_gate_passes_a_real_fix_and_fails_a_non_fix(tmp_path):
    base = _copy_tree(tmp_path / "base"); head = _copy_tree(tmp_path / "head")
    good_patch(head)
    v = check(base, head, "fred_dgs10", [FRED_RENAMED.encode()], [FRED_OK.encode()], run_tests_too=False)
    assert v["verdict"] == "PASS" and v["reason_codes"] == ["REPLAY_PASSED"], v["evidence"]["diagnosis"]
    # a "fix" that does not touch the failure: replay cannot clear -> FAIL, never PASS
    head2 = _copy_tree(tmp_path / "head2")
    (head2 / "tests" / "test_agent_noop.py").write_text("def test_noop():\n    assert True\n")
    v2 = check(base, head2, "fred_dgs10", [FRED_RENAMED.encode()], [], run_tests_too=False)
    assert v2["verdict"] == "FAIL" and "REPLAY_FAILED" in v2["reason_codes"]
    # a payload the base already loads is not a reproducible breach -> FAIL
    v3 = check(base, head, "fred_dgs10", [FRED_OK.encode()], [], run_tests_too=False)
    assert v3["verdict"] == "FAIL" and "REPLAY_FAILED" in v3["reason_codes"]


# ---------------------------------------------------------------- orchestrator (dry run)
def _ctx(fleet, tmp_path, proposer, payloads: dict, auto_merge=True, gh=None):
    return AgentContext(fleet=fleet, package_root=ROOT, github=gh or DryRunGitHub(tmp_path / "site"),
                        payload_loader=lambda feed, sha: payloads.get(sha), deployer=lambda sha: None, proposer=proposer, auto_merge=auto_merge)


def _open_finding(wh, feed, axis, state="BREACH", detail="d"):
    delta = reconcile([Detection(feed, axis, state, 1.0, 2.0, detail)], wh.open_findings(), wh.latest_dispositions(), "r0", NOW)
    apply_ledger(delta, wh)
    return delta.opened[0]


def test_depth_budget_defers_the_rest(fleet, wh, tmp_path):
    opened = [_open_finding(wh, f, "write_recency") for f in ("nws_observations", "open_meteo_current", "usgs_earthquakes_hour")]
    Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}), "r1", NOW).run(opened, [], {})
    disps = wh.latest_dispositions()
    assert disps[opened[2]["finding_id"]]["note"] == "depth_budget" and disps[opened[2]["finding_id"]]["disposition"] == "DEFERRED"
    steps = wh.agent_steps([o["finding_id"] for o in opened])
    assert sum(1 for s in steps if s["step"] == "triage") == DEPTH_BUDGET


def test_escalation_opens_exactly_one_issue(fleet, wh, tmp_path):
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    wh.append("_runs", [run_row("failed", "source_error", "transport: ReadTimeout x")], __import__("pipelineagent.schema", fromlist=["RUNS"]).RUNS)
    ctx = _ctx(fleet, tmp_path, lambda **k: None, {})
    Agent(wh, ctx, "r1", NOW).run([f], [], {})
    assert wh.latest_dispositions()[f["finding_id"]]["disposition"] == "DEFERRED"  # first sight of a stale source waits one run
    Agent(wh, ctx, "r2", NOW + timedelta(hours=1)).run([], [f], {})
    d = wh.latest_dispositions()[f["finding_id"]]
    assert d["disposition"] == "ESCALATED"
    issues = list((tmp_path / "site" / "agent" / "issues").glob("*.md"))
    assert len(issues) == 1 and "needs a human" in issues[0].read_text()
    Agent(wh, ctx, "r3", NOW + timedelta(hours=2)).run([], [f], {})  # escalated findings are not DEFERRED, so they are not re-queued; still exactly one issue
    assert len(list((tmp_path / "site" / "agent" / "issues").glob("*.md"))) == 1


def test_schema_drift_dry_run_without_model(fleet, wh, tmp_path):
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    from pipelineagent.schema import RUNS
    wh.append("_runs", [run_row("failed", "exception:ValueError", "schema: expected columns observation_date,DGS10; got ['DATE', 'DGS10']", "sha-bad")], RUNS)
    ctx = _ctx(fleet, tmp_path, lambda **k: None, {"sha-bad": FRED_RENAMED.encode()})
    Agent(wh, ctx, "r1", NOW).run([f], [], {})
    steps = {s["step"]: s for s in wh.agent_steps([f["finding_id"]])}
    assert steps["triage"]["detail"].startswith("schema_drift -> patch") and steps["patch"]["outcome"] == "dry_run"
    assert wh.latest_dispositions()[f["finding_id"]]["disposition"] == "DEFERRED"


def test_schema_drift_with_a_good_patch_reaches_pr_and_gate_pass(fleet, wh, tmp_path):
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    from pipelineagent.schema import RUNS
    wh.append("_runs", [run_row("failed", "exception:ValueError", "schema: expected columns observation_date,DGS10; got ['DATE', 'DGS10']", "sha-bad"),
                        run_row("ok", sha="sha-ok", hours_ago=24)], RUNS)
    tmp = _copy_tree(tmp_path / "proposal_src"); good_patch(tmp)
    proposal = PatchProposal((tmp / "pipelineagent/feeds/fred_dgs10.py").read_text(), "test_agent_fix_fred.py", (tmp / "tests/test_agent_fix_fred.py").read_text(), "accept DATE as the date column")
    ctx = _ctx(fleet, tmp_path, lambda **k: proposal, {"sha-bad": FRED_RENAMED.encode(), "sha-ok": FRED_OK.encode()})
    Agent(wh, ctx, "r1", NOW).run([f], [], {})
    steps = [(s["step"], s["outcome"]) for s in wh.agent_steps([f["finding_id"]])]
    assert ("gate", "ok") in steps and ("pr", "dry_run") in steps, steps
    assert wh.latest_dispositions()[f["finding_id"]]["disposition"] == "DEFERRED"  # dry run: PR drafted, nothing merged
    pr_dirs = list((tmp_path / "site" / "agent").glob("agent_*"))
    assert pr_dirs and (pr_dirs[0] / "verdict.json").exists() and "PASS" in (pr_dirs[0] / "verdict.json").read_text()


def test_three_rejected_attempts_escalate(fleet, wh, tmp_path):
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    from pipelineagent.schema import RUNS
    wh.append("_runs", [run_row("failed", "exception:ValueError", "schema: expected columns observation_date,DGS10; got ['DATE', 'DGS10']", "sha-bad")], RUNS)
    bad = PatchProposal((ROOT / "pipelineagent/feeds/fred_dgs10.py").read_text() + "\n# no-op\n", "test_agent_noop.py", "def test_noop():\n    assert True\n", "does nothing")
    ctx = _ctx(fleet, tmp_path, lambda **k: bad, {"sha-bad": FRED_RENAMED.encode()})
    for i in range(MAX_ATTEMPTS):
        Agent(wh, ctx, f"r{i}", NOW + timedelta(hours=i)).run([f] if i == 0 else [], [] if i == 0 else [f], {})
    d = wh.latest_dispositions()[f["finding_id"]]
    assert d["disposition"] == "ESCALATED" and "gate rejected" in d["note"]
    gates = [s for s in wh.agent_steps([f["finding_id"]]) if s["step"] == "gate"]
    assert len(gates) == MAX_ATTEMPTS and all(g["outcome"] == "failed" and "REPLAY_FAILED" in g["detail"] for g in gates)


def test_one_issue_per_feed_when_several_axes_escalate(fleet, wh, tmp_path):
    f1 = _open_finding(wh, "usgs_earthquakes_hour", "write_recency")
    f2 = _open_finding(wh, "usgs_earthquakes_hour", "head_lag")
    from pipelineagent.schema import RUNS
    wh.append("_runs", [{**run_row("ok"), "feed": "usgs_earthquakes_hour"}], RUNS)  # green job, no rows -> escalate on both axes
    open_now = {(f["feed"], f["axis"]): f for f in wh.open_findings()}
    Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}), "r1", NOW).run([f1, f2], [], open_now)
    issues = list((tmp_path / "site" / "agent" / "issues").glob("*.md"))
    assert len(issues) == 1
    d = wh.latest_dispositions()
    assert d[f1["finding_id"]]["disposition"] == d[f2["finding_id"]]["disposition"] == "ESCALATED"
    assert d[f1["finding_id"]]["evidence_ref"] == d[f2["finding_id"]]["evidence_ref"]  # both point at the same issue


def test_dry_run_escalation_does_not_count_as_an_open_issue(fleet, wh, tmp_path):
    """Once the GitHub client is live, a feed escalated earlier in dry-run mode must get a real issue."""
    from pipelineagent.schema import AGENT_STEPS, RUNS
    from pipelineagent.agent.github import GitHub, PullRequest

    f = _open_finding(wh, "fred_dgs10", "write_recency")
    wh.append("agent_steps", [{"finding_id": f["finding_id"], "ts": NOW - timedelta(hours=2), "run_id": "r0", "attempt": 1, "step": "escalate",
                               "outcome": "ok", "detail": "earlier, dry", "ref": "dry-run:///app/site/agent/issues/x.md"}], AGENT_STEPS)
    wh.append("_runs", [run_row("failed", "source_error", "transport: ReadTimeout")], RUNS)

    class LiveStub(GitHub):
        def __init__(self):
            self.base_branch, self.opened = "main", []

        def open_issue(self, finding_id, title, body):
            self.opened.append(title)
            return "https://github.com/example/repo/issues/7"

    gh = LiveStub()
    Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}, gh=gh), "r1", NOW).run([], [f], {(f["feed"], f["axis"]): f})
    assert gh.opened and wh.latest_dispositions()[f["finding_id"]]["evidence_ref"] == "https://github.com/example/repo/issues/7"


def test_one_repair_per_feed_siblings_join(fleet, wh, tmp_path):
    f1 = _open_finding(wh, "fred_dgs10", "write_recency")
    f2 = _open_finding(wh, "fred_dgs10", "head_lag")
    from pipelineagent.schema import RUNS
    wh.append("_runs", [run_row("failed", "exception:ValueError", "schema: expected columns observation_date,DGS10; got ['DATE', 'DGS10']", "sha-bad")], RUNS)
    calls = []

    def proposer(**k):
        calls.append(1)
        return None  # dry patch; we only count how often the model would be called

    Agent(wh, _ctx(fleet, tmp_path, proposer, {"sha-bad": FRED_RENAMED.encode()}), "r1", NOW).run([f1, f2], [], {})
    assert len(calls) == 1
    steps = {(s["finding_id"], s["step"]) for s in wh.agent_steps([f1["finding_id"], f2["finding_id"]])}
    assert (f2["finding_id"], "join") in steps and (f2["finding_id"], "triage") not in steps
    assert wh.latest_dispositions()[f2["finding_id"]]["note"] == f"follows {f1['finding_id']}"


def test_human_requeue_resets_the_attempt_budget(fleet, wh, tmp_path):
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    from pipelineagent.schema import RUNS
    from pipelineagent.ledger import dispose
    wh.append("_runs", [run_row("failed", "exception:ValueError", "schema: expected columns observation_date,DGS10; got ['DATE', 'DGS10']", "sha-bad")], RUNS)
    bad = PatchProposal((ROOT / "pipelineagent/feeds/fred_dgs10.py").read_text() + "\n# no-op\n", "test_agent_noop.py", "def test_noop():\n    assert True\n", "does nothing")
    ctx = _ctx(fleet, tmp_path, lambda **k: bad, {"sha-bad": FRED_RENAMED.encode()})
    for i in range(MAX_ATTEMPTS):
        Agent(wh, ctx, f"r{i}", NOW + timedelta(hours=i)).run([f] if i == 0 else [], [] if i == 0 else [f], {})
    assert wh.latest_dispositions()[f["finding_id"]]["disposition"] == "ESCALATED"
    # a human re-queues it: the budget starts over instead of escalating at once
    dispose(wh, f["finding_id"], "DEFERRED", "human:alice", "tests fixed, try again", NOW + timedelta(hours=MAX_ATTEMPTS))
    Agent(wh, ctx, "r-again", NOW + timedelta(hours=MAX_ATTEMPTS + 1)).run([], [f], {})
    steps = wh.agent_steps([f["finding_id"]])
    assert [s["attempt"] for s in steps if s["step"] == "patch"][-1] == 1
    assert wh.latest_dispositions()[f["finding_id"]]["disposition"] == "DEFERRED"


def test_verification_survives_an_unrelated_deploy_in_between(fleet, wh, tmp_path):
    """The merge lands, then someone else merges+deploys: the running commit is a DESCENDANT of the
    repair, not the repair itself. Verification must proceed (FIXED when the finding has cleared),
    not stall on 'deploy pending' until it escalates."""
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    fid = f["finding_id"]
    from pipelineagent.agent.github import GitHub

    class Stub(GitHub):
        def __init__(self, ahead):
            self.base_branch, self.ahead = "main", ahead

        def contains(self, ancestor, sha):
            return self.ahead

    def agent(ahead):
        ctx = _ctx(fleet, tmp_path, lambda **k: None, {}, gh=Stub(ahead))
        ctx.current_commit = "b" * 40
        a = Agent(wh, ctx, "r1", NOW)
        a.step(fid, 1, "merge", "ok", "squash-merged into main", "a" * 40)
        a.disposition(fid, "UNKNOWN", "attempt 1 merged; awaiting verification", "https://github.com/example/repo/pull/1")
        return a

    agent(False).verify_pending({})   # the running commit is unrelated -> still pending
    assert [s["outcome"] for s in wh.agent_steps([fid]) if s["step"] == "verify"] == ["skipped"]
    agent(True).verify_pending({})    # descendant of the merge, finding cleared -> FIXED
    assert wh.latest_dispositions()[fid]["disposition"] == "FIXED"


def test_agent_reclaims_a_repair_the_auto_close_stamped_self_healed(fleet, wh, tmp_path):
    """Live 2026-11-09: the repair merged, the next run landed rows, the ledger's auto rule wrote
    BENIGN 'self-healed' over the agent's 'awaiting verification' and FIXED was never recorded.
    The agent must reclaim such a finding: a merge step with no verify step is its own work."""
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    fid = f["finding_id"]
    from pipelineagent.ledger import dispose
    ctx = _ctx(fleet, tmp_path, lambda **k: None, {})
    ctx.current_commit = "a" * 40
    a = Agent(wh, ctx, "r1", NOW)
    a.step(fid, 1, "pr", "ok", "agent: repair", "https://github.com/example/repo/pull/10")
    a.step(fid, 1, "merge", "ok", "squash-merged into main", "a" * 40)
    a.disposition(fid, "UNKNOWN", "attempt 1 merged; awaiting verification", "https://github.com/example/repo/pull/10")
    dispose(wh, fid, "BENIGN", "auto", "self-healed without intervention", NOW + timedelta(minutes=1))
    later = Agent(wh, ctx, "r2", NOW + timedelta(hours=1))  # the next run
    later.verify_pending({})
    d = wh.latest_dispositions()[fid]
    assert d["disposition"] == "FIXED" and d["evidence_ref"] == "https://github.com/example/repo/pull/10"
    later.verify_pending({})  # idempotent: a verified attempt is not reclaimed twice
    assert sum(1 for s in wh.agent_steps([fid]) if s["step"] == "verify") == 1


def test_needs_human_issue_is_closed_once_when_the_finding_is_no_longer_open(fleet, wh, tmp_path):
    f = _open_finding(wh, "fred_dgs10", "write_recency")
    fid = f["finding_id"]
    from pipelineagent.agent.github import GitHub

    class Stub(GitHub):
        def __init__(self):
            self.base_branch, self.closed = "main", []

        def close_issue(self, url, comment):
            self.closed.append((url, comment))
            return True

    gh = Stub()
    a = Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}, gh=gh), "r1", NOW)
    a.step(fid, 1, "escalate", "ok", "upstream keeps failing", "https://github.com/example/repo/issues/3")
    a.disposition(fid, "ESCALATED", "upstream keeps failing", "https://github.com/example/repo/issues/3")
    a.close_recovered_issues({(f["feed"], f["axis"]): f})   # still open -> untouched
    assert gh.closed == []
    later = Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}, gh=gh), "r2", NOW + timedelta(hours=1))
    later.close_recovered_issues({})                        # recovered -> closed with one comment
    later.close_recovered_issues({})                        # idempotent
    assert len(gh.closed) == 1 and "ESCALATED" in gh.closed[0][1]
    assert [s["outcome"] for s in wh.agent_steps([fid]) if s["step"] == "issue_closed"] == ["ok"]


def test_shared_issue_stays_open_while_a_sibling_finding_is_open_and_a_failed_close_is_retried(fleet, wh, tmp_path):
    f1 = _open_finding(wh, "fred_dgs10", "write_recency")
    f2 = _open_finding(wh, "fred_dgs10", "head_lag")
    from pipelineagent.agent.github import GitHub, GitHubError

    class Stub(GitHub):
        def __init__(self):
            self.base_branch, self.closed, self.fail = "main", [], True

        def close_issue(self, url, comment):
            if self.fail:
                raise GitHubError("502")
            self.closed.append(url)
            return True

    gh = Stub()
    url = "https://github.com/example/repo/issues/3"
    a = Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}, gh=gh), "r1", NOW)
    for f in (f1, f2):
        a.step(f["finding_id"], 1, "escalate", "ok", "upstream", url)
        a.disposition(f["finding_id"], "ESCALATED", "upstream", url)
    # f1 recovered, f2 (same feed, same issue) still open -> the issue is held open
    a.close_recovered_issues({(f2["feed"], f2["axis"]): f2})
    assert gh.closed == [] and not any(s["step"] == "issue_closed" for s in wh.agent_steps([f1["finding_id"]]))
    # both recovered, GitHub fails once -> recorded failed, retried next run
    later = Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}, gh=gh), "r2", NOW + timedelta(hours=1))
    later.close_recovered_issues({})
    assert gh.closed == [] and [s["outcome"] for s in wh.agent_steps([f1["finding_id"], f2["finding_id"]]) if s["step"] == "issue_closed"] == ["failed"]
    gh.fail = False
    Agent(wh, _ctx(fleet, tmp_path, lambda **k: None, {}, gh=gh), "r3", NOW + timedelta(hours=2)).close_recovered_issues({})
    assert gh.closed == [url]


def test_merge_deletes_the_repair_branch_and_a_failed_delete_is_not_fatal():
    from pipelineagent.agent.github import GitHub, GitHubError, PullRequest

    class Rec(GitHub):
        def __init__(self, fail_delete=False):
            self.calls, self.fail_delete, self.base_branch = [], fail_delete, "main"

        def _req(self, method, path, **kw):
            self.calls.append((method, path))
            if method == "DELETE" and self.fail_delete:
                raise GitHubError("DELETE: 422 Reference does not exist")
            return {"sha": "m" * 40}

    pr = PullRequest(10, "https://example/pull/10", "h" * 40, "agent/abc-1")
    gh = Rec()
    assert gh.merge_pr(pr) == "m" * 40
    assert gh.calls == [("PUT", "/pulls/10/merge"), ("DELETE", "/git/refs/heads/agent/abc-1")]
    gh2 = Rec(fail_delete=True)
    assert gh2.merge_pr(pr) == "m" * 40  # the merge stands even if the branch stays
