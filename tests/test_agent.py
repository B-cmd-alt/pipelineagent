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
