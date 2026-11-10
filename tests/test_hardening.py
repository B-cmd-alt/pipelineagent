"""Model fallback and cost, the per-feed repair budget, the agent-stage lease."""
from datetime import datetime, timedelta, timezone

from conftest import NOW

from pipelineagent import benchmark, harness, lease
from pipelineagent.agent import Agent, AgentContext
from pipelineagent.agent import MAX_PATCHES_PER_FEED_WEEK
from pipelineagent.agent import patch as patch_mod
from pipelineagent.agent.github import DryRunGitHub
from pipelineagent.detectors import Detection
from pipelineagent.ledger import apply, reconcile
from pipelineagent.schema import AGENT_STEPS, RUNS


def test_default_models_include_a_fallback(monkeypatch):
    monkeypatch.delenv("PIPELINEAGENT_MODEL", raising=False)
    assert patch_mod.models() == ["deepseek/deepseek-v4-pro", "z-ai/glm-5.3"]
    monkeypatch.setenv("PIPELINEAGENT_MODEL", "a/b")
    assert patch_mod.models() == ["a/b"]


def test_cost_is_summed_over_round_trips_and_recorded(monkeypatch, fleet):
    import json
    original = (patch_mod.Path(__file__).resolve().parent.parent / "pipelineagent" / "feeds" / "fred_dgs10.py").read_text()
    answers = [{"module_source": "not python (", "test_filename": "test_x.py", "test_source": "x = 1", "explanation": "bad"},
               {"module_source": original, "test_filename": "test_x.py", "test_source": "x = 1", "explanation": "good"}]
    sent = []

    class R:
        def __init__(self, d, cost):
            self.status_code, self._d, self.text = 200, {"model": "z-ai/glm-5.3", "usage": {"cost": cost},
                                                          "choices": [{"finish_reason": "stop", "message": {"content": json.dumps(d)}}]}, "x"

        def json(self):
            return self._d

    def post(url, json=None, headers=None, timeout=None):
        sent.append(json)
        return R(answers[len(sent) - 1], 0.0123 if len(sent) == 1 else 0.004)

    monkeypatch.setattr(patch_mod.requests, "post", post)
    monkeypatch.setenv("OPENROUTER_API_KEY", "k")
    p = patch_mod.propose(fleet.feed("fred_dgs10"), original, "schema", "DATE,DGS10")
    assert p.explanation == "good" and p.model == "z-ai/glm-5.3" and abs(p.cost_usd - 0.0163) < 1e-9
    assert sent[0]["usage"] == {"include": True} and sent[0]["models"] == ["deepseek/deepseek-v4-pro", "z-ai/glm-5.3"]


def test_benchmark_sums_recorded_patch_cost(wh):
    harness.record(wh, {"fred_dgs10": harness.Injection("fred_dgs10", "schema_rename")}, "r", NOW)
    d = reconcile([Detection("fred_dgs10", "write_recency", "BREACH", 81.0, 80.0, "s")], [], {}, "r1", NOW + timedelta(hours=3))
    apply(d, wh)
    fid = d.opened[0]["finding_id"]
    wh.append("agent_steps", [{"finding_id": fid, "ts": NOW + timedelta(hours=3, minutes=i), "run_id": "r", "attempt": i + 1, "step": "patch", "outcome": "ok",
                               "detail": f"x [model m, cost ${c}]", "ref": None} for i, c in enumerate(("0.0100", "0.0025"))], AGENT_STEPS)
    res = benchmark.run(wh, now=NOW + timedelta(days=5))
    assert res["episodes"][0].model_cost_usd == 0.0125 and res["totals"]["model_cost_usd"] == 0.0125


def test_a_feed_over_its_weekly_repair_budget_escalates_without_calling_the_model(fleet, wh, tmp_path):
    calls = []
    earlier = reconcile([Detection("fred_dgs10", "head_lag", "BREACH", 121.0, 120.0, "s")], [], {}, "r0", NOW - timedelta(days=2))
    apply(earlier, wh)  # an earlier finding on the same feed used up the week's budget
    d = reconcile([Detection("fred_dgs10", "write_recency", "BREACH", 81.0, 80.0, "s")], wh.open_findings(), wh.latest_dispositions(), "r1", NOW)
    apply(d, wh)
    old = d.opened[0]["finding_id"]
    wh.append("agent_steps", [{"finding_id": earlier.opened[0]["finding_id"], "ts": NOW - timedelta(days=1), "run_id": "r", "attempt": 1, "step": "patch", "outcome": "ok", "detail": "x", "ref": None}
                              for _ in range(MAX_PATCHES_PER_FEED_WEEK)], AGENT_STEPS)
    wh.append("_runs", [{"run_id": "r", "feed": "fred_dgs10", "started_at": NOW, "finished_at": NOW, "status": "failed", "reason": "exception:ValueError",
                         "rows_written": 0, "duration_s": 1.0, "http_status": 200, "requests_used": 1, "source_modified": None,
                         "content_sha256": "sha-bad", "error": "schema: expected columns observation_date,DGS10; got ['DATE', 'DGS10']", "code_sha256": "c"}], RUNS)
    ctx = AgentContext(fleet=fleet, package_root=tmp_path, github=DryRunGitHub(tmp_path / "site"), payload_loader=lambda f, s: b"DATE,DGS10\n",
                       deployer=lambda s: None, proposer=lambda **k: calls.append(1))
    Agent(wh, ctx, "r2", NOW).run([d.opened[0]], [], {})
    assert calls == [] and wh.latest_dispositions()[old]["disposition"] == "ESCALATED"
    assert "repair budget" in wh.latest_dispositions()[old]["note"]


def test_lease_admits_one_holder_and_takes_over_a_stale_one(tmp_path, monkeypatch):
    monkeypatch.delenv("PIPELINEAGENT_PAYLOAD_BUCKET", raising=False)
    monkeypatch.delenv("PIPELINEAGENT_SITE_BUCKET", raising=False)
    monkeypatch.setenv("PIPELINEAGENT_PAYLOAD_DIR", str(tmp_path))
    now = datetime(2026, 11, 11, 3, tzinfo=timezone.utc)
    with lease.agent_stage("a", now) as a:
        with lease.agent_stage("b", now + timedelta(minutes=5)) as b:
            assert a is True and b is False
        with lease.agent_stage("c", now + lease.STALE + timedelta(minutes=1)) as c:
            assert c is True  # a's lease is older than STALE: a crashed run's, taken over
    with lease.agent_stage("d", now + timedelta(hours=3)) as d:
        assert d is True  # released on exit
