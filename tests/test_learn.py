"""A finding a person settles against the agent becomes a replayable scenario draft, never a rule change."""
from datetime import datetime, timedelta, timezone

import yaml

from pipelineagent import evals, learn
from pipelineagent.agent import AgentContext
from pipelineagent.agent.github import DryRunGitHub
from pipelineagent.ledger import dispose
from pipelineagent.runner import load_payload, run

T0 = datetime(2026, 11, 9, 3, 0, tzinfo=timezone.utc)


def _live(fleet, wh, tmp_path, monkeypatch):
    """Five live runs of github_status: one healthy, then timeouts; the agent escalates."""
    monkeypatch.setenv("PIPELINEAGENT_PAYLOAD_DIR", str(tmp_path / "payloads"))
    monkeypatch.delenv("PIPELINEAGENT_INJECT", raising=False)
    gh = DryRunGitHub(tmp_path / "site")
    ctx = AgentContext(fleet=fleet, package_root=evals.ROOT, github=gh, payload_loader=load_payload, deployer=lambda s: None, proposer=lambda **k: None)
    http = evals.ScenarioHttp()
    for h in range(6):
        now = T0 + timedelta(hours=h)
        wh.now_override = now
        http.fail = None if h == 0 else "timeout"
        http.body = evals.render_body((evals.ROOT / "evals" / "bodies" / "github_status.json").read_text(), now)
        run(fleet, wh, feeds=["github_status"], http=http, now=now, agent_ctx=ctx)
    fid = wh.open_findings()[0]["finding_id"]
    assert wh.latest_dispositions()[fid]["disposition"] == "ESCALATED"
    return fid, gh


def test_overruled_finding_becomes_a_draft_that_replays_what_happened_live(fleet, wh, tmp_path, monkeypatch):
    fid, gh = _live(fleet, wh, tmp_path, monkeypatch)
    assert learn.candidates(wh) == []  # the agent's own answer stands: nothing to learn yet
    dispose(wh, fid, "BENIGN", "human:isabelle", "planned maintenance window, not an incident", T0 + timedelta(hours=7))
    cands = learn.candidates(wh)
    assert [c["finding_id"] for c in cands] == [fid] and "overruled" in cands[0]["why"]

    assert learn.draft_scenarios(fleet, wh, gh, "r9", T0 + timedelta(hours=8), load_payload) == [fid]
    assert learn.draft_scenarios(fleet, wh, gh, "r10", T0 + timedelta(hours=9), load_payload) == []  # once per finding
    pr_dir = next((tmp_path / "site" / "agent").glob("learn_draft-*"))
    doc = yaml.safe_load(next((pr_dir / "evals" / "drafts").glob("*.yaml")).read_text())
    assert doc["expect"] == {} and doc["live"]["human_disposition"] == "BENIGN" and doc["live"]["agent_disposition"] == "ESCALATED"
    assert doc["steps"][0].get("raw") and all(s.get("fail") == "timeout" for s in doc["steps"][1:])
    assert not (pr_dir / "verdict.json").exists()  # a draft is evidence for a person, not a gated repair

    # the draft replays offline to exactly what the agent did live
    doc["_bodies_root"] = str(pr_dir / "evals" / "bodies")
    obs = evals.observe(doc)
    assert obs["opened"] == ["write_recency"] and obs["triage_first"] == "stale_source -> defer" and obs["disposition"] == {"write_recency": "ESCALATED"}


def test_an_archive_gap_refuses_to_draft(fleet, wh, tmp_path, monkeypatch):
    fid, gh = _live(fleet, wh, tmp_path, monkeypatch)
    dispose(wh, fid, "BENIGN", "human:isabelle", "x", T0 + timedelta(hours=7))
    assert learn.build_draft(fleet, wh, learn.candidates(wh)[0], lambda feed, sha: None) is None
    assert learn.draft_scenarios(fleet, wh, gh, "r9", T0 + timedelta(hours=8), lambda feed, sha: None) == []
    assert [s["outcome"] for s in wh.agent_steps([fid]) if s["step"] == "scenario_draft"] == ["skipped"]


def test_a_human_requeue_is_not_a_lesson(fleet, wh, tmp_path, monkeypatch):
    fid, gh = _live(fleet, wh, tmp_path, monkeypatch)
    dispose(wh, fid, "DEFERRED", "human:isabelle", "try again", T0 + timedelta(hours=7))
    assert learn.candidates(wh) == []


def test_drafts_in_the_repo_still_replay():
    for d in evals.all_drafts():
        assert d.get("origin") and "live" in d, d["id"]
        evals.observe(d)  # must not crash; it has no answer yet, so nothing is asserted about the outcome
