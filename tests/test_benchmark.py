from datetime import timedelta

from conftest import NOW

from pipelineagent import benchmark, harness
from pipelineagent.detectors import Detection
from pipelineagent.ledger import apply, dispose, reconcile
from pipelineagent.schema import AGENT_STEPS


def _inject(wh, feed, kind, at):
    harness.record(wh, {feed: harness.Injection(feed, kind)}, "r", at)


def test_episodes_split_on_gaps_and_score_against_expected_outcome(wh):
    for h in range(4):
        _inject(wh, "coingecko_prices", "schema_rename", NOW + timedelta(hours=h))
    _inject(wh, "coingecko_prices", "schema_rename", NOW + timedelta(hours=30))   # a second episode after a long gap
    _inject(wh, "github_status", "stale_source", NOW)
    eps = benchmark.episodes(wh.injections())
    assert [(e.feed, e.kind) for e in eps] == [("coingecko_prices", "schema_rename"), ("github_status", "stale_source"), ("coingecko_prices", "schema_rename")]

    # episode 1: detected after 3h, fixed 1h later with one gate rejection -> CORRECT
    d = reconcile([Detection("coingecko_prices", "write_recency", "BREACH", 4.0, 3.0, "stale")], [], {}, "r1", NOW + timedelta(hours=3))
    apply(d, wh)
    fid = d.opened[0]["finding_id"]
    wh.append("agent_steps", [{"finding_id": fid, "ts": NOW + timedelta(hours=3, minutes=10), "run_id": "r1", "attempt": 1, "step": "gate", "outcome": "failed", "detail": "TESTS_FAILED", "ref": None},
                              {"finding_id": fid, "ts": NOW + timedelta(hours=3, minutes=40), "run_id": "r2", "attempt": 2, "step": "pr", "outcome": "ok", "detail": "pr", "ref": "https://example/pull/10"}], AGENT_STEPS)
    dispose(wh, fid, "ESCALATED", "agent", "gate rejected 3 attempts", NOW + timedelta(hours=3, minutes=20), "https://example/issues/6")
    dispose(wh, fid, "DEFERRED", "human:isabelle", "tests fixed, try again", NOW + timedelta(hours=3, minutes=30))
    dispose(wh, fid, "FIXED", "agent", "merged, deployed, cleared", NOW + timedelta(hours=4), "https://example/pull/10")
    # stale_source episode: detected, escalated -> CORRECT (expected ESCALATED)
    d2 = reconcile([Detection("github_status", "write_recency", "BREACH", 4.0, 3.0, "stale")], [], {}, "r3", NOW + timedelta(hours=3))
    apply(d2, wh)
    dispose(wh, d2.opened[0]["finding_id"], "ESCALATED", "agent", "upstream", NOW + timedelta(hours=4), "https://example/issues/3")
    # an uninjected finding on another feed is listed, not scored
    apply(reconcile([Detection("fred_dgs10", "write_recency", "BREACH", 90.0, 80.0, "stale")], [], {}, "r4", NOW + timedelta(hours=5)), wh)

    res = benchmark.run(wh, now=NOW + timedelta(days=5))  # past every PENDING window
    t = res["totals"]
    assert t["episodes"] == 3 and t["detected"] == 2 and t["correct"] == 2 and t["missed"] == 1 and t["uninjected_findings"] == 1
    assert t["median_mttd_min"] == 180 and t["median_mttr_min"] == 60 and t["gate_rejections"] == 1 and t["human_requeues"] == 1
    ep = res["episodes"][0]
    assert ep.outcome == "FIXED" and ep.evidence_ref == "https://example/pull/10"   # latest terminal row wins, not the earlier ESCALATED
    md = benchmark.markdown(res)
    assert "CORRECT" in md and "MISSED" in md and "fred_dgs10" in md and "watchdog-only" in md
    # the same undetected episode is PENDING, not MISSED, while its detection window is still open
    assert benchmark.run(wh, now=NOW + timedelta(hours=31))["totals"]["pending"] == 1


def test_a_human_requeue_after_escalation_reopens_the_episode_and_findings_belong_to_one_episode(wh):
    _inject(wh, "coingecko_prices", "schema_rename", NOW)
    _inject(wh, "coingecko_prices", "schema_rename", NOW + timedelta(hours=5))   # 2nd episode, windows overlap
    d = reconcile([Detection("coingecko_prices", "write_recency", "BREACH", 4.0, 3.0, "s")], [], {}, "r1", NOW + timedelta(hours=3))
    apply(d, wh)
    fid = d.opened[0]["finding_id"]
    dispose(wh, fid, "ESCALATED", "agent", "3 attempts", NOW + timedelta(hours=4), "https://example/issues/6")
    dispose(wh, fid, "DEFERRED", "human:isabelle", "try again", NOW + timedelta(hours=4, minutes=30))
    res = benchmark.run(wh, now=NOW + timedelta(hours=6))
    e1, e2 = res["episodes"]
    assert e1.finding_ids == [fid] and e2.finding_ids == [] and e2.pending          # claimed once
    assert e1.outcome is None and e1.verdict == "OPEN" and e1.human_requeues == 1  # re-queued: not ESCALATED any more
    assert benchmark._median([10, 30]) == 20 and benchmark._median([10, 30, 50]) == 30
    # a never-detected episode stops being PENDING 48h after its FIRST injected run, however long it keeps running
    for h in range(6, 60, 2):
        _inject(wh, "github_status", "stale_source", NOW + timedelta(hours=h))
    res2 = benchmark.run(wh, now=NOW + timedelta(hours=60))
    gh = next(e for e in res2["episodes"] if e.feed == "github_status")
    assert gh.verdict == "MISSED"


def test_a_later_finding_in_the_same_episode_does_not_move_the_outcome(wh):
    _inject(wh, "ieso_ontario_demand", "frozen_payload", NOW)
    d = reconcile([Detection("ieso_ontario_demand", "write_recency", "BREACH", 31.0, 30.0, "s")], [], {}, "r1", NOW + timedelta(hours=27))
    apply(d, wh)
    dispose(wh, d.opened[0]["finding_id"], "ESCALATED", "agent", "green job", NOW + timedelta(hours=27, minutes=1), "https://example/issues/22")
    d2 = reconcile([Detection("ieso_ontario_demand", "head_lag", "BREACH", 55.0, 54.0, "s")], wh.open_findings(), wh.latest_dispositions(), "r2", NOW + timedelta(hours=33))
    apply(d2, wh)
    dispose(wh, d2.opened[0]["finding_id"], "ESCALATED", "agent", "publisher late", NOW + timedelta(hours=33, minutes=1), "https://example/issues/22")
    ep = benchmark.run(wh, now=NOW + timedelta(days=5))["episodes"][0]
    assert len(ep.finding_ids) == 2 and ep.mttr_min == 1 and ep.outcome == "ESCALATED"
