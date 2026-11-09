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

    res = benchmark.run(wh, now=NOW + timedelta(days=3))
    t = res["totals"]
    assert t["episodes"] == 3 and t["detected"] == 2 and t["correct"] == 2 and t["missed"] == 1 and t["uninjected_findings"] == 1
    assert t["median_mttd_min"] == 180 and t["median_mttr_min"] == 60 and t["gate_rejections"] == 1 and t["human_requeues"] == 1
    ep = res["episodes"][0]
    assert ep.outcome == "FIXED" and ep.evidence_ref == "https://example/pull/10"   # latest terminal row wins, not the earlier ESCALATED
    md = benchmark.markdown(res)
    assert "CORRECT" in md and "MISSED" in md and "fred_dgs10" in md and "watchdog-only" in md
    # the same undetected episode is PENDING, not MISSED, while its detection window is still open
    assert benchmark.run(wh, now=NOW + timedelta(hours=31))["totals"]["pending"] == 1
