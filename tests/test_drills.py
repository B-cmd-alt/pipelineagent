"""Layer 2: the canary feed, the clock-driven drill schedule and the watchdog over drill results."""
from datetime import datetime, timedelta, timezone

import pytest
from conftest import NOW

from pipelineagent import drillwatch, harness
from pipelineagent.config import Drill
from pipelineagent.detectors import Detection
from pipelineagent.feeds import canary_selfcheck
from pipelineagent.http import Response
from pipelineagent.ledger import apply, dispose, reconcile

D = Drill("canary_selfcheck", ("stale_source", "frozen_payload", "schema_rename"), 2, 2, 8)
WED = datetime(2026, 11, 11, 2, 0, tzinfo=timezone.utc)  # a Wednesday, week 44 from the fixed Monday


def test_canary_parses_the_published_page_and_fails_loudly_without_a_clock():
    rows, anomalies = canary_selfcheck.parse({"generated_at": "2026-11-10T17:56:01.912837+00:00", "summary": {"ok": 15, "breach": 3, "unknown": 2, "open_findings": 5}})
    assert rows[0]["event_ts"] == datetime(2026, 11, 10, 17, 56, 1, 912837, tzinfo=timezone.utc) and rows[0]["ok_cells"] == 15 and anomalies == []
    with pytest.raises(ValueError, match="schema"):
        # No clock at all, not a renamed one: a weekly drill renames the key and a correct repair accepts
        # it, so pinning "generated_w44 raises" failed every good patch (2026-11-11 drill, issue #32).
        canary_selfcheck.parse({"summary": {"ok": 15}})


def test_schedule_is_a_pure_function_of_the_clock_and_rotates_weekly():
    assert harness.scheduled((D,), WED - timedelta(minutes=1)) == {}
    assert harness.scheduled((D,), WED + timedelta(hours=8)) == {}
    kinds = [harness.scheduled((D,), WED + timedelta(weeks=w, hours=3))["canary_selfcheck"].kind for w in range(3)]
    assert sorted(kinds) == sorted(D.rotation)  # three consecutive weeks cover the whole rotation
    inj = harness.scheduled((D,), WED + timedelta(hours=1))["canary_selfcheck"]
    assert inj.kind == "schema_rename" and inj.variant == "w44"
    late = Drill("canary_selfcheck", ("stale_source",), 6, 22, 6)  # Sunday 22:00 runs into Monday
    assert "canary_selfcheck" in harness.scheduled((late,), datetime(2026, 11, 16, 1, 0, tzinfo=timezone.utc))


def test_each_week_renames_to_a_new_key_so_an_old_repair_does_not_pre_fix_the_drill():
    class Inner:
        used, budget, last_body = 0, 40, None

        def get(self, url, **kw):
            return Response(200, b'{"generated_at":"x"}', "application/json", None)

    a = harness.InjectingClient(Inner(), harness.Injection("canary_selfcheck", "schema_rename", "w44")).get("u").body
    b = harness.InjectingClient(Inner(), harness.Injection("canary_selfcheck", "schema_rename", "w47")).get("u").body
    assert a == b'{"generated_w44":"x"}' and b == b'{"generated_w47":"x"}'


def test_manual_injection_overrides_the_schedule(fleet, monkeypatch):
    assert fleet.drills and fleet.drills[0].feed == "canary_selfcheck"
    monkeypatch.setenv("PIPELINEAGENT_INJECT", "canary_selfcheck=stale_source")
    assert harness.active(fleet, WED + timedelta(hours=1))["canary_selfcheck"].kind == "stale_source"


class Hub:
    def __init__(self):
        self.opened = []

    def open_issue(self, fid, title, body):
        self.opened.append((fid, body))
        return f"https://example/issues/{len(self.opened)}"


def _episode(wh, kind, at):
    harness.record(wh, {"canary_selfcheck": harness.Injection("canary_selfcheck", kind)}, "r", at)


def test_a_wrong_drill_result_opens_exactly_one_issue_and_a_correct_one_none(fleet, wh):
    _episode(wh, "stale_source", NOW)
    d = reconcile([Detection("canary_selfcheck", "write_recency", "BREACH", 4.0, 3.0, "s")], [], {}, "r1", NOW + timedelta(hours=3))
    apply(d, wh)
    fid = d.opened[0]["finding_id"]
    dispose(wh, fid, "ESCALATED", "agent", "upstream", NOW + timedelta(hours=4), "https://example/issues/9")
    hub = Hub()
    assert drillwatch.check(fleet, wh, hub, "r2", NOW + timedelta(hours=5)) == [] and hub.opened == []
    assert drillwatch.status(fleet, wh, NOW + timedelta(hours=5))[0]["state"] == "OK"
    # the same finding ends FIXED although nothing was patchable: a WRONG verdict
    dispose(wh, fid, "FIXED", "agent", "claimed", NOW + timedelta(hours=6), "https://example/pull/1")
    raised = drillwatch.check(fleet, wh, hub, "r3", NOW + timedelta(hours=7))
    assert len(raised) == 1 and "WRONG" in hub.opened[0][1]
    assert drillwatch.check(fleet, wh, hub, "r4", NOW + timedelta(hours=8)) == [] and len(hub.opened) == 1
    assert drillwatch.status(fleet, wh, NOW + timedelta(hours=8))[0]["state"] == "BREACH"


def test_a_missed_drill_and_a_silent_schedule_both_alarm(fleet, wh):
    _episode(wh, "stale_source", NOW)
    hub = Hub()
    assert drillwatch.check(fleet, wh, hub, "r", NOW + timedelta(hours=30)) == []          # still PENDING
    ids = drillwatch.check(fleet, wh, hub, "r", NOW + timedelta(hours=60))                 # never detected -> MISSED
    assert len(ids) == 1 and "MISSED" in hub.opened[0][1]
    ids = drillwatch.check(fleet, wh, hub, "r", NOW + timedelta(days=9))                   # and no drill since
    assert len(ids) == 1 and "stale" in ids[0] and "no drill has run" in hub.opened[1][1]


def test_dashboard_shows_the_self_test_line(fleet, wh):
    from pipelineagent.render_site import collect, render_html
    page = render_html(collect(fleet, wh, NOW))
    assert "Self-test on" in page and "no drill has run yet" in page


def test_moving_target_renames_to_a_new_unrelated_key_every_hour_and_expects_escalation(wh):
    class Inner:
        used, budget, last_body = 0, 40, None

        def get(self, url, **kw):
            return Response(200, b'{"generated_at":"x"}', "application/json", None)

    c = harness.InjectingClient(Inner(), harness.Injection("canary_selfcheck", "moving_target"))
    c.clock = datetime(2026, 11, 11, 11, 5, tzinfo=timezone.utc)
    a = c.get("u").body
    c.clock = datetime(2026, 11, 11, 11, 55, tzinfo=timezone.utc)
    assert c.get("u").body == a                      # stable within the hour, so the gate's replay is honest
    c.clock = datetime(2026, 11, 11, 12, 5, tzinfo=timezone.utc)
    b = c.get("u").body
    assert a != b and b"generated" not in a + b       # a new key, sharing nothing with the old ones
    from pipelineagent import benchmark
    harness.record(wh, {"canary_selfcheck": harness.Injection("canary_selfcheck", "moving_target")}, "r", NOW)
    ep = benchmark.run(wh, now=NOW + timedelta(days=5))["episodes"][0]
    assert ep.failure_class == "schema_drift" and ep.expected == "ESCALATED"
