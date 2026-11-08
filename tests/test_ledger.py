from datetime import timedelta

import pytest
from conftest import NOW

from pipelineagent.detectors import Detection
from pipelineagent.ledger import apply, dispose, reconcile


def det(feed, axis, state, detail="d", transient=False):
    return Detection(feed, axis, state, 1.0, 2.0, detail, transient=transient)


def test_lifecycle_open_close_with_auto_dispositions_and_no_hourly_rows(wh):
    d1 = reconcile([det("f", "head_lag", "BREACH")], wh.open_findings(), wh.latest_dispositions(), "r1", NOW)
    assert len(d1.opened) == 1 and d1.dispositions[0]["disposition"] == "UNKNOWN"
    apply(d1, wh)
    fid = d1.opened[0]["finding_id"]
    assert wh.open_findings()[0]["opened_at"] == NOW

    # known-open and still not OK (even if the state flips BREACH<->UNKNOWN): NOTHING is written —
    # the hourly state lives in detections; the ledger only records transitions.
    d2 = reconcile([det("f", "head_lag", "UNKNOWN")], wh.open_findings(), wh.latest_dispositions(), "r2", NOW + timedelta(hours=1))
    assert d2.finding_rows == [] and d2.dispositions == []

    d3 = reconcile([det("f", "head_lag", "OK")], wh.open_findings(), wh.latest_dispositions(), "r3", NOW + timedelta(hours=2))
    assert [r["event"] for r in d3.finding_rows] == ["CLOSED"]
    assert d3.dispositions[0]["disposition"] == "BENIGN" and d3.dispositions[0]["decided_by"] == "auto"
    apply(d3, wh)
    assert wh.open_findings() == []
    assert wh.latest_dispositions()[fid]["disposition"] == "BENIGN"
    kinds = [e["kind"] for e in wh.ledger_events(50)]
    assert kinds == ["OPENED", "DISPOSITION", "CLOSED", "DISPOSITION"] or kinds == ["DISPOSITION", "OPENED", "DISPOSITION", "CLOSED"] or len(kinds) == 4


def test_transient_unknown_is_shown_but_opens_no_finding(wh):
    d = reconcile([det("f", "flat_value", "UNKNOWN", "insufficient history", transient=True)], [], {}, "r1", NOW)
    assert d.opened == [] and d.dispositions == []
    # ...but a transient state CLOSES an already-open finding on that axis (it is no longer a breach)
    d1 = reconcile([det("f", "flat_value", "BREACH")], [], {}, "r1", NOW)
    apply(d1, wh)
    d2 = reconcile([det("f", "flat_value", "UNKNOWN", "insufficient history", transient=True)], wh.open_findings(), wh.latest_dispositions(), "r2", NOW + timedelta(hours=1))
    assert [r["event"] for r in d2.finding_rows] == ["CLOSED"]


def test_human_disposition_survives_auto_close(wh):
    d1 = reconcile([det("f", "flat_value", "BREACH")], [], {}, "r1", NOW)
    apply(d1, wh)
    fid = d1.opened[0]["finding_id"]
    dispose(wh, fid, "ESCALATED", "human:alice", "upstream ticket filed", NOW + timedelta(minutes=5))
    d3 = reconcile([det("f", "flat_value", "OK")], wh.open_findings(), wh.latest_dispositions(), "r3", NOW + timedelta(hours=2))
    apply(d3, wh)
    assert wh.latest_dispositions()[fid]["disposition"] == "ESCALATED"  # auto-BENIGN must not overwrite a human call


def test_fixed_requires_evidence(wh):
    with pytest.raises(ValueError):
        dispose(wh, "abc", "FIXED", "agent", "patched parser", NOW)
    dispose(wh, "abc", "FIXED", "agent", "patched parser", NOW, evidence_ref="verdicts/abc.json")


def test_removed_detector_does_not_silently_close_findings(wh):
    d1 = reconcile([det("f", "head_lag", "BREACH")], [], {}, "r1", NOW)
    apply(d1, wh)
    d2 = reconcile([], wh.open_findings(), wh.latest_dispositions(), "r2", NOW + timedelta(hours=1))
    assert d2.finding_rows == [] and d2.dispositions == []
    assert len(wh.open_findings()) == 1
