from datetime import timedelta

import pytest
from conftest import NOW

from pipelineagent.detectors import Detection
from pipelineagent.ledger import apply, dispose, reconcile


def det(feed, axis, state, detail="d"):
    return Detection(feed, axis, state, 1.0, 2.0, detail)


def test_lifecycle_open_still_open_close_with_auto_dispositions(wh):
    d1 = reconcile([det("f", "head_lag", "BREACH")], wh.open_findings(), wh.latest_dispositions(), "r1", NOW)
    assert len(d1.opened) == 1 and d1.dispositions[0]["disposition"] == "UNKNOWN"
    apply(d1, wh)
    fid = d1.opened[0]["finding_id"]

    d2 = reconcile([det("f", "head_lag", "UNKNOWN")], wh.open_findings(), wh.latest_dispositions(), "r2", NOW + timedelta(hours=1))
    assert [r["event"] for r in d2.finding_rows] == ["STILL_OPEN"] and d2.finding_rows[0]["finding_id"] == fid
    assert d2.dispositions == []  # known-open: one ledger line, no new disposition, no re-diagnosis
    apply(d2, wh)

    d3 = reconcile([det("f", "head_lag", "OK")], wh.open_findings(), wh.latest_dispositions(), "r3", NOW + timedelta(hours=2))
    assert [r["event"] for r in d3.finding_rows] == ["CLOSED"]
    assert d3.dispositions[0]["disposition"] == "BENIGN" and d3.dispositions[0]["decided_by"] == "auto"
    apply(d3, wh)
    assert wh.open_findings() == []
    assert wh.latest_dispositions()[fid]["disposition"] == "BENIGN"
    assert len(d1.healthlog_lines()) == 1 and len(d3.healthlog_lines()) == 2


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
