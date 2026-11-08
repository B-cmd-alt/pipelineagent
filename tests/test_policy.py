"""Policy: an SLA may not be loosened to make a breach disappear without a
written justification. fleet.baseline.json pins the SLAs; any loosening must
be accompanied by an entry in ops/SLA_CHANGES.md naming the feed and axis."""
import json
from pathlib import Path

from pipelineagent.config import load_fleet

ROOT = Path(__file__).resolve().parent.parent


def current_slas(fleet):
    out = {}
    for f in fleet.feeds:
        out[f.id] = {
            "write_recency_max_hours": f.sla.write_recency_max_hours,
            "head_lag_max_hours": f.sla.head_lag_max_hours,
            "flat_max_consecutive": f.sla.flat_value.max_consecutive if f.sla.flat_value else None,
        }
    return out


def test_slas_not_loosened_without_justification():
    fleet = load_fleet(ROOT / "fleet.yaml")
    baseline = json.loads((ROOT / "fleet.baseline.json").read_text())
    changes = (ROOT / "ops" / "SLA_CHANGES.md").read_text()
    cur = current_slas(fleet)
    for fid, base in baseline.items():
        assert fid in cur, f"feed {fid} removed from fleet.yaml without an SLA_CHANGES.md entry" if fid not in changes else ""
        for axis, bval in base.items():
            cval = cur[fid][axis]
            loosened = (bval is not None and cval is None) or (bval is not None and cval is not None and cval > bval)
            if loosened:
                assert f"{fid}/{axis}" in changes, f"{fid}/{axis} loosened {bval} -> {cval} without a `{fid}/{axis}` entry in ops/SLA_CHANGES.md"


def test_baseline_covers_every_feed():
    fleet = load_fleet(ROOT / "fleet.yaml")
    baseline = json.loads((ROOT / "fleet.baseline.json").read_text())
    for f in fleet.feeds:
        assert f.id in baseline, f"add {f.id} to fleet.baseline.json (new feeds need a pinned SLA)"
