"""Policy: an SLA may not be loosened to make a breach disappear without a written
justification. The baseline is the merge-base version of fleet.yaml in git (so it cannot be
edited in the same diff); any loosening must be accompanied by a `<feed>/<axis>` entry in
ops/SLA_CHANGES.md. Tightening needs no entry."""
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
AXES = ("write_recency_max_hours", "head_lag_max_hours", "flat_max_consecutive")


def slas(doc: dict) -> dict:
    out = {}
    for f in doc.get("feeds") or []:
        sla = f.get("sla") or {}
        fv = sla.get("flat_value") or {}
        out[f["id"]] = {"write_recency_max_hours": sla.get("write_recency_max_hours"), "head_lag_max_hours": sla.get("head_lag_max_hours"),
                        "flat_max_consecutive": fv.get("max_consecutive")}
    return out


def baseline_fleet() -> dict | None:
    """fleet.yaml as of the merge-base with the default branch; None when git cannot tell."""
    try:
        base = subprocess.run(["git", "merge-base", "HEAD", "origin/main"], cwd=ROOT, capture_output=True, text=True, timeout=20)
        if base.returncode != 0:
            return None
        rel = Path("pipelineagent") / "fleet.yaml" if (ROOT.parent / ".git").exists() or (ROOT.parent / "pipelineagent").exists() else Path("fleet.yaml")
        show = subprocess.run(["git", "show", f"{base.stdout.strip()}:{rel.as_posix()}"], cwd=ROOT, capture_output=True, text=True, timeout=20)
        if show.returncode != 0:
            return None  # fleet.yaml did not exist on the base (brand-new project): nothing to compare against yet
        return yaml.safe_load(show.stdout)
    except (OSError, subprocess.SubprocessError):
        return None


def test_slas_not_loosened_without_justification():
    base = baseline_fleet()
    if base is None:
        pytest.skip("no git baseline for fleet.yaml (shallow clone, tarball, or first version)")
    cur = slas(yaml.safe_load((ROOT / "fleet.yaml").read_text()))
    changes = (ROOT / "ops" / "SLA_CHANGES.md").read_text()
    for fid, bsla in slas(base).items():
        if fid not in cur:
            assert f"{fid}/removed" in changes, f"feed {fid} removed from fleet.yaml without a `{fid}/removed` entry in ops/SLA_CHANGES.md"
            continue
        for axis, bval in bsla.items():
            cval = cur[fid][axis]
            loosened = (bval is not None and cval is None) or (bval is not None and cval is not None and cval > bval)
            if loosened:
                assert f"{fid}/{axis}" in changes, f"{fid}/{axis} loosened {bval} -> {cval} without a `{fid}/{axis}` entry in ops/SLA_CHANGES.md"


def test_policy_detects_loosening_on_a_synthetic_pair():
    base = {"feeds": [{"id": "a", "sla": {"write_recency_max_hours": 3, "flat_value": {"column": "x", "max_consecutive": 6}}}]}
    cur = {"feeds": [{"id": "a", "sla": {"write_recency_max_hours": 5}}]}
    b, c = slas(base), slas(cur)
    assert c["a"]["write_recency_max_hours"] > b["a"]["write_recency_max_hours"]  # widened
    assert b["a"]["flat_max_consecutive"] is not None and c["a"]["flat_max_consecutive"] is None  # axis removed = loosened
