"""The gate (`proofgate check`): a diff linter plus a re-run of the replay. No model.

It judges a HEAD tree against a BASE tree for one failing feed and writes a verdict in the
shape of schemas/verdict.schema.json. PASS requires no code other than REPLAY_PASSED and
TESTS_PASSED. Everything the agent claimed in its own evidence is ignored here on purpose.
"""
from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .replay import ReplayResult, replay

DETECTOR_FILES = ("pipelineagent/detectors.py", "pipelineagent/ledger.py", "pipelineagent/runner.py", "pipelineagent/heartbeat.py",
                  "pipelineagent/schema.py", "pipelineagent/config.py", "pipelineagent/warehouse/", "pipelineagent/agent/")
POLICY_FILES = ("fleet.yaml", "ops/", "schemas/", "Dockerfile", "deploy.sh", "cloudbuild.yaml", ".github/", "pyproject.toml")
_PEM = "-" * 5 + "BEGIN [A-Z ]*PRIVATE KEY"  # assembled so this file does not itself look like a key dump
SECRET_RE = re.compile(r"(AKIA[0-9A-Z]{16}|" + _PEM + r"|sk-ant-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9]{20,}|api[_-]?key\s*=\s*['\"][^'\"]{8,})", re.I)
SKIP_LINT = re.compile(r"(^|/)(__pycache__|\.pytest_cache|site|\.git)(/|$)|\.pyc$|\.db$|^\.replay-")


def _tree(root: Path) -> dict[str, str]:
    out = {}
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            if SKIP_LINT.search(rel):
                continue
            out[rel] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def changed_files(base: Path, head: Path) -> tuple[set[str], set[str], set[str]]:
    b, h = _tree(base), _tree(head)
    modified = {f for f in b if f in h and b[f] != h[f]}
    added = set(h) - set(b)
    deleted = set(b) - set(h)
    return modified, added, deleted


def slas(doc: dict) -> dict:
    out = {}
    for f in doc.get("feeds") or []:
        sla = f.get("sla") or {}
        fv = sla.get("flat_value") or {}
        out[f["id"]] = {"write_recency_max_hours": sla.get("write_recency_max_hours"), "head_lag_max_hours": sla.get("head_lag_max_hours"),
                        "flat_max_consecutive": fv.get("max_consecutive")}
    return out


def sla_widened(base_fleet: Path, head_fleet: Path) -> list[str]:
    if not base_fleet.exists() or not head_fleet.exists():
        return ["fleet.yaml removed"] if base_fleet.exists() else []
    b, h = slas(yaml.safe_load(base_fleet.read_text())), slas(yaml.safe_load(head_fleet.read_text()))
    out = []
    for fid, bs in b.items():
        if fid not in h:
            out.append(f"{fid} removed")
            continue
        for axis, bv in bs.items():
            hv = h[fid][axis]
            if (bv is not None and hv is None) or (bv is not None and hv is not None and hv > bv):
                out.append(f"{fid}/{axis} {bv}->{hv}")
    return out


def lint(base: Path, head: Path, feed_id: str) -> tuple[list[str], dict]:
    modified, added, deleted = changed_files(base, head)
    codes: list[str] = []
    notes: dict = {"modified": sorted(modified), "added": sorted(added), "deleted": sorted(deleted)}
    allowed_mod = {f"pipelineagent/feeds/{feed_id}.py"}
    touched = modified | added | deleted
    if any(f.startswith("tests/") for f in modified | deleted):
        codes.append("TEST_WEAKENED")
    if any(f.startswith(DETECTOR_FILES) for f in touched):
        codes.append("DETECTOR_DISABLED")
    if any(f.startswith(POLICY_FILES) for f in touched):
        codes.append("POLICY_FILE_TOUCHED")
    widened = sla_widened(base / "fleet.yaml", head / "fleet.yaml")
    if widened:
        codes.append("SLA_WIDENED")
        notes["sla_widened"] = widened
    out_of_scope = {f for f in touched if not (f in allowed_mod or (f.startswith("tests/") and f in added))}
    if out_of_scope:
        codes.append("SCOPE_EXCEEDED")
        notes["out_of_scope"] = sorted(out_of_scope)
    for f in modified | added:
        if SECRET_RE.search((head / f).read_text(errors="ignore")):
            codes.append("SECRET_IN_DIFF")
            break
    return sorted(set(codes)), notes


def run_tests(head: Path, timeout_s: int = 300) -> tuple[bool, str]:
    if not (head / "tests").exists():
        return False, "no tests/ directory in the head tree"
    # the agent's own end-to-end tests spawn this gate; running them here would recurse
    proc = subprocess.run([sys.executable, "-E", "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", "-m", "not agent_e2e"],
                          cwd=str(head), capture_output=True, text=True, timeout=timeout_s)
    return proc.returncode == 0, (proc.stdout + proc.stderr)[-1500:]


def check(base: Path, head: Path, feed_id: str, failing_payloads: list[bytes], healthy_payloads: list[bytes], fleet_rel: str = "fleet.yaml",
          finding_id: str = "", run_tests_too: bool = True) -> dict:
    codes, notes = lint(base, head, feed_id)
    diag: list[str] = []
    before: dict[str, ReplayResult] = {}
    after: dict[str, ReplayResult] = {}
    # 1 + 2: reproduce on base, clear on head
    for i, pl in enumerate(failing_payloads):
        before[f"failing[{i}]"] = b = replay(base, feed_id, pl, fleet_rel)
        after[f"failing[{i}]"] = h = replay(head, feed_id, pl, fleet_rel)
        if b.loads_cleanly:
            diag.append(f"failing payload {i}: base code loads it cleanly ({b.rows} rows) — the breach does not reproduce from these bytes")
            codes.append("REPLAY_FAILED")
        elif not h.loads_cleanly:
            diag.append(f"failing payload {i}: patched code still fails: {h.error or h.anomalies}")
            codes.append("REPLAY_FAILED")
        else:
            diag.append(f"failing payload {i}: base {b.error or 'no rows'} -> patched {h.rows} rows")
    # 3: no collateral change on healthy payloads
    for i, pl in enumerate(healthy_payloads):
        b, h = replay(base, feed_id, pl, fleet_rel), replay(head, feed_id, pl, fleet_rel)
        before[f"healthy[{i}]"], after[f"healthy[{i}]"] = b, h
        if b.loads_cleanly and (not h.loads_cleanly or b.rows_digest != h.rows_digest):
            diag.append(f"healthy payload {i}: output changed under the patch ({b.rows} rows -> {h.rows}, digest {'differs' if h.loads_cleanly else 'n/a'})")
            codes.append("COLLATERAL_CHANGE")
    if "REPLAY_FAILED" not in codes and failing_payloads:
        codes.append("REPLAY_PASSED")
    # 4: tests
    if run_tests_too:
        ok, out = run_tests(head)
        codes.append("TESTS_PASSED" if ok else "TESTS_FAILED")
        if not ok:
            diag.append("tests: " + out[-600:])
    codes = sorted(set(codes))
    verdict = "PASS" if set(codes) <= {"REPLAY_PASSED", "TESTS_PASSED"} and "REPLAY_PASSED" in codes else "FAIL"
    patch_text = "".join(f"{f}\n{(head / f).read_text(errors='ignore') if (head / f).exists() else ''}" for f in sorted(set(notes["modified"]) | set(notes["added"])))
    return {
        "schema_version": 1,
        "finding_id": finding_id or "0000000000000000",
        "run_manifest": {
            "agent_run_id": "gate", "base_commit": "", "code_sha256_before": _code_hash(base), "code_sha256_after": _code_hash(head),
            "replay_inputs": [hashlib.sha256(p).hexdigest() for p in failing_payloads + healthy_payloads],
            "started_at": datetime.now(timezone.utc).isoformat(), "finished_at": datetime.now(timezone.utc).isoformat(),
        },
        "patch_sha256": hashlib.sha256(patch_text.encode()).hexdigest(),
        "verdict": verdict,
        "reason_codes": codes,
        "evidence": {"failure_class": "schema_drift", "diagnosis": "; ".join(diag) or "no replay inputs",
                     "before": {k: {"fetch_ok": v.fetch_ok, "rows": v.rows, "error": v.error} for k, v in before.items()},
                     "after": {k: {"fetch_ok": v.fetch_ok, "rows": v.rows, "error": v.error} for k, v in after.items()},
                     "lint": notes},
    }


def _code_hash(root: Path) -> str:
    h = hashlib.sha256()
    for rel, digest in sorted(_tree(root).items()):
        if rel.startswith("pipelineagent/") or rel == "fleet.yaml":
            h.update(rel.encode()); h.update(digest.encode())
    return h.hexdigest()


def main(argv=None) -> int:
    import argparse, json
    ap = argparse.ArgumentParser(prog="proofgate", description="diff linter + replay re-run; writes a verdict.json")
    ap.add_argument("check", nargs="?")
    ap.add_argument("--base", required=True); ap.add_argument("--head", required=True); ap.add_argument("--feed", required=True)
    ap.add_argument("--failing", action="append", default=[], help="archived failing payload file(s)")
    ap.add_argument("--healthy", action="append", default=[], help="archived healthy payload file(s)")
    ap.add_argument("--out", default="verdict.json"); ap.add_argument("--no-tests", action="store_true")
    a = ap.parse_args(argv)
    v = check(Path(a.base), Path(a.head), a.feed, [Path(p).read_bytes() for p in a.failing], [Path(p).read_bytes() for p in a.healthy], run_tests_too=not a.no_tests)
    Path(a.out).write_text(json.dumps(v, indent=2, default=str))
    print(v["verdict"], ",".join(v["reason_codes"]))
    return 0 if v["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
