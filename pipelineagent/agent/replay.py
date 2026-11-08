"""Replay: feed archived bytes back through a fetcher's own fetch/parse/load path in a
plain subprocess with sockets blocked, against an in-memory SQLite warehouse.

Used by the agent (before proposing a PR) and re-run by the gate (which trusts nothing
the agent says). The judgement is about the LOAD path: did the parser accept the bytes
and land rows? Head-lag on historical bytes is meaningless and is not judged here.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ReplayResult:
    fetch_ok: bool
    rows: int
    error: str | None
    anomalies: list[str] = field(default_factory=list)
    rows_digest: str = ""
    raw: dict = field(default_factory=dict)

    @property
    def loads_cleanly(self) -> bool:
        return self.fetch_ok and self.rows > 0 and not any("schema" in a or "non-numeric" in a for a in self.anomalies)


def load_payload_bodies(raw: bytes) -> list[bytes]:
    """An archive is either one body or a JSON array of bodies (multi-request feeds)."""
    if raw[:2] == b'["':
        try:
            arr = json.loads(raw)
            if isinstance(arr, list) and all(isinstance(x, str) for x in arr):
                return [x.encode("utf-8") for x in arr]
        except ValueError:
            pass
    return [raw]


def replay(package_root: Path, feed_id: str, payload: bytes, fleet_rel: str = "fleet.yaml", timeout_s: int = 60) -> ReplayResult:
    """Run `replay_worker` from `package_root` (which must contain the `pipelineagent` package)."""
    payload_file = package_root / f".replay-{hashlib.sha256(payload).hexdigest()[:12]}.bin"
    payload_file.write_bytes(payload)
    try:
        # -E ignores PYTHON* env vars so nothing outside `package_root` is imported by accident; the
        # tree under test is put on sys.path explicitly. (-I would also drop the user site-packages
        # where the HTTP/YAML libraries may live, so it is not used.)
        boot = ("import sys, runpy; sys.path.insert(0, sys.argv.pop(1)); "
                "runpy.run_module('pipelineagent.agent.replay_worker', run_name='__main__')")
        proc = subprocess.run(
            [sys.executable, "-E", "-c", boot, str(package_root), "--fleet", str(package_root / fleet_rel), "--feed", feed_id, "--payload", str(payload_file)],
            cwd=str(package_root), capture_output=True, text=True, timeout=timeout_s, env={"PATH": "/usr/bin:/bin", "HOME": "/tmp"},
        )
    except subprocess.TimeoutExpired:
        return ReplayResult(False, 0, f"replay timed out after {timeout_s}s")
    finally:
        payload_file.unlink(missing_ok=True)
    out = proc.stdout.strip().splitlines()
    try:
        data = json.loads(out[-1]) if out else {}
    except ValueError:
        data = {}
    if not data:
        return ReplayResult(False, 0, f"worker produced no result (rc={proc.returncode}): {proc.stderr[-800:]}")
    return ReplayResult(bool(data.get("fetch_ok")), int(data.get("rows", 0)), data.get("error"), list(data.get("anomalies", [])),
                        data.get("rows_digest", ""), data)
