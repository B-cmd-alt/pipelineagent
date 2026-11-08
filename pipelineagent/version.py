"""Runtime code identity.

`code_sha256()` hashes every .py file in this package plus the fleet manifest in
use. The running job stamps it into every heartbeat row, so an operator can
compare what is RUNNING against what is COMMITTED (`python -m pipelineagent
version` in the repo) — a committed fix that was never deployed shows up as a
hash mismatch on the dashboard instead of being silently assumed live.
"""
from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path

from . import __version__
from .config import DEFAULT_FLEET_PATH

PACKAGE_DIR = Path(__file__).resolve().parent


@lru_cache(maxsize=4)
def code_sha256(fleet_path: str | None = None) -> str:
    h = hashlib.sha256()
    for p in sorted(PACKAGE_DIR.rglob("*.py")):
        if "__pycache__" in p.parts:
            continue
        h.update(p.relative_to(PACKAGE_DIR).as_posix().encode())
        h.update(b"\0")
        h.update(p.read_bytes())
        h.update(b"\0")
    fp = Path(fleet_path) if fleet_path else DEFAULT_FLEET_PATH
    if fp.exists():
        h.update(b"fleet.yaml\0")
        h.update(fp.read_bytes())
    return h.hexdigest()


def describe(fleet_path: str | None = None) -> dict[str, str]:
    import os

    return {
        "version": __version__,
        "code_sha256": code_sha256(fleet_path),
        "git_commit": os.environ.get("GIT_COMMIT", ""),
    }
