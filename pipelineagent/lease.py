"""One agent stage at a time. Fetching and detecting are append-only and safe to overlap, but two
agent stages running at once (an hourly run and a manual or deploy-verification run) could open two
PRs for one finding. The lease is a single object created with an atomic "only if it does not exist"
precondition: in GCS when a bucket is configured, else an O_EXCL file. A lease older than STALE is a
crashed run's and is taken over."""
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("pipelineagent.lease")
STALE = timedelta(minutes=90)
NAME = "locks/agent-stage"


def _bucket():
    name = os.environ.get("PIPELINEAGENT_PAYLOAD_BUCKET") or os.environ.get("PIPELINEAGENT_SITE_BUCKET")
    if not name:
        return None
    from google.cloud import storage
    return storage.Client().bucket(name)


@contextmanager
def agent_stage(run_id: str, now: datetime | None = None):
    """Yields True when this run holds the lease, False when another live run does."""
    now = now or datetime.now(timezone.utc)
    body = json.dumps({"run_id": run_id, "at": now.isoformat()})
    held, release = False, lambda: None
    try:
        b = _bucket()
        if b is not None:
            from google.api_core.exceptions import NotFound, PreconditionFailed
            blob = b.blob(NAME)
            for _ in range(2):
                try:
                    blob.upload_from_string(body, if_generation_match=0)
                    held = True
                    gen = blob.generation
                    release = lambda: b.blob(NAME).delete(if_generation_match=gen)  # noqa: E731 — only our own lease
                    break
                except PreconditionFailed:
                    try:
                        cur = b.get_blob(NAME)
                        at = datetime.fromisoformat(json.loads(cur.download_as_text())["at"]) if cur else None
                        if cur is not None and (at is None or now - at > STALE):
                            cur.delete(if_generation_match=cur.generation)  # take over a crashed run's lease
                            continue
                    except (NotFound, PreconditionFailed, ValueError, KeyError):
                        continue
                    break
        else:
            p = Path(os.environ.get("PIPELINEAGENT_PAYLOAD_DIR", "payloads")) / NAME
            p.parent.mkdir(parents=True, exist_ok=True)
            for _ in range(2):
                try:
                    fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    os.write(fd, body.encode()); os.close(fd)
                    held, release = True, lambda: p.unlink(missing_ok=True)
                    break
                except FileExistsError:
                    try:
                        at = datetime.fromisoformat(json.loads(p.read_text())["at"])
                    except (OSError, ValueError, KeyError):
                        at = None
                    if at is None or now - at > STALE:
                        p.unlink(missing_ok=True)
                        continue
                    break
    except Exception:  # noqa: BLE001 — if the lease store is unreachable, skip the agent stage rather than risk a double PR
        log.exception("agent-stage lease unavailable; skipping the agent stage this run")
        held = False
    if not held:
        log.warning("agent stage skipped: another run holds the lease")
    try:
        yield held
    finally:
        if held:
            try:
                release()
            except Exception:  # noqa: BLE001 — it goes stale and is taken over
                log.exception("agent-stage lease release failed")
