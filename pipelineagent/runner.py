"""The loop: fetch -> write -> detect -> ledger -> heartbeat -> render.

Stage isolation is the point: a feed that explodes is recorded as a failed run
and the loop moves on; detectors run regardless of how the fetch stage went
(they read the data, not this process); the heartbeat is written only after
detection + ledger completed, so "alive" means the whole loop ran.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import feeds as feed_registry
from . import harness
from .config import Fleet, FeedSpec
from .detectors import Detection, detect_feed
from .heartbeat import beat
from .http import BudgetExhausted, Client, NotFound, RateLimited, SourceError
from .ledger import LedgerDelta, apply as apply_ledger, reconcile
from .schema import DETECTIONS, FEED_COMMON, RUNS, SYSTEM_PARTITION, SYSTEM_TABLES
from .version import code_sha256
from .warehouse.base import Warehouse
from .feeds.base import FetchContext

log = logging.getLogger("pipelineagent")


@dataclass
class RunReport:
    run_id: str
    started_at: datetime
    runs: list[dict[str, Any]] = field(default_factory=list)
    detections: list[Detection] = field(default_factory=list)
    ledger: LedgerDelta | None = None
    heartbeat: dict | None = None
    duration_s: float = 0.0

    @property
    def feeds_ok(self) -> int:
        return sum(1 for r in self.runs if r["status"] in ("ok", "no_change"))

    @property
    def breaches(self) -> int:
        return sum(1 for d in self.detections if d.state == "BREACH")

    @property
    def unknowns(self) -> int:
        return sum(1 for d in self.detections if d.state == "UNKNOWN")


def ensure_warehouse(fleet: Fleet, wh: Warehouse) -> None:
    for name, cols in SYSTEM_TABLES.items():
        wh.ensure_table(name, cols, SYSTEM_PARTITION[name], False, description=f"PipelineAgent {name} (append-only)")
    for spec in fleet.feeds:
        feed = feed_registry.get(spec.id)
        wh.ensure_table(spec.table, FEED_COMMON + feed.columns, "captured_at", True, description=spec.description)


def _payload_store():
    """(writer, reader) for raw payloads keyed by feed + content sha. GCS when
    PIPELINEAGENT_PAYLOAD_BUCKET is set, else a local directory (PIPELINEAGENT_PAYLOAD_DIR,
    default ./payloads). Both fail soft on write; the row is already in the warehouse."""
    bucket = os.environ.get("PIPELINEAGENT_PAYLOAD_BUCKET")
    if bucket:
        def write(feed_id: str, sha: str, raw: bytes) -> None:
            from google.cloud import storage

            blob = storage.Client().bucket(bucket).blob(f"raw/{feed_id}/{sha}")
            if not blob.exists():
                blob.upload_from_string(raw, content_type="application/octet-stream")

        def read(feed_id: str, sha: str) -> bytes | None:
            from google.cloud import storage

            blob = storage.Client().bucket(bucket).blob(f"raw/{feed_id}/{sha}")
            return blob.download_as_bytes() if blob.exists() else None
        return write, read
    root = Path(os.environ.get("PIPELINEAGENT_PAYLOAD_DIR", "payloads"))

    def write_local(feed_id: str, sha: str, raw: bytes) -> None:
        p = root / feed_id / sha
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.write_bytes(raw)

    def read_local(feed_id: str, sha: str) -> bytes | None:
        p = root / feed_id / sha
        return p.read_bytes() if p.exists() else None
    return write_local, read_local


def _frozen_bodies(wh: Warehouse, feed_id: str) -> list[bytes]:
    """The newest archived HEALTHY payload of a feed (for the class-1 frozen_payload injection)."""
    from .agent.replay import load_payload_bodies

    read = _payload_store()[1]
    try:
        for run in wh.runs_for_feed(feed_id, 200):
            # no_change runs carry the same healthy sha, so a frozen daily feed keeps resolving for days
            if run["status"] in ("ok", "no_change") and run.get("content_sha256"):
                raw = read(feed_id, run["content_sha256"])
                if raw:
                    return load_payload_bodies(raw)
    except Exception:  # noqa: BLE001 — an archive read failure must not abort the whole run; the drill just fails loudly
        log.exception("frozen_payload: archive lookup failed for %s", feed_id)
    return []


def archive_payload(feed_id: str, sha: str | None, raw: bytes | None) -> None:
    """Keep the exact bytes a run saw, keyed by content hash, so a failure can be replayed later."""
    if not sha or raw is None:
        return
    try:
        _payload_store()[0](feed_id, sha, raw)
    except Exception:  # noqa: BLE001
        log.exception("payload archive failed for %s (data row already written)", feed_id)


def load_payload(feed_id: str, sha: str) -> bytes | None:
    try:
        return _payload_store()[1](feed_id, sha)
    except Exception:  # noqa: BLE001
        log.exception("payload load failed for %s/%s", feed_id, sha)
        return None


def _archive_failed_body(feed_id: str, http: Client, row: dict[str, Any]) -> None:
    """A parser that rejects a payload is exactly the case the agent must replay later:
    archive the bytes the failing run saw and record their hash on the failed run row."""
    body = getattr(http, "last_body", None)
    if body:
        sha = hashlib.sha256(body).hexdigest()
        row["content_sha256"] = sha
        archive_payload(feed_id, sha, body)
    http.last_body = None if hasattr(http, "last_body") else None


def _due(spec: FeedSpec, last_runs: dict, now: datetime, refetch_after_h: float) -> tuple[bool, str]:
    if spec.cadence == "hourly":
        return True, ""
    lr = last_runs.get(spec.id)
    if lr is None or lr.status == "failed":
        return True, ""
    age_h = (now - lr.finished_at).total_seconds() / 3600
    if age_h < refetch_after_h:
        return False, f"daily feed refreshed {age_h:.1f}h ago (< {refetch_after_h:g}h)"
    return True, ""


def run_feed(spec: FeedSpec, fleet: Fleet, wh: Warehouse, http: Client, last_runs: dict, run_id: str, now: datetime) -> dict[str, Any]:
    feed = feed_registry.get(spec.id)
    t0 = time.monotonic()
    started = now  # the loop's reference clock, so every row of one run shares a vintage and tests can freeze time
    row: dict[str, Any] = {"run_id": run_id, "feed": spec.id, "started_at": started, "status": "failed", "reason": None,
                           "rows_written": 0, "duration_s": 0.0, "http_status": None, "requests_used": 0, "source_modified": None,
                           "content_sha256": None, "error": None, "code_sha256": code_sha256(str(fleet.path))}
    used0 = http.used
    if hasattr(http, "last_body"):
        http.last_body = None
    due, why = _due(spec, last_runs, now, fleet.daily_feed_refetch_after_hours)
    if isinstance(http, harness.InjectingClient):
        due, why = True, ""  # an injected feed is always due, so a daily feed's drill does not wait 20h
    try:
        if not due:
            row.update(status="skipped", reason="not_due", error=why)
        else:
            ctx = FetchContext(http=http, params=spec.params, last_run=last_runs.get(spec.id), warehouse=wh, table=spec.table, now=now)
            res = feed.fetch(ctx)
            row.update(http_status=res.http_status, source_modified=res.source_modified, content_sha256=res.content_sha256)
            prev = last_runs.get(spec.id)
            same = bool(prev and prev.content_sha256 and prev.content_sha256 == res.content_sha256 and prev.status != "failed")
            if same and spec.on_unchanged == "skip":
                row.update(status="no_change", reason="same_content_sha256")
            elif not res.rows:
                row.update(status="no_change", reason="no_new_rows", error=res.note or None)
            else:
                for r in res.rows:
                    r.update(captured_at=started, run_id=run_id, source_modified=res.source_modified, content_sha256=res.content_sha256)
                n = wh.append(spec.table, res.rows, FEED_COMMON + feed.columns)
                row.update(status="ok", rows_written=n)
                archive_payload(spec.id, res.content_sha256, res.raw)
            if res.anomalies:
                row["error"] = "; ".join(res.anomalies)[:1000]
    except NotFound as e:
        row.update(status="skipped", reason="not_found", error=str(e)[:500])  # benign 404
    except RateLimited as e:
        row.update(status="failed", reason="rate_limited", error=str(e)[:500])
    except BudgetExhausted as e:
        row.update(status="skipped", reason="budget_exhausted", error=str(e)[:500])
    except SourceError as e:
        row.update(status="failed", reason="source_error", error=str(e)[:500])
        _archive_failed_body(spec.id, http, row)
    except Exception as e:  # noqa: BLE001 — isolate the feed, record the class, move on
        log.exception("feed %s crashed", spec.id)
        row.update(status="failed", reason=f"exception:{e.__class__.__name__}", error=str(e)[:500])
        _archive_failed_body(spec.id, http, row)
    row["requests_used"] = http.used - used0
    row["duration_s"] = round(time.monotonic() - t0, 3)
    row["finished_at"] = now + timedelta(seconds=row["duration_s"])
    try:
        wh.append("_runs", [row], RUNS)
    except Exception:  # noqa: BLE001
        log.exception("could not write _runs row for %s", spec.id)
    return row


def run(fleet: Fleet, wh: Warehouse, *, feeds: list[str] | None = None, fetch: bool = True, detect: bool = True,
        http: Client | None = None, now: datetime | None = None, agent_ctx=None) -> RunReport:
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    report = RunReport(run_id=uuid.uuid4().hex[:12], started_at=now)
    ensure_warehouse(fleet, wh)
    specs = [s for s in fleet.feeds if not feeds or s.id in feeds]

    if fetch:
        http = http or Client(fleet.user_agent, fleet.request_budget_per_run, fleet.min_seconds_between_requests)
        try:
            last_runs = wh.last_runs()
        except Exception:  # noqa: BLE001
            log.exception("could not read last runs; proceeding as first run")
            last_runs = {}
        injections = harness.rules_from_env()
        harness.record(wh, {k: v for k, v in injections.items() if any(s.id == k for s in specs)}, report.run_id, now)
        for spec in specs:
            client = http
            if spec.id in injections:
                frozen = _frozen_bodies(wh, spec.id) if injections[spec.id].kind == "frozen_payload" else None
                client = harness.InjectingClient(http, injections[spec.id], frozen)
            r = run_feed(spec, fleet, wh, client, last_runs, report.run_id, now)
            report.runs.append(r)
            log.info("feed %-22s %-9s rows=%-4d %5.1fs %s", spec.id, r["status"], r["rows_written"], r["duration_s"], r["error"] or "")

    if detect:
        at = now + timedelta(seconds=time.monotonic() - t0)
        for spec in specs:
            report.detections.extend(detect_feed(spec, wh, at))
        try:
            wh.append("detections", [d.as_row(report.run_id, at) for d in report.detections], DETECTIONS)
            open_f = wh.open_findings()
            cur_d = wh.latest_dispositions()
            ledger = reconcile(report.detections, open_f, cur_d, report.run_id, at)
            apply_ledger(ledger, wh)
        except Exception:  # noqa: BLE001 — no ledger => no heartbeat => the dead-man's switch shows it
            log.exception("detections/ledger write failed; no heartbeat this run (findings are append-only, nothing is lost)")
        else:
            report.ledger = ledger
            # the heartbeat means "the WHOLE loop completed", so it is written only after the ledger
            report.heartbeat = beat(wh, report.run_id, at, str(fleet.path))
            if agent_ctx is not None:
                _run_agent(fleet, wh, report, agent_ctx, at)
        for d in report.detections:
            log.info("detect %-22s %-13s %-7s %s", d.feed, d.axis, d.state, d.detail)

    report.duration_s = time.monotonic() - t0
    return report


def _run_agent(fleet: Fleet, wh: Warehouse, report: RunReport, agent_ctx, at: datetime) -> None:
    """The repair agent runs after the ledger + heartbeat, inside the same job. It gets this
    run's OPENED findings plus the DEFERRED ones, never anything else."""
    from .agent import Agent

    try:
        open_now = {(f["feed"], f["axis"]): f for f in wh.open_findings()}
        disps = wh.latest_dispositions()
        opened = [dict(o) for o in (report.ledger.opened if report.ledger else [])]
        opened_ids = {o["finding_id"] for o in opened}

        def queued(f: dict) -> bool:
            d = disps.get(f["finding_id"])
            if d is None:
                return True  # open with no disposition row at all: never looked at
            if d["disposition"] == "DEFERRED":
                return True  # the patrol re-queues deferred work
            # untriaged backlog: the auto 'awaiting triage' row the ledger writes on OPENED
            # (an agent-written UNKNOWN 'awaiting verification' is handled by verify_pending instead)
            return d["disposition"] == "UNKNOWN" and d["decided_by"] == "auto"

        backlog = [f for f in open_now.values() if f["finding_id"] not in opened_ids and queued(f)]
        Agent(wh, agent_ctx, report.run_id, at).run(opened, backlog, open_now)
    except Exception:  # noqa: BLE001 — the agent is downstream of everything that matters
        log.exception("agent stage failed")
