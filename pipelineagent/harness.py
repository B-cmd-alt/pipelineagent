"""Fault-injection harness: corrupt a feed's payload BETWEEN the HTTP layer and the parser, so the
rest of the system (archive, detectors, ledger, agent, gate, deploy, verify) sees exactly what a
real upstream change would produce. Ground truth lands in `harness_injections`, which the
benchmark joins against `findings` for MTTD and against `dispositions` for MTTR.

Activated by PIPELINEAGENT_INJECT="feed=kind[,feed=kind]" on the job. Kinds:
  schema_rename  a key/column the parser depends on is renamed (failure class 2, schema drift)
  stale_source   the upstream times out (class 3; nothing to patch, the agent must escalate)
  html_error     the upstream serves an HTML error page with HTTP 200 (class 2/3 boundary)
  frozen_payload the upstream keeps serving the last healthy bytes with HTTP 200 (class 1, green job /
                 dead data): a daily feed sees no_change runs until write_recency breaches; an hourly
                 append feed lands rows whose event_ts never moves until head_lag breaches. Nothing to patch.
Deploy drift (class 7) is not an injection: deploy an older image and watch the hash check.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

from .http import Response, SourceError
from .schema import Column

HARNESS_INJECTIONS = (
    Column("ts", "TIMESTAMP", True),
    Column("run_id", "STRING", True),
    Column("feed", "STRING", True),
    Column("kind", "STRING", True),
    Column("failure_class", "STRING", True),
)

# Per feed: the byte-level rename that breaks its parser the way a real upstream rename would.
RENAMES: dict[str, tuple[bytes, bytes]] = {
    "coingecko_prices": (b'"usd"', b'"usd_price"'),
    "fred_dgs10": (b"observation_date", b"DATE"),
    "ieso_ontario_demand": (b"Ontario Demand", b"Ontario Load"),
    "usgs_earthquakes_hour": (b'"count"', b'"feature_count"'),
    "github_status": (b'"updated_at"', b'"last_updated"'),
    "open_meteo_current": (b'"temperature_2m"', b'"temp_2m"'),
    "nws_observations": (b'"timestamp"', b'"observedAt"'),
}
FAILURE_CLASS = {"schema_rename": "schema_drift", "stale_source": "stale_source", "html_error": "stale_source",
                 "frozen_payload": "green_job_dead_data"}
HTML_ERROR = b"<!DOCTYPE html><html><head><title>503 Service Unavailable</title></head><body><h1>Service Unavailable</h1></body></html>"


@dataclass(frozen=True)
class Injection:
    feed: str
    kind: str

    @property
    def failure_class(self) -> str:
        return FAILURE_CLASS[self.kind]


def rules_from_env(value: str | None = None) -> dict[str, Injection]:
    raw = os.environ.get("PIPELINEAGENT_INJECT", "") if value is None else value
    out: dict[str, Injection] = {}
    for part in (p.strip() for p in raw.split(",") if p.strip()):
        feed, _, kind = part.partition("=")
        if kind not in FAILURE_CLASS:
            raise ValueError(f"unknown injection kind {kind!r} for {feed!r}; known: {sorted(FAILURE_CLASS)}")
        out[feed] = Injection(feed, kind)
    return out


class InjectingClient:
    """Wraps the real HTTP client for ONE feed. Same surface the fetchers use (get / used / budget /
    last_body); the injected body is what gets archived, which is exactly what should be replayed."""

    def __init__(self, inner, injection: Injection, frozen_bodies: list[bytes] | None = None):
        self.inner, self.injection = inner, injection
        self.last_body: bytes | None = None
        self.frozen = list(frozen_bodies or [])  # frozen_payload: the archived healthy bodies, served in request order

    @property
    def used(self) -> int:
        return self.inner.used

    @property
    def budget(self) -> int:
        return self.inner.budget

    def get(self, url, **kw) -> Response:
        if self.injection.kind == "stale_source":
            self.inner.used += 1
            raise SourceError("transport: ReadTimeout [injected by the harness]")
        if self.injection.kind == "frozen_payload":
            self.inner.used += 1
            if not self.frozen:
                raise SourceError("frozen_payload: no archived healthy body to serve [harness]")
            body = self.frozen.pop(0)
            self.last_body = self.inner.last_body = body
            ct = "application/json" if kw.get("expect", "json") == "json" else "text/csv"
            return Response(200, body, ct, None)
        r = self.inner.get(url, **kw)
        if self.injection.kind == "html_error":
            self.last_body = self.inner.last_body = HTML_ERROR
            raise SourceError(f"html_error_page: expected {kw.get('expect', 'json')}, got HTML ({len(HTML_ERROR)} bytes) [injected by the harness]")
        old, new = RENAMES.get(self.injection.feed, (b"", b""))
        body = r.body.replace(old, new) if old else r.body
        self.last_body = self.inner.last_body = body
        return Response(r.status, body, r.content_type, r.last_modified)


def record(wh, injections: dict[str, Injection], run_id: str, now: datetime) -> None:
    """One ground-truth row per active injection per run (the benchmark takes MIN(ts))."""
    if not injections:
        return
    wh.ensure_table("harness_injections", HARNESS_INJECTIONS, "ts", False, description="fault-injection ground truth (append-only)")
    wh.append("harness_injections", [{"ts": now, "run_id": run_id, "feed": i.feed, "kind": i.kind, "failure_class": i.failure_class}
                                     for i in injections.values()], HARNESS_INJECTIONS)
