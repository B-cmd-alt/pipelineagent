"""Subprocess entry for replay. Blocks all sockets, then runs one feed's fetch() with an
HTTP stub that returns the archived bytes, loads the rows into SQLite and reports.
Prints exactly one JSON line on stdout."""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import sys
from datetime import datetime, timezone


def _block_network() -> None:
    def _blocked(*a, **k):
        raise RuntimeError("network disabled in replay")

    socket.socket.connect = _blocked  # type: ignore[assignment]
    socket.create_connection = _blocked  # type: ignore[assignment]
    socket.getaddrinfo = _blocked  # type: ignore[assignment]


class ReplayClient:
    """Stands in for http.Client: hands back archived bodies in request order."""

    def __init__(self, bodies: list[bytes]):
        self.bodies = list(bodies)
        self.used = 0
        self.budget = len(bodies)

    def get(self, url, *, accept="*/*", expect="json", params=None, retries=1):
        from pipelineagent.http import Response, SourceError, looks_like_html

        if not self.bodies:
            raise SourceError(f"replay: no archived body left for {url}")
        body = self.bodies.pop(0)
        self.used += 1
        ct = "text/html" if looks_like_html(body, "") else ("application/json" if expect == "json" else "text/csv")
        if expect in ("json", "csv") and looks_like_html(body, ct):
            raise SourceError(f"html_error_page: expected {expect}, got HTML ({len(body)} bytes) [replay]")
        if expect == "json" and body.lstrip()[:1] not in (b"{", b"["):
            raise SourceError(f"not_json: body starts with {body[:20]!r} [replay]")
        return Response(200, body, ct, None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fleet", required=True)
    ap.add_argument("--feed", required=True)
    ap.add_argument("--payload", required=True)
    a = ap.parse_args(sys.argv[1:])
    _block_network()
    from pipelineagent import feeds as reg
    from pipelineagent.config import load_fleet
    from pipelineagent.feeds.base import FetchContext
    from pipelineagent.runner import ensure_warehouse
    from pipelineagent.schema import FEED_COMMON
    from pipelineagent.warehouse.sqlite import SQLiteWarehouse
    from pipelineagent.agent.replay import load_payload_bodies

    fleet = load_fleet(a.fleet)
    spec = fleet.feed(a.feed)
    wh = SQLiteWarehouse(":memory:")
    ensure_warehouse(fleet, wh)
    bodies = load_payload_bodies(open(a.payload, "rb").read())
    now = datetime.now(timezone.utc)
    result = {"fetch_ok": False, "rows": 0, "error": None, "anomalies": [], "rows_digest": ""}
    try:
        feed = reg.get(spec.id)
        res = feed.fetch(FetchContext(ReplayClient(bodies), spec.params, None, wh, spec.table, now))
        rows = res.rows
        for r in rows:
            r.update(captured_at=now, run_id="replay", source_modified=None, content_sha256=res.content_sha256)
        wh.append(spec.table, rows, FEED_COMMON + feed.columns)
        canon = json.dumps([{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in sorted(r.items()) if k not in ("captured_at", "run_id")} for r in rows],
                           sort_keys=True, default=str)
        result.update(fetch_ok=True, rows=len(rows), anomalies=res.anomalies, rows_digest=hashlib.sha256(canon.encode()).hexdigest())
    except Exception as e:  # noqa: BLE001 — the whole point is to report what the fetcher does with these bytes
        result.update(fetch_ok=False, error=f"{e.__class__.__name__}: {str(e)[:500]}")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
