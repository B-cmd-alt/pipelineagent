"""Provider-friendly HTTP: identifying User-Agent, per-host pacing, a hard
per-run request budget, conditional GET, and detection of HTML error pages
served with a 200 where JSON/CSV was expected.

Benign-404 is a *source* outcome, not a crash: callers get `NotFound` and
record a `skipped` run.
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30


class SourceError(RuntimeError):
    """The upstream answered, but not with usable data."""


class NotFound(SourceError):
    pass


class RateLimited(SourceError):
    pass


class BudgetExhausted(RuntimeError):
    pass


@dataclass
class Response:
    status: int
    body: bytes
    content_type: str
    etag: str | None
    last_modified: datetime | None
    not_modified: bool
    elapsed_s: float

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()

    def text(self) -> str:
        return self.body.decode("utf-8-sig", errors="replace")


def looks_like_html(body: bytes, content_type: str) -> bool:
    if "text/html" in (content_type or "").lower():
        return True
    head = body[:512].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")


class Client:
    def __init__(self, user_agent: str, budget: int, min_interval_s: float = 1.0, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = user_agent
        self.budget = budget
        self.used = 0
        self.min_interval_s = min_interval_s
        self._last_by_host: dict[str, float] = {}

    def _pace(self, host: str) -> None:
        last = self._last_by_host.get(host)
        if last is not None:
            wait = self.min_interval_s - (time.monotonic() - last)
            if wait > 0:
                time.sleep(wait)
        self._last_by_host[host] = time.monotonic()

    def get(
        self,
        url: str,
        *,
        accept: str = "*/*",
        etag: str | None = None,
        last_modified: str | None = None,
        expect: str = "json",  # json | csv | any
        params: dict | None = None,
        retries: int = 1,
    ) -> Response:
        if self.used >= self.budget:
            raise BudgetExhausted(f"request budget {self.budget} exhausted")
        headers = {"Accept": accept}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        host = urlsplit(url).netloc
        attempt = 0
        while True:
            self._pace(host)
            self.used += 1
            t0 = time.monotonic()
            try:
                r = self.session.get(url, headers=headers, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            except requests.RequestException as e:
                if attempt < retries:
                    attempt += 1
                    time.sleep(2.0 * attempt)
                    continue
                raise SourceError(f"transport: {e.__class__.__name__}: {e}") from e
            elapsed = time.monotonic() - t0
            if r.status_code >= 500 and attempt < retries:
                attempt += 1
                time.sleep(2.0 * attempt)
                continue
            break
        ct = r.headers.get("Content-Type", "")
        lm = None
        if r.headers.get("Last-Modified"):
            try:
                lm = parsedate_to_datetime(r.headers["Last-Modified"]).astimezone(timezone.utc)
            except (TypeError, ValueError):
                lm = None
        if r.status_code == 304:
            return Response(304, b"", ct, r.headers.get("ETag") or etag, lm, True, elapsed)
        if r.status_code == 404:
            raise NotFound(f"404 for {url}")
        if r.status_code == 429:
            raise RateLimited(f"429 for {host}; Retry-After={r.headers.get('Retry-After')}")
        if r.status_code >= 400:
            raise SourceError(f"HTTP {r.status_code} for {url}")
        body = r.content
        if expect in ("json", "csv") and looks_like_html(body, ct):
            raise SourceError(f"html_error_page: expected {expect}, got HTML ({len(body)} bytes) from {host}")
        if expect == "json" and not body.lstrip()[:1] in (b"{", b"["):
            raise SourceError(f"not_json: body starts with {body[:20]!r}")
        return Response(r.status_code, body, ct, r.headers.get("ETag"), lm, False, elapsed)
