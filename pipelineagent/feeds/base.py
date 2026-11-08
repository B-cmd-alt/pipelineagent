"""Feed contract.

A feed is a small, stateless fetcher: given a context (HTTP client, params,
what we knew from the last run) it returns rows + provenance. It never writes;
the runner does, so every feed shares one append-only, audited write path.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from ..http import Client
from ..schema import Column
from ..warehouse.base import LastRun, Warehouse


@dataclass
class FetchContext:
    http: Client
    params: dict[str, Any]
    last_run: LastRun | None
    warehouse: Warehouse
    table: str
    now: datetime


@dataclass
class FetchResult:
    rows: list[dict[str, Any]]
    content_sha256: str | None
    source_modified: datetime | None
    http_status: int | None
    note: str = ""
    # Parsers can report soft anomalies (e.g. 'N/A' coerced to NULL) without failing.
    anomalies: list[str] = field(default_factory=list)
    # Raw upstream bytes, archived by the runner under raw/<feed>/<content_sha256> when a
    # payload archive is configured: the replay input for a future repair agent.
    raw: bytes | None = None


@dataclass(frozen=True)
class Feed:
    id: str
    columns: tuple[Column, ...]  # feed-specific columns (common ones are added by the runner)
    fetch: Callable[[FetchContext], FetchResult]


def to_float(v: Any, anomalies: list[str], label: str) -> float | None:
    """Numeric coercion that records, rather than hides, bad upstream cells."""
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        anomalies.append(f"{label}: non-numeric {v!r} -> NULL")
        return None
    if not math.isfinite(f):  # NaN/inf is not valid JSON for the BigQuery load job
        anomalies.append(f"{label}: non-finite {v!r} -> NULL")
        return None
    return f
