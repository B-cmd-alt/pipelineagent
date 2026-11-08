"""The warehouse interface the rest of the package talks to.

Deliberately narrow: detectors and the ledger never write raw SQL, so the
same logic runs against BigQuery in production and SQLite in tests.
Timestamps cross this boundary as timezone-aware UTC datetimes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from ..schema import Column


@dataclass
class FeedSnapshot:
    """Everything the three detectors need about one feed, from ONE query."""
    last_captured_at: datetime | None
    last_event_ts: datetime | None
    # newest first: (group, value, captured_at)
    recent_values: list[tuple[str, float | None, datetime]] = field(default_factory=list)


@dataclass
class LastRun:
    feed: str
    finished_at: datetime
    status: str
    etag: str | None
    content_sha256: str | None
    source_modified: datetime | None


class Warehouse(Protocol):
    def ensure_table(self, table: str, columns: tuple[Column, ...], partition_column: str | None, require_partition_filter: bool, description: str = "") -> None: ...
    def table_exists(self, table: str) -> bool: ...
    def append(self, table: str, rows: list[dict[str, Any]], columns: tuple[Column, ...]) -> int: ...

    # --- detector reads (data, never job status) ---
    def feed_snapshot(self, table: str, lookback_hours: float, event_ts_column: str = "event_ts",
                      value_column: str | None = None, group_by: str | None = None, max_rows: int = 0) -> FeedSnapshot: ...
    def max_event_ts(self, table: str, lookback_hours: float) -> datetime | None: ...

    # --- ledger / runner reads ---
    def last_runs(self) -> dict[str, LastRun]: ...
    def runs_since(self, since: datetime) -> list[dict[str, Any]]: ...
    def open_findings(self) -> list[dict[str, Any]]: ...
    def recent_finding_events(self, limit: int) -> list[dict[str, Any]]: ...
    def latest_dispositions(self) -> dict[str, dict[str, Any]]: ...
    def latest_heartbeat(self) -> dict[str, Any] | None: ...
    def latest_detections(self) -> list[dict[str, Any]]: ...
