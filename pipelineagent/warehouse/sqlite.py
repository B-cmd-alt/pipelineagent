"""SQLite backend: tests, offline demos, and the fault-injection harness.
Semantics mirror the BigQuery backend exactly (append-only, UTC)."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from ..schema import Column
from .base import FeedSnapshot, LastRun

_TYPE = {"STRING": "TEXT", "FLOAT64": "REAL", "INT64": "INTEGER", "TIMESTAMP": "TEXT", "DATE": "TEXT", "BOOL": "INTEGER", "JSON": "TEXT"}


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse(s: str | None) -> datetime | None:
    if s is None:
        return None
    return datetime.fromisoformat(s).astimezone(timezone.utc)


class SQLiteWarehouse:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._columns: dict[str, tuple[Column, ...]] = {}
        self.now_override: datetime | None = None  # tests can freeze "now"

    def _now(self) -> datetime:
        return self.now_override or datetime.now(timezone.utc)

    # --- DDL ---
    def ensure_table(self, table, columns, partition_column=None, require_partition_filter=False, description=""):
        cols = ", ".join(f'"{c.name}" {_TYPE[c.type]}' for c in columns)
        self.conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols})')
        self._columns[table] = columns
        self.conn.commit()

    def table_exists(self, table: str) -> bool:
        r = self.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return r is not None

    # --- writes ---
    def append(self, table, rows, columns):
        if not rows:
            return 0
        names = [c.name for c in columns]
        types = {c.name: c.type for c in columns}

        def conv(name: str, v: Any) -> Any:
            if v is None:
                return None
            t = types[name]
            if t == "TIMESTAMP":
                return _iso(v) if isinstance(v, datetime) else str(v)
            if t == "DATE":
                return v.isoformat() if hasattr(v, "isoformat") else str(v)
            if t == "JSON":
                return json.dumps(v) if not isinstance(v, str) else v
            if t == "BOOL":
                return int(bool(v))
            return v

        ph = ", ".join("?" for _ in names)
        self.conn.executemany(
            f'INSERT INTO "{table}" ({", ".join(chr(34)+n+chr(34) for n in names)}) VALUES ({ph})',
            [[conv(n, r.get(n)) for n in names] for r in rows],
        )
        self.conn.commit()
        return len(rows)

    # --- detector reads ---
    def feed_snapshot(self, table, lookback_hours, event_ts_column="event_ts", value_column=None, group_by=None, max_rows=0):
        since = _iso(self._now() - timedelta(hours=lookback_hours))
        row = self.conn.execute(
            f'SELECT MAX(captured_at) AS c, MAX("{event_ts_column}") AS e FROM "{table}" WHERE captured_at >= ?', (since,)
        ).fetchone()
        snap = FeedSnapshot(_parse(row["c"]), _parse(row["e"]))
        if value_column and max_rows:
            grp = f'"{group_by}"' if group_by else "''"
            cur = self.conn.execute(
                f'SELECT {grp} AS g, "{value_column}" AS v, captured_at FROM "{table}" WHERE captured_at >= ? '
                f'ORDER BY captured_at DESC, "{event_ts_column}" DESC LIMIT ?',
                (since, max_rows),
            )
            for r in cur:
                v = r["v"]
                try:
                    fv = None if v is None else float(v)
                except (TypeError, ValueError):
                    fv = None
                snap.recent_values.append((str(r["g"] or ""), fv, _parse(r["captured_at"])))
        return snap

    def max_event_ts(self, table, lookback_hours):
        since = _iso(self._now() - timedelta(hours=lookback_hours))
        r = self.conn.execute(f'SELECT MAX(event_ts) AS e FROM "{table}" WHERE captured_at >= ?', (since,)).fetchone()
        return _parse(r["e"])

    # --- ledger / runner reads ---
    def last_runs(self):
        out: dict[str, LastRun] = {}
        cur = self.conn.execute(
            "SELECT feed, finished_at, status, etag, content_sha256, source_modified FROM _runs r "
            "WHERE finished_at = (SELECT MAX(finished_at) FROM _runs WHERE feed = r.feed AND status IN ('ok','no_change','failed'))"
        )
        for r in cur:
            out[r["feed"]] = LastRun(r["feed"], _parse(r["finished_at"]), r["status"], r["etag"], r["content_sha256"], _parse(r["source_modified"]))
        return out

    def runs_since(self, since):
        cur = self.conn.execute("SELECT * FROM _runs WHERE started_at >= ? ORDER BY started_at DESC", (_iso(since),))
        return [self._row(r) for r in cur]

    def open_findings(self):
        cur = self.conn.execute(
            "SELECT * FROM findings f WHERE event_at = (SELECT MAX(event_at) FROM findings WHERE finding_id = f.finding_id) "
            "AND event != 'CLOSED' ORDER BY event_at DESC"
        )
        return [self._row(r) for r in cur]

    def recent_finding_events(self, limit):
        cur = self.conn.execute("SELECT * FROM findings ORDER BY event_at DESC LIMIT ?", (limit,))
        return [self._row(r) for r in cur]

    def latest_dispositions(self):
        cur = self.conn.execute(
            "SELECT * FROM dispositions d WHERE decided_at = (SELECT MAX(decided_at) FROM dispositions WHERE finding_id = d.finding_id)"
        )
        return {r["finding_id"]: self._row(r) for r in cur}

    def latest_heartbeat(self):
        r = self.conn.execute("SELECT * FROM heartbeat ORDER BY ts DESC LIMIT 1").fetchone()
        return self._row(r) if r else None

    def latest_detections(self):
        cur = self.conn.execute(
            "SELECT * FROM detections d WHERE detected_at = (SELECT MAX(detected_at) FROM detections WHERE feed = d.feed AND axis = d.axis)"
        )
        return [self._row(r) for r in cur]

    def _row(self, r: sqlite3.Row) -> dict[str, Any]:
        d = dict(r)
        for k, v in list(d.items()):
            if isinstance(v, str) and k.endswith(("_at", "_ts")) or k in ("ts",):
                try:
                    d[k] = _parse(v)
                except (ValueError, TypeError):
                    pass
        return d
