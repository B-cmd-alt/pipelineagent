"""SQLite backend: tests, offline demos, and the fault-injection harness.
Semantics mirror the BigQuery backend exactly (append-only, UTC)."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from ..schema import Column
from .base import FeedSnapshot, LastRun

_TYPE = {"STRING": "TEXT", "FLOAT64": "REAL", "INT64": "INTEGER", "TIMESTAMP": "TEXT", "DATE": "TEXT"}


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
            return v

        ph = ", ".join("?" for _ in names)
        self.conn.executemany(
            f'INSERT INTO "{table}" ({", ".join(chr(34)+n+chr(34) for n in names)}) VALUES ({ph})',
            [[conv(n, r.get(n)) for n in names] for r in rows],
        )
        self.conn.commit()
        return len(rows)

    # --- detector reads ---
    def feed_snapshot(self, table, lookback_hours, value_column=None, group_by=None, max_rows=0):
        since = _iso(self._now() - timedelta(hours=lookback_hours))
        row = self.conn.execute(
            f'SELECT MAX(captured_at) AS c, MAX(event_ts) AS e FROM "{table}" WHERE captured_at >= ?', (since,)
        ).fetchone()
        snap = FeedSnapshot(_parse(row["c"]), _parse(row["e"]))
        if value_column and max_rows:
            grp = f'"{group_by}"' if group_by else "''"
            cur = self.conn.execute(
                f'SELECT {grp} AS g, "{value_column}" AS v, captured_at FROM "{table}" WHERE captured_at >= ? '
                f'ORDER BY captured_at DESC, event_ts DESC LIMIT ?',
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

    # --- ledger / runner reads ---
    def last_runs(self):
        out: dict[str, LastRun] = {}
        cur = self.conn.execute(
            "SELECT feed, finished_at, status, content_sha256 FROM _runs r "
            "WHERE finished_at = (SELECT MAX(finished_at) FROM _runs WHERE feed = r.feed AND status IN ('ok','no_change','failed'))"
        )
        for r in cur:
            out[r["feed"]] = LastRun(r["feed"], _parse(r["finished_at"]), r["status"], r["content_sha256"])
        return out

    def runs_since(self, since):
        cur = self.conn.execute("SELECT * FROM _runs WHERE started_at >= ? ORDER BY started_at DESC", (_iso(since),))
        return [self._row(r) for r in cur]

    def open_findings(self):
        cur = self.conn.execute(
            "SELECT f.*, (SELECT MIN(event_at) FROM findings WHERE finding_id = f.finding_id) AS opened_at FROM findings f "
            "WHERE event_at = (SELECT MAX(event_at) FROM findings WHERE finding_id = f.finding_id) "
            "AND event = 'OPENED' ORDER BY event_at DESC"
        )
        return [self._row(r) for r in cur]

    def ledger_events(self, limit):
        cur = self.conn.execute(
            "SELECT * FROM ("
            "  SELECT event_at AS ts, event AS kind, finding_id, feed, axis, state AS value, detail, 'detector' AS actor FROM findings"
            "  UNION ALL"
            "  SELECT decided_at, 'DISPOSITION', finding_id, NULL, NULL, disposition, note, decided_by FROM dispositions"
            ") ORDER BY ts DESC LIMIT ?", (limit,))
        return [self._row(r) for r in cur][::-1]

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
