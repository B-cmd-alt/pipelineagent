"""BigQuery backend.

Cost posture: append-only LOAD jobs (free), DAY partitioning on the capture
column, `require_partition_filter` on feed tables so every detector read is
bounded, and ONE query per feed per run (`feed_snapshot`). No staging tables
are ever created, so there is nothing to leak.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..schema import Column
from .base import FeedSnapshot, LastRun

_BQ_TYPE = {"STRING": "STRING", "FLOAT64": "FLOAT64", "INT64": "INT64", "TIMESTAMP": "TIMESTAMP", "DATE": "DATE", "BOOL": "BOOL", "JSON": "STRING"}


def _ident(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier {name!r}")
    return f"`{name}`"


class BigQueryWarehouse:
    def __init__(self, project: str, dataset: str, location: str = "US", max_bytes_billed: int = 1 << 30):
        from google.cloud import bigquery  # lazy: optional dependency

        self.bq = bigquery
        self.client = bigquery.Client(project=project)
        self.project = project
        self.dataset = dataset
        self.location = location
        self.max_bytes_billed = max_bytes_billed  # 1 GiB hard ceiling per query; tables are KB-MB sized

    # --- helpers ---
    def _ref(self, table: str) -> str:
        return f"`{self.project}.{self.dataset}.{table}`"

    def _query(self, sql: str, params: list | None = None) -> list[dict[str, Any]]:
        cfg = self.bq.QueryJobConfig(maximum_bytes_billed=self.max_bytes_billed, query_parameters=params or [])
        job = self.client.query(sql, job_config=cfg, location=self.location)
        return [dict(r) for r in job.result()]

    def _schema(self, columns: tuple[Column, ...]):
        return [
            self.bq.SchemaField(c.name, _BQ_TYPE[c.type], mode="REQUIRED" if c.required else "NULLABLE", description=c.description or None)
            for c in columns
        ]

    # --- DDL ---
    def ensure_dataset(self, description: str = "") -> None:
        ds = self.bq.Dataset(f"{self.project}.{self.dataset}")
        ds.location = self.location
        ds.description = description or None
        self.client.create_dataset(ds, exists_ok=True)

    def table_exists(self, table: str) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            self.client.get_table(f"{self.project}.{self.dataset}.{table}")
            return True
        except NotFound:
            return False

    def ensure_table(self, table, columns, partition_column=None, require_partition_filter=False, description=""):
        if self.table_exists(table):
            return
        t = self.bq.Table(f"{self.project}.{self.dataset}.{table}", schema=self._schema(columns))
        if partition_column:
            t.time_partitioning = self.bq.TimePartitioning(type_=self.bq.TimePartitioningType.DAY, field=partition_column)
            t.require_partition_filter = require_partition_filter
        t.description = description or None
        self.client.create_table(t)

    # --- writes: LOAD jobs, append-only ---
    def append(self, table, rows, columns):
        if not rows:
            return 0
        types = {c.name: c.type for c in columns}

        def conv(name: str, v: Any) -> Any:
            if v is None:
                return None
            t = types[name]
            if t == "TIMESTAMP" and isinstance(v, datetime):
                if v.tzinfo is None:
                    v = v.replace(tzinfo=timezone.utc)
                return v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            if t == "DATE" and hasattr(v, "isoformat"):
                return v.isoformat()
            return v

        payload = [{c.name: conv(c.name, r.get(c.name)) for c in columns} for r in rows]
        cfg = self.bq.LoadJobConfig(
            schema=self._schema(columns),
            write_disposition=self.bq.WriteDisposition.WRITE_APPEND,
            source_format=self.bq.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = self.client.load_table_from_json(payload, f"{self.project}.{self.dataset}.{table}", job_config=cfg, location=self.location)
        job.result()
        return len(rows)

    # --- detector reads ---
    def feed_snapshot(self, table, lookback_hours, event_ts_column="event_ts", value_column=None, group_by=None, max_rows=0):
        grp = _ident(group_by) if group_by else "''"
        val = f"SAFE_CAST({_ident(value_column)} AS FLOAT64)" if value_column else "CAST(NULL AS FLOAT64)"
        sql = f"""
        WITH recent AS (
          SELECT captured_at, {_ident(event_ts_column)} AS event_ts, CAST({grp} AS STRING) AS g, {val} AS v
          FROM {self._ref(table)}
          WHERE captured_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback HOUR)
        )
        SELECT
          (SELECT MAX(captured_at) FROM recent) AS c,
          (SELECT MAX(event_ts) FROM recent) AS e,
          ARRAY(SELECT AS STRUCT g, v, captured_at FROM recent ORDER BY captured_at DESC, event_ts DESC LIMIT @n) AS vals
        """
        rows = self._query(sql, [
            self.bq.ScalarQueryParameter("lookback", "INT64", int(lookback_hours)),
            self.bq.ScalarQueryParameter("n", "INT64", int(max_rows) if value_column else 0),
        ])
        r = rows[0]
        snap = FeedSnapshot(r["c"], r["e"])
        for v in r["vals"] or []:
            snap.recent_values.append((v["g"] or "", v["v"], v["captured_at"]))
        return snap

    def max_event_ts(self, table, lookback_hours):
        rows = self._query(
            f"SELECT MAX(event_ts) AS e FROM {self._ref(table)} WHERE captured_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback HOUR)",
            [self.bq.ScalarQueryParameter("lookback", "INT64", int(lookback_hours))],
        )
        return rows[0]["e"] if rows else None

    # --- ledger / runner reads ---
    def last_runs(self):
        sql = f"""
        SELECT feed, finished_at, status, etag, content_sha256, source_modified FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY feed ORDER BY finished_at DESC) AS rn
          FROM {self._ref('_runs')} WHERE status IN ('ok','no_change','failed')
        ) WHERE rn = 1"""
        return {r["feed"]: LastRun(r["feed"], r["finished_at"], r["status"], r["etag"], r["content_sha256"], r["source_modified"]) for r in self._query(sql)}

    def runs_since(self, since):
        return self._query(
            f"SELECT * FROM {self._ref('_runs')} WHERE started_at >= @since ORDER BY started_at DESC",
            [self.bq.ScalarQueryParameter("since", "TIMESTAMP", since)],
        )

    def open_findings(self):
        sql = f"""
        SELECT * EXCEPT(rn) FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY finding_id ORDER BY event_at DESC) AS rn FROM {self._ref('findings')}
        ) WHERE rn = 1 AND event != 'CLOSED' ORDER BY event_at DESC"""
        return self._query(sql)

    def recent_finding_events(self, limit):
        return self._query(f"SELECT * FROM {self._ref('findings')} ORDER BY event_at DESC LIMIT {int(limit)}")

    def latest_dispositions(self):
        sql = f"""
        SELECT * EXCEPT(rn) FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY finding_id ORDER BY decided_at DESC) AS rn FROM {self._ref('dispositions')}
        ) WHERE rn = 1"""
        return {r["finding_id"]: r for r in self._query(sql)}

    def latest_heartbeat(self):
        rows = self._query(f"SELECT * FROM {self._ref('heartbeat')} ORDER BY ts DESC LIMIT 1")
        return rows[0] if rows else None

    def latest_detections(self):
        sql = f"""
        SELECT * EXCEPT(rn) FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY feed, axis ORDER BY detected_at DESC) AS rn
          FROM {self._ref('detections')}
          WHERE detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
        ) WHERE rn = 1"""
        return self._query(sql)
