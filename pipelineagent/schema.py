"""Table schemas. Types are a tiny portable subset: STRING, FLOAT64, INT64,
TIMESTAMP, DATE, BOOL, JSON (stored as STRING text in SQLite)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool = False
    description: str = ""


# Columns every feed table carries, in this order, before its own columns.
FEED_COMMON = (
    Column("captured_at", "TIMESTAMP", True, "When this vintage was captured (append-only; never overwritten)."),
    Column("run_id", "STRING", True, "Loop run that wrote the row."),
    Column("event_ts", "TIMESTAMP", False, "The data's own clock: when the observation is for."),
    Column("source_modified", "TIMESTAMP", False, "Upstream Last-Modified header when provided."),
    Column("content_sha256", "STRING", False, "SHA-256 of the raw upstream payload the row came from."),
)

RUNS = (
    Column("run_id", "STRING", True),
    Column("feed", "STRING", True),
    Column("started_at", "TIMESTAMP", True),
    Column("finished_at", "TIMESTAMP", True),
    Column("status", "STRING", True, "ok | no_change | failed | skipped"),
    Column("reason", "STRING", False, "skipped/failed reason class"),
    Column("rows_written", "INT64", True),
    Column("duration_s", "FLOAT64", True),
    Column("http_status", "INT64"),
    Column("requests_used", "INT64"),
    Column("source_modified", "TIMESTAMP"),
    Column("content_sha256", "STRING"),
    Column("etag", "STRING"),
    Column("error", "STRING"),
    Column("code_sha256", "STRING", True),
)

DETECTIONS = (
    Column("run_id", "STRING", True),
    Column("detected_at", "TIMESTAMP", True),
    Column("feed", "STRING", True),
    Column("axis", "STRING", True, "write_recency | head_lag | flat_value"),
    Column("state", "STRING", True, "OK | BREACH | UNKNOWN"),
    Column("observed", "FLOAT64", False, "observed hours (recency/head) or run length (flat)"),
    Column("threshold", "FLOAT64"),
    Column("detail", "STRING"),
)

FINDINGS = (
    Column("finding_id", "STRING", True),
    Column("event", "STRING", True, "OPENED | STILL_OPEN | CLOSED"),
    Column("event_at", "TIMESTAMP", True),
    Column("run_id", "STRING", True),
    Column("feed", "STRING", True),
    Column("axis", "STRING", True),
    Column("state", "STRING", True, "BREACH | UNKNOWN (OK on CLOSED)"),
    Column("observed", "FLOAT64"),
    Column("threshold", "FLOAT64"),
    Column("detail", "STRING"),
)

DISPOSITIONS = (
    Column("finding_id", "STRING", True),
    Column("disposition", "STRING", True, "FIXED | BENIGN | ESCALATED | DEFERRED | UNKNOWN"),
    Column("decided_at", "TIMESTAMP", True),
    Column("decided_by", "STRING", True, "auto | human:<name> | agent"),
    Column("note", "STRING"),
    Column("evidence_ref", "STRING", False, "verdict/evidence JSON reference once the repair agent exists"),
)

HEARTBEAT = (
    Column("ts", "TIMESTAMP", True),
    Column("run_id", "STRING", True),
    Column("code_sha256", "STRING", True),
    Column("git_commit", "STRING"),
    Column("feeds_attempted", "INT64"),
    Column("feeds_ok", "INT64"),
    Column("breaches", "INT64"),
    Column("unknowns", "INT64"),
    Column("duration_s", "FLOAT64"),
)

SYSTEM_TABLES: dict[str, tuple[Column, ...]] = {
    "_runs": RUNS,
    "detections": DETECTIONS,
    "findings": FINDINGS,
    "dispositions": DISPOSITIONS,
    "heartbeat": HEARTBEAT,
}

# Partition column per system table (DAY partitioning in BigQuery).
SYSTEM_PARTITION = {
    "_runs": "started_at",
    "detections": "detected_at",
    "findings": "event_at",
    "dispositions": "decided_at",
    "heartbeat": "ts",
}

DISPOSITION_VALUES = ("FIXED", "BENIGN", "ESCALATED", "DEFERRED", "UNKNOWN")
