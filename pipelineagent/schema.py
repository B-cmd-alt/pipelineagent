"""Table schemas. Types are a tiny portable subset: STRING, FLOAT64, INT64, TIMESTAMP, DATE."""
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
    Column("event", "STRING", True, "OPENED | CLOSED (transitions only; the hourly state lives in detections)"),
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

# One row per COMPLETED loop (fetch + detect + ledger). Counts are derivable from _runs and
# detections by run_id, so they are not repeated here.
HEARTBEAT = (
    Column("ts", "TIMESTAMP", True),
    Column("run_id", "STRING", True),
    Column("code_sha256", "STRING", True, "Runtime hash of the deployed package; compare with `python -m pipelineagent version`."),
    Column("git_commit", "STRING"),
)

# The repair agent's chain, one row per step per finding (append-only). The dashboard card
# renders these rows in order: triage -> replay -> patch -> pr -> gate -> merge -> deploy -> verify.
AGENT_STEPS = (
    Column("finding_id", "STRING", True),
    Column("ts", "TIMESTAMP", True),
    Column("run_id", "STRING", True),
    Column("attempt", "INT64", True, "1-based repair attempt on this finding"),
    Column("step", "STRING", True, "triage | replay | patch | pr | gate | merge | deploy | verify | revert | escalate | defer"),
    Column("outcome", "STRING", True, "ok | failed | skipped | dry_run"),
    Column("detail", "STRING"),
    Column("ref", "STRING", False, "PR/issue URL, commit sha, or artifact path"),
)

SYSTEM_TABLES: dict[str, tuple[Column, ...]] = {
    "_runs": RUNS,
    "detections": DETECTIONS,
    "findings": FINDINGS,
    "dispositions": DISPOSITIONS,
    "heartbeat": HEARTBEAT,
    "agent_steps": AGENT_STEPS,
}

# Partition column per system table (DAY partitioning in BigQuery).
SYSTEM_PARTITION = {
    "_runs": "started_at",
    "detections": "detected_at",
    "findings": "event_at",
    "dispositions": "decided_at",
    "heartbeat": "ts",
    "agent_steps": "ts",
}

DISPOSITION_VALUES = ("FIXED", "BENIGN", "ESCALATED", "DEFERRED", "UNKNOWN")
