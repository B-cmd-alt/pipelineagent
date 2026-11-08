"""Typed loader for fleet.yaml."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

AXES = ("write_recency", "head_lag", "flat_value")

DEFAULT_FLEET_PATH = Path(__file__).resolve().parent.parent / "fleet.yaml"


class ManifestError(ValueError):
    pass


@dataclass(frozen=True)
class FlatValueSLA:
    column: str
    max_consecutive: int
    group_by: str | None = None


@dataclass(frozen=True)
class FeedSLA:
    write_recency_max_hours: float | None
    head_lag_max_hours: float | None
    flat_value: FlatValueSLA | None

    def declared(self, axis: str) -> bool:
        return {
            "write_recency": self.write_recency_max_hours is not None,
            "head_lag": self.head_lag_max_hours is not None,
            "flat_value": self.flat_value is not None,
        }[axis]


@dataclass(frozen=True)
class FeedSpec:
    id: str
    description: str
    cadence: str  # hourly | daily
    table: str
    sla: FeedSLA
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def cadence_hours(self) -> int:
        return 1 if self.cadence == "hourly" else 24


@dataclass(frozen=True)
class Fleet:
    version: int
    project: str
    user_agent: str
    request_budget_per_run: int
    min_seconds_between_requests: float
    heartbeat_max_age_hours: float
    daily_feed_refetch_after_hours: float
    feeds: tuple[FeedSpec, ...]
    path: Path

    def feed(self, feed_id: str) -> FeedSpec:
        for f in self.feeds:
            if f.id == feed_id:
                return f
        raise KeyError(feed_id)


def _sla(raw: dict[str, Any], where: str) -> FeedSLA:
    if not isinstance(raw, dict):
        raise ManifestError(f"{where}.sla must be a mapping")
    fv = raw.get("flat_value")
    flat = None
    if fv is not None:
        if not isinstance(fv, dict) or "column" not in fv or "max_consecutive" not in fv:
            raise ManifestError(f"{where}.sla.flat_value needs column + max_consecutive")
        if int(fv["max_consecutive"]) < 2:
            raise ManifestError(f"{where}.sla.flat_value.max_consecutive must be >= 2")
        flat = FlatValueSLA(str(fv["column"]), int(fv["max_consecutive"]), fv.get("group_by"))
    for k in ("write_recency_max_hours", "head_lag_max_hours"):
        v = raw.get(k)
        if v is not None and float(v) <= 0:
            raise ManifestError(f"{where}.sla.{k} must be positive")
    return FeedSLA(
        write_recency_max_hours=None if raw.get("write_recency_max_hours") is None else float(raw["write_recency_max_hours"]),
        head_lag_max_hours=None if raw.get("head_lag_max_hours") is None else float(raw["head_lag_max_hours"]),
        flat_value=flat,
    )


def load_fleet(path: str | os.PathLike | None = None) -> Fleet:
    p = Path(path or os.environ.get("PIPELINEAGENT_FLEET") or DEFAULT_FLEET_PATH)
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise ManifestError("fleet.yaml must be a mapping with version: 1")
    feeds: list[FeedSpec] = []
    seen: set[str] = set()
    for i, f in enumerate(raw.get("feeds") or []):
        where = f"feeds[{i}]"
        for k in ("id", "cadence", "table", "sla"):
            if k not in f:
                raise ManifestError(f"{where} missing {k}")
        if f["cadence"] not in ("hourly", "daily"):
            raise ManifestError(f"{where}.cadence must be hourly|daily")
        if f["id"] in seen:
            raise ManifestError(f"duplicate feed id {f['id']!r}")
        seen.add(f["id"])
        feeds.append(
            FeedSpec(
                id=str(f["id"]),
                description=str(f.get("description", "")),
                cadence=f["cadence"],
                table=str(f["table"]),
                sla=_sla(f["sla"], where),
                params=dict(f.get("params") or {}),
            )
        )
    if not feeds:
        raise ManifestError("fleet.yaml declares no feeds")
    return Fleet(
        version=1,
        project=str(raw.get("project", "PipelineAgent")),
        user_agent=str(raw.get("user_agent") or "PipelineAgent/0.1 (automated monitor)"),
        request_budget_per_run=int(raw.get("request_budget_per_run", 40)),
        min_seconds_between_requests=float(raw.get("min_seconds_between_requests", 1.0)),
        heartbeat_max_age_hours=float(raw.get("heartbeat_max_age_hours", 26)),
        daily_feed_refetch_after_hours=float(raw.get("daily_feed_refetch_after_hours", 20)),
        feeds=tuple(feeds),
        path=p,
    )
