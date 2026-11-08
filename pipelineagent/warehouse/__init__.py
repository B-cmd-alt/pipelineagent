"""Storage backends behind one small interface (see base.py)."""
from __future__ import annotations

import os

from .base import Warehouse


def open_warehouse(url: str | None = None) -> Warehouse:
    """`sqlite:///path.db` (or `sqlite://:memory:`) or `bigquery://project/dataset`.
    Defaults to $PIPELINEAGENT_WAREHOUSE."""
    url = url or os.environ.get("PIPELINEAGENT_WAREHOUSE") or "sqlite:///pipelineagent.db"
    if url.startswith("sqlite://"):
        from .sqlite import SQLiteWarehouse

        path = url[len("sqlite://"):]
        if path.startswith("/") and not path.startswith("//") and path != "/:memory:":
            path = path  # sqlite:///abs/path -> /abs/path ; sqlite://rel -> rel
        if path in ("/:memory:", ":memory:"):
            path = ":memory:"
        return SQLiteWarehouse(path)
    if url.startswith("bigquery://"):
        from .bigquery import BigQueryWarehouse

        rest = url[len("bigquery://"):]
        project, _, dataset = rest.partition("/")
        if not project or not dataset:
            raise ValueError("bigquery://<project>/<dataset>")
        return BigQueryWarehouse(project, dataset)
    raise ValueError(f"unknown warehouse url {url!r}")
