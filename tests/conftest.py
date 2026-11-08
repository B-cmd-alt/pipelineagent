import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipelineagent.config import load_fleet  # noqa: E402
from pipelineagent.runner import ensure_warehouse  # noqa: E402
from pipelineagent.warehouse.sqlite import SQLiteWarehouse  # noqa: E402

FIX = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def fleet():
    return load_fleet(ROOT / "fleet.yaml")


@pytest.fixture
def wh(fleet):
    w = SQLiteWarehouse(":memory:")
    w.now_override = NOW
    ensure_warehouse(fleet, w)
    return w


def fixture_text(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")
