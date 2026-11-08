import pytest

from pipelineagent.config import AXES, ManifestError, load_fleet
from pipelineagent import feeds


def test_fleet_loads_and_every_feed_has_a_fetcher(fleet):
    assert 6 <= len(fleet.feeds) <= 8
    for spec in fleet.feeds:
        feeds.get(spec.id)
        assert spec.sla.declared("write_recency") and spec.sla.declared("head_lag"), spec.id


def test_axes_are_three():
    assert AXES == ("write_recency", "head_lag", "flat_value")


def test_manifest_rejects_bad_sla(tmp_path):
    p = tmp_path / "f.yaml"
    p.write_text("version: 1\nfeeds:\n  - {id: a, cadence: hourly, table: a, sla: {write_recency_max_hours: -1}}\n")
    with pytest.raises(ManifestError):
        load_fleet(p)
    p.write_text("version: 1\nfeeds:\n  - {id: a, cadence: hourly, table: a, sla: {flat_value: {column: x, max_consecutive: 1}}}\n")
    with pytest.raises(ManifestError):
        load_fleet(p)
