from datetime import timedelta

from conftest import NOW

from pipelineagent.detectors import detect_feed, evaluate_snapshot, lookback_hours
from pipelineagent.warehouse.base import FeedSnapshot


def by_axis(dets):
    return {d.axis: d for d in dets}


def test_recency_and_head_lag_ok_and_breach(fleet):
    spec = fleet.feed("usgs_earthquakes_hour")  # 3h / 3h / flat 24
    fresh = FeedSnapshot(NOW - timedelta(minutes=30), NOW - timedelta(minutes=40), [("", float(i), NOW - timedelta(hours=i)) for i in range(30)])
    d = by_axis(evaluate_snapshot(spec, fresh, NOW))
    assert d["write_recency"].state == "OK" and d["head_lag"].state == "OK" and d["flat_value"].state == "OK"
    stale = FeedSnapshot(NOW - timedelta(hours=5), NOW - timedelta(hours=9), fresh.recent_values)
    d = by_axis(evaluate_snapshot(spec, stale, NOW))
    assert d["write_recency"].state == "BREACH" and d["write_recency"].observed == 5.0
    assert d["head_lag"].state == "BREACH" and d["head_lag"].observed == 9.0


def test_green_job_dead_data_is_a_write_recency_breach(fleet):
    """The job may exit 0 forever; if nothing was captured in the window it is a BREACH, not a blind spot."""
    spec = fleet.feed("github_status")
    d = by_axis(evaluate_snapshot(spec, FeedSnapshot(None, None, []), NOW))
    assert d["write_recency"].state == "BREACH"
    assert d["head_lag"].state == "UNKNOWN"  # nothing to read a head from — and that is not OK
    assert "flat_value" not in d  # undeclared axis emits nothing (rendered as not declared, never green)


def test_flat_value_breach_per_group_and_insufficient_history_is_unknown(fleet):
    spec = fleet.feed("coingecko_prices")  # flat on price_usd per coin, N=6
    vals = []
    for i in range(8):
        vals.append(("bitcoin", 75000.0, NOW - timedelta(hours=i)))   # frozen
        vals.append(("ethereum", 2200.0 + i, NOW - timedelta(hours=i)))  # moving
    d = by_axis(evaluate_snapshot(spec, FeedSnapshot(NOW, NOW, vals), NOW))
    assert d["flat_value"].state == "BREACH" and "bitcoin" in d["flat_value"].detail and "ethereum" not in d["flat_value"].detail
    short = [("bitcoin", 1.0 * i, NOW - timedelta(hours=i)) for i in range(3)]
    d = by_axis(evaluate_snapshot(spec, FeedSnapshot(NOW, NOW, short), NOW))
    assert d["flat_value"].state == "UNKNOWN" and "insufficient" in d["flat_value"].detail


def test_all_null_window_is_a_breach(fleet):
    spec = fleet.feed("nws_observations")
    vals = [("KOKC", None, NOW - timedelta(hours=i)) for i in range(12)] + [("KDFW", 10.0 + i, NOW - timedelta(hours=i)) for i in range(12)]
    d = by_axis(evaluate_snapshot(spec, FeedSnapshot(NOW, NOW, vals), NOW))
    assert d["flat_value"].state == "BREACH" and "all-NULL" in d["flat_value"].detail


def test_missing_table_and_read_failure_are_unknown_never_ok(fleet):
    class Broken:
        def table_exists(self, t):
            return True

        def feed_snapshot(self, *a, **k):
            raise RuntimeError("403 billing disabled")

    dets = detect_feed(fleet.feed("fred_dgs10"), Broken(), NOW)
    assert dets and all(d.state == "UNKNOWN" for d in dets)
    assert all("403" in d.detail for d in dets)

    class Missing(Broken):
        def table_exists(self, t):
            return False

    dets = detect_feed(fleet.feed("fred_dgs10"), Missing(), NOW)
    assert all(d.state == "UNKNOWN" and "does not exist" in d.detail for d in dets)


def test_lookback_bounds_every_read(fleet):
    for spec in fleet.feeds:
        lb = lookback_hours(spec)
        assert lb >= 72
        if spec.sla.write_recency_max_hours:
            assert lb >= 3 * spec.sla.write_recency_max_hours
