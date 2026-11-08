import json
from datetime import datetime, timezone, date

from conftest import fixture_text

from pipelineagent.feeds import coingecko_prices, fred_dgs10, github_status, ieso_ontario_demand, open_meteo_current, usgs_earthquakes_hour
from pipelineagent.http import looks_like_html


def test_open_meteo_parse():
    pts = [{"name": "oklahoma_city", "latitude": 35.47, "longitude": -97.52}]
    rows, anomalies = open_meteo_current.parse(json.loads(fixture_text("open_meteo.json")), pts)
    assert rows[0]["location"] == "oklahoma_city" and rows[0]["event_ts"].tzinfo is not None
    assert isinstance(rows[0]["temperature_2m"], float) and anomalies == []


def test_usgs_parse():
    row = usgs_earthquakes_hour.parse(json.loads(fixture_text("usgs_all_hour.json")))
    assert row["feature_count"] >= 0 and row["event_ts"].tzinfo == timezone.utc


def test_github_status_parse():
    row = github_status.parse(json.loads(fixture_text("github_summary.json")))
    assert row["status_indicator"] in ("none", "minor", "major", "critical") and row["components_total"] > 0


def test_coingecko_parse_and_missing_coin_is_an_anomaly():
    rows, anomalies = coingecko_prices.parse(json.loads(fixture_text("coingecko.json")), ["bitcoin", "ethereum", "dogecoin"])
    assert {r["coin"] for r in rows} == {"bitcoin", "ethereum"} and anomalies == ["dogecoin: missing from response"]


def test_fred_parse_keeps_dots_as_null_and_flags_them():
    text = fixture_text("fredgraph_tail.csv").rstrip("\n") + "\n2026-11-10,.\n"
    rows, anomalies = fred_dgs10.parse(text, "DGS10", 10)
    assert len(rows) == 10 and rows[-1]["value"] is None and rows[-1]["raw_value"] == "."
    assert any("missing" in a for a in anomalies)
    assert rows[-1]["observation_date"] == date(2026, 11, 10)


def test_fred_schema_rename_fails_loudly():
    import pytest

    with pytest.raises(ValueError, match="schema"):
        fred_dgs10.parse("DATE,VALUE\n2026-01-01,4.0\n", "DGS10", 5)


def test_ieso_parse_fixed_est_and_incremental():
    rows, anomalies = ieso_ontario_demand.parse(fixture_text("ieso_demand_tail.csv"), after=None)
    assert rows and rows[-1]["event_ts"].tzinfo == timezone.utc
    # HE1 on 2026-11-07 in fixed EST (UTC-5) ends 01:00 EST = 06:00Z
    he1 = [r for r in rows if r["local_date"] == date(2026, 11, 7) and r["hour_ending"] == 1][0]
    assert he1["event_ts"] == datetime(2026, 11, 7, 6, 0, tzinfo=timezone.utc)
    # IESO does NOT observe DST: a July hour-ending converts with the same -5h offset as a January one
    assert ieso_ontario_demand.hour_ending_to_utc(date(2026, 7, 1), 24) == datetime(2026, 7, 2, 5, 0, tzinfo=timezone.utc)
    assert ieso_ontario_demand.hour_ending_to_utc(date(2026, 1, 1), 24) == datetime(2026, 1, 2, 5, 0, tzinfo=timezone.utc)
    # the fixture's partial first day is older than the newest day -> flagged as not having 24 labels
    assert any("hour-ending labels" in a for a in anomalies)
    # incremental: nothing older than the newest stored event_ts comes back
    newest = max(r["event_ts"] for r in rows)
    rows2, _ = ieso_ontario_demand.parse(fixture_text("ieso_demand_tail.csv"), after=newest)
    assert rows2 == []


def test_html_error_page_detection():
    assert looks_like_html(b"<!DOCTYPE html><html>...", "text/plain")
    assert looks_like_html(b"{}", "text/html; charset=utf-8")
    assert not looks_like_html(b'{"a": 1}', "application/json")
