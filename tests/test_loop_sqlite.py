"""End-to-end against SQLite with stubbed fetchers: the green-job-dead-data
scenario, UNKNOWN never rendering green, heartbeat staleness."""
from datetime import timedelta

from conftest import NOW

from pipelineagent import feeds as reg
from pipelineagent.feeds.base import Feed, FetchResult
from pipelineagent.heartbeat import status
from pipelineagent.render_site import collect, render_html
from pipelineagent.runner import run


class StubHttp:
    used = 0


def stub_all(monkeypatch, make_result):
    for fid, feed in list(reg.REGISTRY.items()):
        monkeypatch.setitem(reg.REGISTRY, fid, Feed(fid, feed.columns, lambda ctx, fid=fid: make_result(fid, ctx)))


def healthy_result(fid, ctx):
    rows = [{"event_ts": ctx.now - timedelta(minutes=10)}]
    if fid == "nws_observations":
        rows = [{"station": s, "event_ts": ctx.now - timedelta(minutes=10), "temperature_c": 5.0} for s in ctx.params["stations"]]
    if fid == "coingecko_prices":
        rows = [{"coin": c, "event_ts": ctx.now, "price_usd": 1.0} for c in ctx.params["coins"]]
    if fid == "open_meteo_current":
        rows = [{"location": p["name"], "event_ts": ctx.now, "temperature_2m": 1.0} for p in ctx.params["points"]]
    if fid == "fred_dgs10":
        rows = [{"series_id": "DGS10", "observation_date": ctx.now.date(), "event_ts": ctx.now - timedelta(hours=20), "value": 4.1}]
    if fid == "ieso_ontario_demand":
        rows = [{"local_date": ctx.now.date(), "hour_ending": 1, "event_ts": ctx.now - timedelta(hours=1), "ontario_demand": 15000.0}]
    return FetchResult(rows, f"sha-{fid}-{ctx.now.isoformat()}", None, 200)


def test_full_loop_then_green_job_dead_data(fleet, wh, monkeypatch):
    stub_all(monkeypatch, healthy_result)
    rep = run(fleet, wh, http=StubHttp(), now=NOW)
    assert all(r["status"] == "ok" for r in rep.runs) and len(rep.runs) == len(fleet.feeds)
    states = {(d.feed, d.axis): d.state for d in rep.detections}
    assert states[("usgs_earthquakes_hour", "write_recency")] == "OK"
    assert states[("usgs_earthquakes_hour", "head_lag")] == "OK"
    # first run: flat-value has one row per group -> insufficient history -> UNKNOWN (never OK)...
    assert states[("coingecko_prices", "flat_value")] == "UNKNOWN"
    # ...but warm-up UNKNOWNs are transient: amber on the dashboard, no finding opened
    assert rep.ledger is not None and rep.ledger.opened == []
    assert rep.heartbeat is not None and rep.heartbeat["run_id"] == rep.run_id  # heartbeat is written by the runner, after the ledger

    # Now the job keeps "succeeding" but writes NOTHING new for 7 hours (exit 0, dead data).
    later = NOW + timedelta(hours=7)
    wh.now_override = later
    stub_all(monkeypatch, lambda fid, ctx: FetchResult([], "unchanged", None, 200))
    rep2 = run(fleet, wh, http=StubHttp(), now=later)
    assert all(r["status"] in ("no_change", "skipped") for r in rep2.runs)  # the job log is green (daily feeds not due)
    s2 = {(d.feed, d.axis): d.state for d in rep2.detections}
    assert s2[("usgs_earthquakes_hour", "write_recency")] == "BREACH"
    assert s2[("usgs_earthquakes_hour", "head_lag")] == "BREACH"
    # daily feeds have 30h SLAs; still OK after 7h
    assert s2[("fred_dgs10", "write_recency")] == "OK"
    opened = {(o["feed"], o["axis"]) for o in rep2.ledger.opened}
    assert ("usgs_earthquakes_hour", "write_recency") in opened

    # a third run with the same breaches writes NO ledger rows (transitions only)
    rep3 = run(fleet, wh, http=StubHttp(), now=later + timedelta(hours=1))
    assert rep3.ledger.finding_rows == [] and rep3.ledger.dispositions == []

    data = collect(fleet, wh, later + timedelta(hours=1))
    assert data["heartbeat"]["state"] == "ALIVE"
    assert data["summary"]["breach"] >= 2 and data["summary"]["not_declared"] == 1  # github_status flat undeclared
    assert all(f["opened_at"] for f in data["open_findings"])
    page = render_html(data)
    assert "DEAD-MAN" not in page.split("<script>")[0] and "not declared" in page and "BREACH" in page
    assert 'data-generated-at="' in page and "page-stale" in page  # client-side staleness check is wired
    from pipelineagent.render_site import render_healthlog
    log_md = render_healthlog(data)
    assert "OPENED" in log_md and "DISPOSITION" in log_md
    # 30h later with no heartbeat: loud red banner, and stale detector cells are downgraded to UNKNOWN
    much_later = later + timedelta(hours=30)
    data2 = collect(fleet, wh, much_later)
    assert data2["heartbeat"]["state"] == "STALE"
    assert "DEAD-MAN" in render_html(data2).split("<script>")[0]
    assert all(c["state"] in ("UNKNOWN", "N/A") for g in data2["grid"] for c in g["cells"].values())
    assert status(None, 26)["state"] == "MISSING"


def test_feed_crash_is_isolated_and_recorded(fleet, wh, monkeypatch):
    def flaky(fid, ctx):
        if fid == "coingecko_prices":
            raise RuntimeError("boom")
        return healthy_result(fid, ctx)

    stub_all(monkeypatch, flaky)
    rep = run(fleet, wh, http=StubHttp(), now=NOW)
    by = {r["feed"]: r for r in rep.runs}
    assert by["coingecko_prices"]["status"] == "failed" and by["coingecko_prices"]["reason"] == "exception:RuntimeError"
    assert by["usgs_earthquakes_hour"]["status"] == "ok"
    assert any(d.feed == "coingecko_prices" for d in rep.detections)  # detectors still ran for the crashed feed


def test_daily_feed_is_not_refetched_within_window(fleet, wh, monkeypatch):
    stub_all(monkeypatch, healthy_result)
    run(fleet, wh, http=StubHttp(), now=NOW, feeds=["fred_dgs10"])
    rep = run(fleet, wh, http=StubHttp(), now=NOW + timedelta(hours=2), feeds=["fred_dgs10"])
    assert rep.runs[0]["status"] == "skipped" and rep.runs[0]["reason"] == "not_due"


def test_on_unchanged_policy(fleet, wh, monkeypatch):
    """hourly (append): byte-identical content is still an observation and lands as a row;
    daily (skip): identical content is no_change. Decided BEFORE any write."""
    const = lambda fid, ctx: FetchResult([{"event_ts": ctx.now, "feature_count": 3}] if fid == "usgs_earthquakes_hour"
                                         else [{"series_id": "DGS10", "observation_date": ctx.now.date(), "event_ts": ctx.now, "value": 4.0}],
                                         "same-sha", None, 200)
    stub_all(monkeypatch, const)
    assert fleet.feed("usgs_earthquakes_hour").on_unchanged == "append" and fleet.feed("fred_dgs10").on_unchanged == "skip"
    run(fleet, wh, http=StubHttp(), now=NOW, feeds=["usgs_earthquakes_hour", "fred_dgs10"])
    later = NOW + timedelta(hours=25)
    wh.now_override = later
    rep = run(fleet, wh, http=StubHttp(), now=later, feeds=["usgs_earthquakes_hour", "fred_dgs10"])
    by = {r["feed"]: r for r in rep.runs}
    assert by["usgs_earthquakes_hour"]["status"] == "ok" and by["usgs_earthquakes_hour"]["rows_written"] == 1
    assert by["fred_dgs10"]["status"] == "no_change" and by["fred_dgs10"]["reason"] == "same_content_sha256"


def test_client_side_staleness_script_is_valid_js(tmp_path):
    import shutil, subprocess
    from pipelineagent.render_site import STALE_JS
    if not shutil.which("node"):
        import pytest
        pytest.skip("node not available")
    js = tmp_path / "stale.js"
    js.write_text("var document={body:{dataset:{generatedAt:new Date(Date.now()-30*36e5).toISOString(),maxAgeHours:'26'}},"
                  "getElementById:function(){return this._el}, _el:{className:'',textContent:''}};\n" + STALE_JS +
                  "\nif(document._el.className!=='banner red')throw new Error('expected red banner, got '+document._el.className);"
                  "console.log(document._el.textContent);")
    out = subprocess.run(["node", str(js)], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    assert "DEAD-MAN" in out.stdout
