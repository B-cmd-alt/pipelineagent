"""Fault injection sits between HTTP and the parser, so everything downstream sees a real failure."""
import json

import pytest
from conftest import NOW, fixture_text

from pipelineagent import harness
from pipelineagent.feeds import coingecko_prices
from pipelineagent.feeds.base import FetchContext
from pipelineagent.http import Response, SourceError


class FakeHttp:
    def __init__(self, body: bytes):
        self.body, self.used, self.budget, self.last_body = body, 0, 40, None

    def get(self, url, **kw):
        self.used += 1
        self.last_body = self.body
        return Response(200, self.body, "application/json", None)


def test_rules_from_env():
    r = harness.rules_from_env("coingecko_prices=schema_rename, fred_dgs10=stale_source")
    assert r["coingecko_prices"].failure_class == "schema_drift" and r["fred_dgs10"].failure_class == "stale_source"
    assert harness.rules_from_env("") == {}
    with pytest.raises(ValueError):
        harness.rules_from_env("x=explode")


def test_schema_rename_breaks_the_parser_and_archives_the_injected_bytes(wh):
    inner = FakeHttp(fixture_text("coingecko.json").encode())
    client = harness.InjectingClient(inner, harness.Injection("coingecko_prices", "schema_rename"))
    ctx = FetchContext(client, {"coins": ["bitcoin", "ethereum"]}, None, wh, "coingecko_prices", NOW)
    with pytest.raises(ValueError, match="schema"):
        coingecko_prices.fetch(ctx)
    assert b'"usd_price"' in client.last_body and client.last_body == inner.last_body  # what the parser saw is what gets archived


def test_coingecko_missing_key_is_schema_error_but_null_value_is_data():
    with pytest.raises(ValueError, match="schema"):
        coingecko_prices.parse({"bitcoin": {"usd_price": 1.0}}, ["bitcoin"])
    rows, anomalies = coingecko_prices.parse({"bitcoin": {"usd": None, "last_updated_at": 1700000000}}, ["bitcoin"])
    assert rows[0]["price_usd"] is None and anomalies == []


def test_stale_source_and_html_error_raise_like_the_real_client():
    inner = FakeHttp(b"{}")
    with pytest.raises(SourceError, match="ReadTimeout"):
        harness.InjectingClient(inner, harness.Injection("github_status", "stale_source")).get("https://x")
    c = harness.InjectingClient(inner, harness.Injection("github_status", "html_error"))
    with pytest.raises(SourceError, match="html_error_page"):
        c.get("https://x", expect="json")
    assert c.last_body.startswith(b"<!DOCTYPE html")


def test_ground_truth_rows_and_runner_wiring(fleet, wh, monkeypatch):
    from pipelineagent.runner import run

    monkeypatch.setenv("PIPELINEAGENT_INJECT", "coingecko_prices=schema_rename")
    body = fixture_text("coingecko.json").encode()

    class Http(FakeHttp):
        def get(self, url, **kw):
            self.used += 1
            self.last_body = body
            return Response(200, body, "application/json", None)

    rep = run(fleet, wh, http=Http(body), now=NOW, feeds=["coingecko_prices"], detect=False)
    r = rep.runs[0]
    assert r["status"] == "failed" and r["reason"] == "exception:ValueError" and "schema" in r["error"]
    assert r["content_sha256"]  # the injected bytes were archived under this hash
    gt = wh.conn.execute("SELECT feed, kind, failure_class FROM harness_injections").fetchall()
    assert [tuple(x) for x in gt] == [("coingecko_prices", "schema_rename", "schema_drift")]


def test_injected_daily_feed_is_always_due(fleet, wh, monkeypatch):
    from pipelineagent.runner import run

    monkeypatch.setenv("PIPELINEAGENT_INJECT", "ieso_ontario_demand=stale_source")
    rep = run(fleet, wh, http=FakeHttp(b""), now=NOW, feeds=["ieso_ontario_demand"], detect=False)
    assert rep.runs[0]["status"] == "failed" and "injected" in rep.runs[0]["error"]
    rep2 = run(fleet, wh, http=FakeHttp(b""), now=NOW, feeds=["ieso_ontario_demand"], detect=False)
    assert rep2.runs[0]["status"] == "failed"  # not 'skipped: not_due'
