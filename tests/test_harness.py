"""Fault injection sits between HTTP and the parser, so everything downstream sees a real failure."""
import json

import pytest
from conftest import NOW, fixture_text

from pipelineagent import harness
from pipelineagent.feeds import coingecko_prices
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


def test_schema_rename_rewrites_the_bytes_the_parser_sees_and_archives_them(wh):
    """The injection is judged on the BYTES, not on the parser's reaction: once the agent has
    repaired the parser, the renamed key is accepted and that is the desired end state."""
    inner = FakeHttp(fixture_text("coingecko.json").encode())
    client = harness.InjectingClient(inner, harness.Injection("coingecko_prices", "schema_rename"))
    r = client.get("https://api.coingecko.com/api/v3/simple/price", expect="json")
    assert b'"usd_price"' in r.body and b'"usd"' not in r.body.replace(b'"usd_price"', b"")
    assert client.last_body == inner.last_body == r.body  # what the parser saw is what gets archived
    assert client.used == 1 and client.budget == inner.budget


def test_coingecko_null_value_is_data_not_an_error():
    # DESIGN §4: do not pin how the parser treats a MISSING key — that is exactly what a
    # schema-drift repair changes. Only the null-value contract is fixed.
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
    """Ground truth is written and the injected bytes are archived under the run's sha.
    Judged with stale_source (fails regardless of parser) + schema_rename (bytes only): a test
    must never assert that the parser REJECTS the drill's rename, because the repair agent is
    expected to make it accept it, and this suite is part of the gate that judges that repair."""
    from pipelineagent.runner import run

    body = fixture_text("coingecko.json").encode()

    class Http(FakeHttp):
        def get(self, url, **kw):
            self.used += 1
            self.last_body = body
            return Response(200, body, "application/json", None)

    monkeypatch.setenv("PIPELINEAGENT_INJECT", "coingecko_prices=stale_source")
    rep = run(fleet, wh, http=Http(body), now=NOW, feeds=["coingecko_prices"], detect=False)
    r = rep.runs[0]
    assert r["status"] == "failed" and "injected" in r["error"]
    gt = wh.conn.execute("SELECT feed, kind, failure_class FROM harness_injections").fetchall()
    assert [tuple(x) for x in gt] == [("coingecko_prices", "stale_source", "stale_source")]

    monkeypatch.setenv("PIPELINEAGENT_INJECT", "coingecko_prices=schema_rename")
    rep2 = run(fleet, wh, http=Http(body), now=NOW, feeds=["coingecko_prices"], detect=False)
    r2 = rep2.runs[0]
    assert r2["content_sha256"]  # whatever the parser did with them, the injected bytes are archived under this hash
    import hashlib
    assert r2["content_sha256"] == hashlib.sha256(body.replace(b'"usd"', b'"usd_price"')).hexdigest()
    gt2 = wh.conn.execute("SELECT kind FROM harness_injections ORDER BY ts").fetchall()
    assert [x[0] for x in gt2] == ["stale_source", "schema_rename"]


def test_injected_daily_feed_is_always_due(fleet, wh, monkeypatch):
    from pipelineagent.runner import run

    monkeypatch.setenv("PIPELINEAGENT_INJECT", "ieso_ontario_demand=stale_source")
    rep = run(fleet, wh, http=FakeHttp(b""), now=NOW, feeds=["ieso_ontario_demand"], detect=False)
    assert rep.runs[0]["status"] == "failed" and "injected" in rep.runs[0]["error"]
    rep2 = run(fleet, wh, http=FakeHttp(b""), now=NOW, feeds=["ieso_ontario_demand"], detect=False)
    assert rep2.runs[0]["status"] == "failed"  # not 'skipped: not_due'


def test_frozen_payload_serves_the_archived_healthy_bytes_with_http_200():
    """Class 1 (green job, dead data): the request 'succeeds' but the bytes never change."""
    inner = FakeHttp(b'{"live": 1}')
    c = harness.InjectingClient(inner, harness.Injection("coingecko_prices", "frozen_payload"), [b'{"bitcoin":{"usd":1,"last_updated_at":1700000000}}'])
    r = c.get("https://x", expect="json")
    assert r.status == 200 and r.body == b'{"bitcoin":{"usd":1,"last_updated_at":1700000000}}' and inner.used == 1
    assert c.last_body == r.body  # what gets archived is the frozen body, not the live one
    with pytest.raises(SourceError, match="frozen_payload"):
        c.get("https://x", expect="json")  # bodies are served in request order; none left -> loud
    assert harness.rules_from_env("ieso_ontario_demand=frozen_payload")["ieso_ontario_demand"].failure_class == "green_job_dead_data"
