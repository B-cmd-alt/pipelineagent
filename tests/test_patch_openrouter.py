"""The patch step's model call (OpenRouter chat-completions), with the HTTP layer stubbed:
no network, no credentials. Covers the request contract (strict json_schema, routing only to
providers that honour it, fallbacks) and every way the response can be unusable."""
import json
from types import SimpleNamespace

import pytest
import requests

from pipelineagent.agent import patch

SPEC = SimpleNamespace(id="demo_feed", description="a demo feed")
GOOD = {"module_source": "COLUMNS = []\n", "test_filename": "test_agent_fix_demo_feed.py",
        "test_source": "def test_ok():\n    assert True\n", "explanation": "Two sentences. Done."}


class _Resp:
    def __init__(self, status=200, body=None, text=None):
        self.status_code, self._body = status, body
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


def _ok(content, finish="stop", **msg):
    return _Resp(200, {"model": "m", "provider": "p", "usage": {},
                       "choices": [{"finish_reason": finish, "message": {"content": content, **msg}}]})


@pytest.fixture
def calls(monkeypatch):
    seen, queue = [], []
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")
    monkeypatch.delenv("PIPELINEAGENT_MODEL", raising=False)
    monkeypatch.delenv("OPENROUTER_BASE_URL", raising=False)

    def fake_post(url, json=None, headers=None, timeout=None):
        seen.append({"url": url, "json": json, "headers": headers})
        r = queue.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(patch.requests, "post", fake_post)
    return SimpleNamespace(seen=seen, queue=queue)


def _propose():
    return patch.propose(SPEC, "COLUMNS = []\n", "KeyError: 'x'", '{"y": 1}')


def test_no_key_is_dry_and_makes_no_call(calls, monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY")
    assert _propose() is None
    assert calls.seen == []


def test_request_contract_and_parse(calls):
    calls.queue.append(_ok(json.dumps(GOOD)))
    p = _propose()
    assert p == patch.PatchProposal(**GOOD)
    (c,) = calls.seen
    assert c["url"] == "https://openrouter.ai/api/v1/chat/completions"
    assert c["headers"]["Authorization"] == "Bearer sk-or-test"
    body = c["json"]
    assert body["model"] == patch.DEFAULT_MODEL and "models" not in body
    rf = body["response_format"]
    assert rf["type"] == "json_schema" and rf["json_schema"]["strict"] is True and rf["json_schema"]["schema"] == patch.SCHEMA
    assert body["provider"]["require_parameters"] is True
    assert [m["role"] for m in body["messages"]] == ["system", "user"]
    assert "demo_feed" in body["messages"][1]["content"]


def test_model_list_becomes_fallbacks(calls, monkeypatch):
    monkeypatch.setenv("PIPELINEAGENT_MODEL", " a/one , b/two ")
    calls.queue.append(_ok(json.dumps(GOOD)))
    _propose()
    body = calls.seen[0]["json"]
    assert body["model"] == "a/one" and body["models"] == ["a/one", "b/two"]


def test_fenced_json_and_bad_test_filename_are_normalised(calls):
    calls.queue.append(_ok("```json\n" + json.dumps({**GOOD, "test_filename": "fix.py"}) + "\n```"))
    assert _propose().test_filename == "test_agent_fix_demo_feed.py"


@pytest.mark.parametrize("resp", [
    _Resp(402, {"error": {"code": 402, "message": "insufficient credits"}}),
    _Resp(200, {"error": {"code": 502, "message": "upstream"}}),
    _Resp(500, None, text="<html>"),
    _ok(json.dumps(GOOD), finish="length"),
    _ok("", refusal="no"),
    _ok("not json"),
    _ok(json.dumps({k: v for k, v in GOOD.items() if k != "module_source"})),
    _Resp(200, {"choices": []}),
    requests.ConnectionError("down"),
])
def test_unusable_responses_yield_none(calls, resp):
    calls.queue.append(resp)
    assert _propose() is None
