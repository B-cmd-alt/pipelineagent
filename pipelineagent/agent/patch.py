"""The one model call: propose a minimal patch to a failing fetcher module for a
schema-drift failure, plus a regression test with the failing payload as its fixture.
Structured output; the gate decides whether it is any good. Dry run without credentials.

The call goes to OpenRouter's OpenAI-compatible chat-completions endpoint with plain
`requests` (no provider SDK). `PIPELINEAGENT_MODEL` may be a comma-separated list: the first
id is the primary, the rest are OpenRouter fallbacks tried in order when it is down."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import requests

from ..config import FeedSpec

log = logging.getLogger("pipelineagent.patch")

DEFAULT_MODEL = "deepseek/deepseek-v4-pro"
OPENROUTER_API = "https://openrouter.ai/api/v1"
MAX_TOKENS = 32000  # reasoning tokens count against this; the answer is a whole module + a test file
TIMEOUT_S = 600


def models() -> list[str]:
    raw = os.environ.get("PIPELINEAGENT_MODEL", "") or DEFAULT_MODEL
    return [m.strip() for m in raw.split(",") if m.strip()] or [DEFAULT_MODEL]

SYSTEM = """You repair one data-pipeline fetcher module in a Python project called PipelineAgent.
Rules you must follow; a separate gate rejects the patch otherwise:
- Change ONLY the given module. Keep its public names (COLUMNS, FEED, fetch, parse) and signatures.
- Do not change SLAs, detectors, the warehouse, tests that already exist, or any other file.
- The fix must make the module parse the provided failing payload AND still parse earlier healthy payloads identically.
- Prefer tolerant column/field mapping (accept old and new names) over dropping data. Record anything unexpected in `anomalies`, never silently.
- Add one new pytest test file whose fixture is the failing payload (inline as a string) and asserts the module now parses it.
- No network access, no new dependencies, no secrets. Keep the diff minimal and explain it in two sentences."""

SCHEMA = {
    "type": "object",
    "properties": {
        "module_source": {"type": "string", "description": "full new content of the fetcher module"},
        "test_filename": {"type": "string", "description": "new test file name, e.g. test_agent_fix_<feed>.py"},
        "test_source": {"type": "string", "description": "full content of the new pytest file"},
        "explanation": {"type": "string"},
    },
    "required": ["module_source", "test_filename", "test_source", "explanation"],
    "additionalProperties": False,
}


@dataclass
class PatchProposal:
    module_source: str
    test_filename: str
    test_source: str
    explanation: str


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else ""
        if t.rstrip().endswith("```"):
            t = t.rstrip()[:-3]
    return t.strip()


def propose(spec: FeedSpec, module_source: str, error: str, payload_sample: str, prior_gate_feedback: str = "", existing_tests: str = "") -> PatchProposal | None:
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        log.info("no model credentials (OPENROUTER_API_KEY); patch step runs dry")
        return None
    user = (f"Feed `{spec.id}` ({spec.description}).\n\nThe fetcher failed with:\n```\n{error}\n```\n\n"
            f"Failing payload (first bytes):\n```\n{payload_sample}\n```\n\nCurrent module `pipelineagent/feeds/{spec.id}.py`:\n```python\n{module_source}\n```\n\n"
            f"Existing parser tests (for style; do not modify them):\n```python\n{existing_tests[:6000]}\n```\n"
            + (f"\nThe gate rejected the previous attempt with: {prior_gate_feedback}\n" if prior_gate_feedback else ""))
    ids = models()
    body: dict = {
        "model": ids[0],
        "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
        "max_tokens": MAX_TOKENS,
        "response_format": {"type": "json_schema", "json_schema": {"name": "patch_proposal", "strict": True, "schema": SCHEMA}},
        # route only to providers that honour response_format/json_schema; never silently drop it
        "provider": {"require_parameters": True},
        "reasoning": {"effort": "high", "exclude": True},
    }
    if len(ids) > 1:
        body["models"] = ids
    base = os.environ.get("OPENROUTER_BASE_URL", OPENROUTER_API).rstrip("/")
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json", "X-Title": "PipelineAgent"}
    try:
        r = requests.post(f"{base}/chat/completions", json=body, headers=headers, timeout=TIMEOUT_S)
    except requests.RequestException as e:
        log.error("model call failed: %s", e)
        return None
    try:
        resp = r.json()
    except ValueError:
        resp = {}
    if not isinstance(resp, dict):
        resp = {}
    if r.status_code >= 400 or "error" in resp:
        log.error("model call failed: %s %s", r.status_code, str(resp.get("error") or r.text)[:300])
        return None
    choice = (resp.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    log.info("model %s via %s: finish=%s usage=%s", resp.get("model"), resp.get("provider"), choice.get("finish_reason"), resp.get("usage"))
    if msg.get("refusal"):
        log.warning("model refused the patch request")
        return None
    if choice.get("finish_reason") == "length":
        log.error("model output truncated at max_tokens=%d", MAX_TOKENS)
        return None
    try:
        d = json.loads(_strip_fences(msg.get("content") or ""))
    except ValueError:
        log.error("model output was not valid JSON")
        return None
    if not isinstance(d, dict) or not all(isinstance(d.get(k), str) for k in SCHEMA["required"]):
        log.error("model output is missing required fields")
        return None
    if not d.get("test_filename", "").startswith("test_") or not d["test_filename"].endswith(".py"):
        d["test_filename"] = f"test_agent_fix_{spec.id}.py"
    return PatchProposal(d["module_source"], d["test_filename"], d["test_source"], d["explanation"])


def apply_proposal(p: PatchProposal, head: Path, module_rel: Path) -> None:
    (head / module_rel).write_text(p.module_source)
    (head / "tests" / p.test_filename).write_text(p.test_source)
