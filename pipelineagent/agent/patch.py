"""The one model call: propose a minimal patch to a failing fetcher module for a
schema-drift failure, plus a regression test with the failing payload as its fixture.
Structured output; the gate decides whether it is any good. Dry run without credentials.

The call goes to OpenRouter's OpenAI-compatible chat-completions endpoint with plain
`requests` (no provider SDK). `PIPELINEAGENT_MODEL` may be a comma-separated list: the first
id is the primary, the rest are OpenRouter fallbacks tried in order when it is down."""
from __future__ import annotations

import ast
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
- "Fixed" means rows LAND for the failing payload: never keep the feed failing or raising on it "to preserve alerting"; record the drift as an anomaly string instead.
- Prefer tolerant column/field mapping (accept old and new names) over dropping data. Record anything unexpected in `anomalies`, never silently.
- Add one new pytest test file whose fixture is the failing payload (inline as a string) and asserts the module now parses it.
- No network access, no new dependencies, no secrets. Keep the diff minimal and explain it in two sentences.
Output: one JSON object. `module_source` is the COMPLETE new text of the module file, every line from the
docstring to the last line (never a path, a diff, or an excerpt). `test_source` is the COMPLETE text of the new
pytest file. `test_filename` is its bare file name. `explanation` is the two-sentence explanation."""

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


PUBLIC_NAMES = ("COLUMNS", "FEED", "fetch", "parse")


def _top_level_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def problems(d: dict, original_module: str) -> list[str]:
    """Cheap structural checks before the gate: both sources parse, and the module still defines the public
    names the original had. Catches answers that put a path or an excerpt where the whole file belongs."""
    out = []
    try:
        tree = ast.parse(d["module_source"])
    except SyntaxError as e:
        out.append(f"module_source is not valid Python ({e.msg}, line {e.lineno})")
        tree = None
    if tree is not None:
        try:
            want = _top_level_names(ast.parse(original_module)) & set(PUBLIC_NAMES)
        except SyntaxError:
            want = set()
        missing = sorted(want - _top_level_names(tree))
        if missing:
            out.append(f"module_source does not define {missing}")
    try:
        ast.parse(d["test_source"])
    except SyntaxError as e:
        out.append(f"test_source is not valid Python ({e.msg}, line {e.lineno})")
    return out


def _call(messages: list[dict], key: str) -> tuple[dict | None, str]:
    """One chat-completions round trip. Returns (parsed object or None, raw content)."""
    ids = models()
    body: dict = {
        "model": ids[0],
        "messages": messages,
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
        return None, ""
    try:
        resp = r.json()
    except ValueError:
        resp = {}
    if not isinstance(resp, dict):
        resp = {}
    if r.status_code >= 400 or "error" in resp:
        log.error("model call failed: %s %s", r.status_code, str(resp.get("error") or r.text)[:300])
        return None, ""
    choice = (resp.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    log.info("model %s via %s: finish=%s usage=%s", resp.get("model"), resp.get("provider"), choice.get("finish_reason"), resp.get("usage"))
    if msg.get("refusal"):
        log.warning("model refused the patch request")
        return None, ""
    if choice.get("finish_reason") == "length":
        log.error("model output truncated at max_tokens=%d", MAX_TOKENS)
        return None, ""
    content = msg.get("content") or ""
    try:
        d = json.loads(_strip_fences(content))
    except ValueError:
        log.error("model output was not valid JSON")
        return None, content
    if not isinstance(d, dict) or not all(isinstance(d.get(k), str) for k in SCHEMA["required"]):
        log.error("model output is missing required fields")
        return None, content
    return d, content


def propose(spec: FeedSpec, module_source: str, error: str, payload_sample: str, prior_gate_feedback: str = "", existing_tests: str = "") -> PatchProposal | None:
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        log.info("no model credentials (OPENROUTER_API_KEY); patch step runs dry")
        return None
    user = (f"Feed `{spec.id}` ({spec.description}).\n\nThe fetcher failed with:\n```\n{error}\n```\n\n"
            f"Failing payload (first bytes):\n```\n{payload_sample}\n```\n\nCurrent module `pipelineagent/feeds/{spec.id}.py`:\n```python\n{module_source}\n```\n\n"
            f"Existing parser tests (for style; do not modify them):\n```python\n{existing_tests[:6000]}\n```\n"
            + (f"\nThe gate rejected the previous attempt with: {prior_gate_feedback}\n" if prior_gate_feedback else ""))
    messages = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]
    d, content = _call(messages, key)
    if d is None:
        return None
    issues = problems(d, module_source)
    if issues:
        # one corrective round trip; if it is still malformed, hand it to the gate anyway so the
        # rejection is recorded as an attempt (and escalates after MAX_ATTEMPTS) instead of looping as a dry run
        log.warning("proposal failed pre-gate checks (%s); asking once for a corrected answer", "; ".join(issues))
        fix = (f"That answer is unusable: {'; '.join(issues)}. Return the same JSON object again, with `module_source` set to "
               f"the COMPLETE new text of pipelineagent/feeds/{spec.id}.py (every line, starting from its docstring) and "
               f"`test_source` set to the COMPLETE text of the new test file.")
        d2, _ = _call(messages + [{"role": "assistant", "content": content}, {"role": "user", "content": fix}], key)
        if d2 is not None:
            d = d2
    if not d.get("test_filename", "").startswith("test_") or not d["test_filename"].endswith(".py"):
        d["test_filename"] = f"test_agent_fix_{spec.id}.py"
    return PatchProposal(d["module_source"], d["test_filename"], d["test_source"], d["explanation"])


def apply_proposal(p: PatchProposal, head: Path, module_rel: Path) -> None:
    (head / module_rel).write_text(p.module_source)
    (head / "tests" / p.test_filename).write_text(p.test_source)
