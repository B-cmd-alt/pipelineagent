"""The one model call: propose a minimal patch to a failing fetcher module for a
schema-drift failure, plus a regression test with the failing payload as its fixture.
Structured output; the gate decides whether it is any good. Dry run without credentials."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from ..config import FeedSpec

log = logging.getLogger("pipelineagent.patch")

MODEL = os.environ.get("PIPELINEAGENT_MODEL", "claude-opus-5-5")

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


def propose(spec: FeedSpec, module_source: str, error: str, payload_sample: str, prior_gate_feedback: str = "", existing_tests: str = "") -> PatchProposal | None:
    try:
        import anthropic
    except ImportError:
        log.warning("anthropic SDK not installed; patch step runs dry")
        return None
    if not (os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN")):
        log.info("no model credentials; patch step runs dry")
        return None
    user = (f"Feed `{spec.id}` ({spec.description}).\n\nThe fetcher failed with:\n```\n{error}\n```\n\n"
            f"Failing payload (first bytes):\n```\n{payload_sample}\n```\n\nCurrent module `pipelineagent/feeds/{spec.id}.py`:\n```python\n{module_source}\n```\n\n"
            f"Existing parser tests (for style; do not modify them):\n```python\n{existing_tests[:6000]}\n```\n"
            + (f"\nThe gate rejected the previous attempt with: {prior_gate_feedback}\n" if prior_gate_feedback else ""))
    client = anthropic.Anthropic()
    try:
        resp = client.messages.create(
            model=MODEL, max_tokens=16000, system=SYSTEM,
            messages=[{"role": "user", "content": user}],
            output_config={"format": {"type": "json_schema", "schema": SCHEMA}, "effort": "high"},
        )
    except anthropic.APIStatusError as e:
        log.error("model call failed: %s", e)
        return None
    if resp.stop_reason == "refusal":
        log.warning("model refused the patch request")
        return None
    import json

    text = next((b.text for b in resp.content if b.type == "text"), "")
    try:
        d = json.loads(text)
    except ValueError:
        log.error("model output was not valid JSON")
        return None
    if not d.get("test_filename", "").startswith("test_") or not d["test_filename"].endswith(".py"):
        d["test_filename"] = f"test_agent_fix_{spec.id}.py"
    return PatchProposal(d["module_source"], d["test_filename"], d["test_source"], d["explanation"])


def apply_proposal(p: PatchProposal, head: Path, module_rel: Path) -> None:
    (head / module_rel).write_text(p.module_source)
    (head / "tests" / p.test_filename).write_text(p.test_source)
