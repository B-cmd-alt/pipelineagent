"""Layer-1 evals, and the guards that keep the evals themselves honest."""
import copy
import subprocess
from functools import lru_cache
from pathlib import Path

import pytest
import yaml

from pipelineagent import evals

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = {sc["id"]: sc for sc in evals.all_scenarios()}
FAST = sorted(i for i, sc in SCENARIOS.items() if not sc.get("proposal"))
SLOW = sorted(i for i, sc in SCENARIOS.items() if sc.get("proposal"))  # these run the gate (replay subprocesses + a nested pytest)
gate_run = pytest.mark.agent_e2e  # excluded inside the gate's own pytest run, or it would recurse


@lru_cache(maxsize=None)
def observed(sid: str) -> dict:
    return evals.observe(SCENARIOS[sid])


def _falsified(expect: dict) -> list[dict]:
    """One wrong answer per expected key: each must be caught."""
    out = []
    for key, val in expect.items():
        e = copy.deepcopy(expect)
        if key == "opened":
            e[key] = sorted(set(val) ^ {"head_lag"})
        elif key in ("triage", "triage_first"):
            e[key] = "unknown -> benign"
        elif key == "steps":
            e[key] = {k: ("failed" if v != "failed" else "ok") for k, v in val.items()}
        elif key == "absent_steps":
            e[key] = list(val) + ["triage"] if "triage" not in val else ["__never__"]
        elif key == "gate_codes":
            e[key] = list(val) + ["SLA_WIDENED"]
        elif key == "disposition":
            e[key] = {k: ("FIXED" if v != "FIXED" else "BENIGN") for k, v in val.items()}
        elif key == "opened_at_h":
            e[key] = {k: v + 1 for k, v in val.items()}
        elif key == "model_calls":
            e[key] = val + 1
        else:
            raise AssertionError(f"no falsifier for expect key {key!r}: add one before using it")
        out.append((key, e))
    return out


def _check(sid: str) -> None:
    sc, obs = SCENARIOS[sid], observed(sid)
    assert evals.diff(obs, sc["expect"]) == [], sid
    for key, wrong in _falsified(sc["expect"]):
        if key == "absent_steps" and wrong[key] == ["__never__"]:
            continue  # nothing observable to contradict in a scenario with no agent activity at all
        assert evals.diff(obs, wrong), f"{sid}: a falsified `{key}` still passes, so this scenario does not check it"


@pytest.mark.parametrize("sid", FAST)
def test_scenario(sid):
    _check(sid)


@gate_run
@pytest.mark.parametrize("sid", SLOW)
def test_scenario_through_the_gate(sid):
    _check(sid)


def test_every_scenario_file_runs_and_none_is_skipped():
    files = sorted(p.stem for p in evals.SCEN_DIR.glob("*.yaml"))
    assert files == sorted(SCENARIOS) and len(files) >= 9, "a scenario file whose id differs from its name, or a shrunken suite"
    assert sorted(FAST + SLOW) == files
    for sc in SCENARIOS.values():
        assert sc.get("origin") and sc["expect"], f"{sc['id']}: every scenario needs an origin and a non-empty answer"
    # a scenario that cannot run is a failure, never a pass
    broken = dict(SCENARIOS[FAST[0]], steps=[{"at_h": 0, "body": "does-not-exist.json"}])
    with pytest.raises(Exception):
        evals.observe(broken)


# ---- breaking the system must turn a scenario red (the evals are not blind) ----------------------------
def test_loosening_the_sla_is_caught(monkeypatch):
    from pipelineagent import config
    real = config.load_fleet

    def loose(path):
        fleet = real(path)
        for spec in fleet.feeds:
            if spec.sla.write_recency_max_hours:
                object.__setattr__(spec.sla, "write_recency_max_hours", spec.sla.write_recency_max_hours * 10)
        return fleet

    monkeypatch.setattr(config, "load_fleet", loose)
    sc = SCENARIOS["stale-source-github-status"]
    assert evals.diff(evals.observe(sc), sc["expect"]), "a 10x looser SLA went unnoticed"


def test_breaking_triage_is_caught(monkeypatch):
    from pipelineagent.agent import triage
    monkeypatch.setattr(triage, "classify", lambda f, runs, spec, now, first_seen=True: triage.Triage("stale_source", "defer", "broken on purpose"))
    sc = SCENARIOS["green-job-dead-data-ieso"]
    assert evals.diff(evals.observe(sc), sc["expect"]), "a triage that defers everything went unnoticed"


@gate_run
def test_a_gate_that_passes_everything_is_caught(monkeypatch):
    import pipelineagent.agent as agent_mod
    real = agent_mod.gate_check

    def rubber_stamp(*a, **k):
        v = real(*a, **k)
        v.update(verdict="PASS", reason_codes=["REPLAY_PASSED", "TESTS_PASSED"])
        return v

    monkeypatch.setattr(agent_mod, "gate_check", rubber_stamp)
    sc = SCENARIOS["schema-drift-fred-patch-still-raises"]
    assert evals.diff(evals.observe(sc), sc["expect"]), "a gate that passes a patch which still fails went unnoticed"


# ---- answers are recorded facts: changing or removing one needs a written reason -----------------------
def answer_changes(base: dict[str, dict], cur: dict[str, dict]) -> list[str]:
    return sorted(sid for sid, exp in base.items() if cur.get(sid) != exp)


def test_answers_not_changed_without_a_reason():
    try:
        mb = subprocess.run(["git", "merge-base", "HEAD", "origin/main"], cwd=ROOT, capture_output=True, text=True, timeout=20)
        ls = subprocess.run(["git", "ls-tree", "--name-only", mb.stdout.strip(), "evals/scenarios/"], cwd=ROOT, capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        pytest.skip("no git")
    if mb.returncode != 0 or ls.returncode != 0 or not ls.stdout.strip():
        pytest.skip("no git baseline for evals/scenarios (first version, shallow clone or tarball)")
    base = {}
    for rel in ls.stdout.split():
        doc = yaml.safe_load(subprocess.run(["git", "show", f"{mb.stdout.strip()}:{rel}"], cwd=ROOT, capture_output=True, text=True, timeout=20).stdout)
        base[doc["id"]] = doc["expect"]
    log = (ROOT / "evals" / "CHANGES.md").read_text()
    for sid in answer_changes(base, {i: sc["expect"] for i, sc in SCENARIOS.items()}):
        assert f"| {sid} |" in log, f"scenario {sid}: answer changed or scenario removed without a row in evals/CHANGES.md"


def test_answer_change_detection_on_a_synthetic_pair():
    base = {"a": {"opened": ["write_recency"]}, "b": {"opened": []}, "c": {"model_calls": 0}}
    cur = {"a": {"opened": []}, "c": {"model_calls": 0}, "d": {"opened": []}}
    assert answer_changes(base, cur) == ["a", "b"]  # changed, removed; unchanged and added are free


def test_the_agent_may_not_touch_the_evals(tmp_path):
    import shutil
    from pipelineagent.agent.gate import lint
    ig = shutil.ignore_patterns("__pycache__", ".pytest_cache", "site", "*.db", "payloads", ".git")
    base, head = tmp_path / "base", tmp_path / "head"
    shutil.copytree(ROOT, base, ignore=ig); shutil.copytree(ROOT, head, ignore=ig)
    p = head / "evals" / "scenarios" / "stale-source-github-status.yaml"
    p.write_text(p.read_text().replace("ESCALATED", "FIXED"))
    assert "POLICY_FILE_TOUCHED" in lint(base, head, "github_status")[0]
