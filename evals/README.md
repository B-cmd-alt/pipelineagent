# Scenario evals (layer 1)

Each file in `scenarios/` scripts a sequence of runs for one feed and records what the system must
conclude: which findings open, how triage classifies them, what the agent does, how the gate rules, and
the final disposition. `python -m pipelineagent eval` replays all of them through the real fetchers,
detectors, ledger and agent on SQLite with no network and no model; `tests/test_evals.py` runs the same
thing on every change.

Guards on the evals themselves: answers cannot change without a line in `CHANGES.md`; every scenario
must fail when its answer is deliberately falsified (a check that still passes is blind); breaking
triage, the SLA or the gate must turn at least one scenario red; a scenario that crashes or is skipped
counts as a failure. `recorded/` holds reference patches (hand-written, labelled as such): this layer
tests everything around the model, not the model. The agent's own gate treats `evals/` as policy.
