# Scenario drafts

Opened by the agent (`pipelineagent/learn.py`) when a person settles a finding the agent could not
classify, or overrules the agent. Each draft holds the archived bytes of the runs around the finding
(`../bodies/drafts/<id>/`) and a `live` block: what the agent concluded and what the person decided.

A draft is not a test. To promote one: write its `expect` block (what the system should conclude),
move the YAML to `../scenarios/`, and run `python -m pipelineagent eval`. If the right answer needs a
new triage rule, that rule is a separate, human-reviewed PR that must keep every scenario green.
