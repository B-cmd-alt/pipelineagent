# Scenario answer changes

A scenario's `expect` block is a recorded fact with an origin (a drill, a finding, a PR). Changing or
removing one needs a line here naming the scenario id and why; `tests/test_evals.py` compares every
scenario with its version at the merge-base and fails otherwise. Adding scenarios needs no entry.

| date | scenario id | change | why |
|---|---|---|---|
