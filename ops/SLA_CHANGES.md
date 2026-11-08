# SLA change log

Every loosening of a detector SLA in `fleet.yaml` (a larger `*_max_hours`, a larger
`flat_value.max_consecutive`, or removing an axis) must be recorded here as
`<feed_id>/<axis>` with a reason, or `tests/test_policy.py` fails. Tightening needs no entry.
Widening an SLA to make an open finding disappear is a policy violation; the release gate
rejects such a change (`SLA_WIDENED`).

| date | feed/axis | old → new | reason | finding ids affected |
|---|---|---|---|---|
| 2026-11-08 | (baseline) | — | initial SLAs pinned in `fleet.baseline.json` | — |
