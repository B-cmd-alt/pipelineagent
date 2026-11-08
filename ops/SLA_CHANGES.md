# SLA change log

Every loosening of a detector SLA in `fleet.yaml` (a larger `*_max_hours`, a larger
`flat_value.max_consecutive`, or removing an axis) must be recorded here as
`<feed_id>/<axis>` with a reason, or `tests/test_policy.py` fails. Tightening needs no entry.
Widening an SLA to make an open finding disappear is a policy violation; the release gate
rejects such a change (`SLA_WIDENED`).

| date | feed/axis | old → new | reason | finding ids affected |
|---|---|---|---|---|
| 2026-11-08 | (baseline) | — | initial SLAs pinned in `fleet.baseline.json` | — |
| 2026-11-08 | fred_dgs10/write_recency_max_hours | 30 → 80 | pre-deploy script review: the feed is `on_unchanged: skip` and DGS10 does not print on weekends, so Friday's file is byte-identical until Monday evening (~72h); 30h would raise a false BREACH every weekend. Baseline re-pinned the same day, before the first scheduled run. | none (pre-production) |
