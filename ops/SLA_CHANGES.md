# SLA change log

Every loosening of a detector SLA in `fleet.yaml` (a larger `*_max_hours`, a larger
`flat_value.max_consecutive`, or removing an axis) must be recorded here as
`<feed_id>/<axis>` with a reason, or `tests/test_policy.py` fails; removing a feed needs a
`<feed_id>/removed` entry. The baseline is the merge-base version of `fleet.yaml` in git, so
it cannot be edited away in the same diff. Tightening needs no entry.
Widening an SLA to make an open finding disappear is a policy violation; the release gate
rejects such a change (`SLA_WIDENED`).

| date | feed/axis | old → new | reason | finding ids affected |
|---|---|---|---|---|
| 2026-11-08 | fred_dgs10/write_recency_max_hours | 30 → 80 | pre-deploy script review: daily feeds skip identical content and DGS10 does not print on weekends, so Friday's file is byte-identical until Monday evening (~72h); 30h would raise a false BREACH every weekend. Changed before the first scheduled run. | none (pre-production) |
