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
| 2026-11-08 | ieso_ontario_demand/head_lag_max_hours | 36 → 54 | mis-specified SLA, found by the first live breach: PUB_Demand.csv is published once a day (~07:30 ET) containing the previous day's 24 hours, so the newest event_ts is 24-48h behind by the publisher's design; 36h breached every evening. Human-authored change (this is exactly the kind of PR the agent may not open). | `1855a49cd2088146` (disposed BENIGN: mis-specified SLA) |
