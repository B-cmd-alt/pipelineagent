# PipelineAgent

**An autonomous detection-and-repair agent for data pipelines, with a dashboard that shows decisions, not just graphs.**

Most pipeline monitoring watches the *job*: did it exit 0? PipelineAgent watches the
*data*: did fresh rows land, is the newest timestamp recent, did the values stop
moving? A job that exits 0 while writing nothing is the failure this project is built
around, and it is far more common than a crash.

v0 (this repo, week 1) ships the fleet, the detectors, the ledger, the dead-man's
switch and the dashboard, so a real incident history starts accruing on day one.
The repair agent, the fault-injection harness and the benchmark are specified in
[`DESIGN.md`](DESIGN.md) and come next.

## What runs

```
fleet.yaml ──► 7 public feeds ──► warehouse (append-only, vintage-stamped)
                                      │
                 detectors (read DATA, never exit codes) ──► OK / BREACH / UNKNOWN
                                      │
                 ledger: findings (OPENED/STILL_OPEN/CLOSED) + dispositions
                                      │
                 heartbeat (dead-man's switch, carries runtime code hash)
                                      │
                 render_site ──► site/index.html + data.json (static, zero hosting cost)
```

| Feed | Source | Cadence | Why it is in the fleet |
|---|---|---|---|
| `nws_observations` | api.weather.gov latest obs, 4 stations | hourly | sensors go null; station pages lag |
| `open_meteo_current` | Open-Meteo forecast API | hourly | stable control feed |
| `usgs_earthquakes_hour` | USGS GeoJSON all_hour | hourly | fast-moving data clock |
| `fred_dgs10` | FRED fredgraph CSV | daily | `.` for missing values; business-day gaps; connection resets |
| `github_status` | Statuspage summary.json | hourly | legitimately flat for weeks (flat axis deliberately *not declared*) |
| `coingecko_prices` | CoinGecko simple/price | hourly | HTTP 429 rate limits |
| `ieso_ontario_demand` | IESO PUB_Demand.csv | daily | hour-ending local time: DST days have 23/25 hours |

Every feed table carries `captured_at` (vintage), `run_id`, `event_ts` (the data's own
clock), `source_modified`, `content_sha256`. Rows are never updated or deleted.

### Three detector axes, one manifest

Each feed declares its SLAs in [`fleet.yaml`](fleet.yaml):

- **write recency**: newest `captured_at` vs now (did *we* write?)
- **head lag**: newest `event_ts` vs now (is the *data* current?)
- **flat value**: N identical consecutive values on a declared column, per group (frozen upstream)

Every result is exactly one of `OK`, `BREACH`, `UNKNOWN`. `UNKNOWN` means "we could not
look" (table missing, read failed, not enough history) and is rendered amber with a `?`,
never green. An axis that is not declared renders as "not declared", never green either.

### Ledger and dispositions

A *finding* is one continuous not-OK stretch of a feed × axis. Each detector run emits
exactly one event per open finding: `OPENED`, `STILL_OPEN` (one line, never a
re-diagnosis), or `CLOSED`. Each finding has exactly one current disposition:
`FIXED | BENIGN | ESCALATED | DEFERRED | UNKNOWN`. In v0 a new finding gets `UNKNOWN`
("awaiting triage") and a finding that closes by itself gets `BENIGN`; humans override
with `pipelineagent dispose`. `FIXED` is refused without an evidence reference (a gate
verdict). Everything also lands as lines in [`ops/HEALTHLOG.md`](ops/HEALTHLOG.md).

### Policies that are tests

- `tests/test_policy.py`: SLAs cannot be loosened relative to `fleet.baseline.json`
  without a `feed/axis` entry in `ops/SLA_CHANGES.md`. Widening an SLA to make a
  finding disappear is the classic non-fix.
- `tests/test_no_private_leakage.py`: the tree contains no project ids, service
  accounts, tokens, or references to any private codebase.
- Removing a detector never auto-closes its findings (`test_ledger.py`).

### Dead-man's switch and deploy drift

The loop writes a `heartbeat` row after detection + ledger complete. If the newest row
is older than 26h the dashboard shows a red full-width banner and downgrades every stale
cell to `UNKNOWN`. The row carries `code_sha256`, a hash of the running package, so the
operator can prove the deployed code is the committed code:

```bash
python -m pipelineagent version          # repo hash
# ... compare with heartbeat.code_sha256 (also shown on the dashboard)
```

## Running it

```bash
pip install -e ".[dev]"
pytest -q

# offline demo against SQLite (real HTTP to the public feeds, no cloud needed)
export PIPELINEAGENT_WAREHOUSE=sqlite:///pipelineagent.db
python -m pipelineagent fetch --feed usgs_earthquakes_hour   # pre-flight: prints rows, writes nothing
python -m pipelineagent run                                  # fetch -> detect -> ledger -> heartbeat -> render
open site/index.html

# production: BigQuery warehouse + hourly Cloud Run Job (see deploy.sh)
export PIPELINEAGENT_WAREHOUSE=bigquery://<project>/pipelineagent
python -m pipelineagent init-warehouse
PROJECT=<project> REGION=us-central1 SA=<runtime-sa> SITE_BUCKET=<bucket> ./deploy.sh
```

`python -m pipelineagent dispose <finding_id> ESCALATED --who alice --note "upstream ticket"`
records a human decision.

## Provider etiquette

Identifying User-Agent, per-host pacing, a hard request budget per run, conditional GET
(ETag / If-Modified-Since) and content-hash short-circuit before any write, daily feeds
are not re-fetched inside 20h, a 404 is a benign `skipped` run, and an HTML error page
served with HTTP 200 is detected and recorded as a `failed` run instead of being parsed.

## Cost

One hourly Cloud Run Job (1 vCPU, 512 MiB, ~1 minute), one Cloud Scheduler trigger, BigQuery
load jobs (free) and a handful of KB-scale queries per run, a static site in a bucket.
Steady state is well under $2/month.

## The seam to a release gate

Repair PRs (when the agent exists) carry a verdict/evidence JSON conforming to
[`schemas/verdict.schema.json`](schemas/verdict.schema.json): `run_manifest`,
`patch_sha256`, `verdict`, `reason_codes`, `evidence`. A separate gate judges it; the
agent cannot grade its own work.

## License

MIT.
