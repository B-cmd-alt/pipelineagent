# PipelineAgent

**An autonomous detection-and-repair agent for data pipelines, with a dashboard that shows decisions, not just graphs.**

**Live dashboard: <https://b-cmd-alt.github.io/pipelineagent/>** — regenerated after every hourly run; a red
banner means the job itself has been silent for more than 26h. First real repair, end to end with zero human
edits to the fetcher: [#10](https://github.com/B-cmd-alt/pipelineagent/pull/10) (record in `ops/DRILL.md`).
License: MIT.

Most pipeline monitoring watches the *job*: did it exit 0? PipelineAgent watches the
*data*: did fresh rows land, is the newest timestamp recent, did the values stop
moving? A job that exits 0 while writing nothing is the failure this project is built
around, and it is far more common than a crash.

v0 ships the fleet, the detectors, the ledger, the dead-man's switch and the dashboard,
so a real incident history starts accruing on day one. v1 adds the repair agent (triage →
replay → patch → PR → gate → merge → deploy → verify, with revert and escalation). The
fault-injection harness and the benchmark are specified in [`DESIGN.md`](DESIGN.md) and
come next.

## Results: three injected failures, three correct outcomes

Faults are injected between the HTTP client and the parser, so everything downstream reacts as it
would to a real upstream change. Each injection writes a ground-truth row; `python -m pipelineagent
benchmark` joins that against the ledger. Live numbers, 2026-11-08 to 2026-11-10:

| injected failure | feed | detected after | agent's outcome | expected | time from detection to outcome | evidence |
|---|---|---|---|---|---|---|
| schema drift (a key renamed) | coingecko_prices | 182 min (3h SLA) | **FIXED** by its own PR | FIXED | 85 min | [#10](https://github.com/B-cmd-alt/pipelineagent/pull/10) |
| stale source (every fetch times out) | github_status | 182 min (3h SLA) | **ESCALATED**, no PR | ESCALATED | 60 min | [#19](https://github.com/B-cmd-alt/pipelineagent/issues/19) |
| green job, dead data (frozen payload, exit 0) | ieso_ontario_demand | 30h (the daily feed's SLA) | **ESCALATED**, no PR | ESCALATED | same run | [#22](https://github.com/B-cmd-alt/pipelineagent/issues/22) |

- **Baseline.** Detectors without the agent find the same three failures at the same time and then
  wait for a person. The agent's contribution is the last three columns.
- **Knowing when not to patch** is half the result: two of three failures have nothing to fix in the
  repo, and the agent opened one issue each, called no model, and closed its own issue when the
  source recovered.
- **The honest part.** The schema-drift repair took six gate rejections and one human re-queue. Every
  rejection was a defect in this repo's own gate or tests (a test pinning the pre-repair behaviour, a
  gate contradicting the prompt, feedback too terse to act on), none in the model's patches. They are
  listed with their fixes in [`ops/DRILL.md`](ops/DRILL.md). The other two drills needed no human.
- Findings with no injection active (FRED has been timing out from the cloud for days) are real
  incidents; the benchmark lists them and does not score them.

## What runs

```
fleet.yaml ──► 7 public feeds ──► warehouse (append-only, vintage-stamped)
                                      │
                 detectors (read DATA, never exit codes) ──► OK / BREACH / UNKNOWN
                                      │
                 ledger: findings (OPENED/CLOSED transitions) + dispositions
                                      │
                 heartbeat (dead-man's switch, carries runtime code hash)
                                      │
                 render_site ──► site/index.html + data.json (static, zero hosting cost)
```

| Feed | Source | Cadence | Why it is in the fleet |
|---|---|---|---|
| `nws_observations` | api.weather.gov latest obs, 4 stations | hourly | sensors go null; station pages lag |
| `open_meteo_current` | Open-Meteo current conditions | hourly | stable control feed |
| `usgs_earthquakes_hour` | USGS GeoJSON all_hour | hourly | fast-moving data clock |
| `fred_dgs10` | FRED fredgraph CSV | daily | `.` for missing values; business-day gaps; connection resets |
| `github_status` | Statuspage summary.json | hourly | legitimately flat for weeks (flat axis deliberately *not declared*) |
| `coingecko_prices` | CoinGecko simple/price | hourly | HTTP 429 rate limits |
| `ieso_ontario_demand` | IESO PUB_Demand.csv | daily | hour-ending in fixed EST all year: the obvious DST conversion is wrong |

Every feed table carries `captured_at` (vintage), `run_id`, `event_ts` (the data's own
clock), `source_modified`, `content_sha256`. Rows are never updated or deleted. Hourly feeds
are *state* feeds: every poll is written even when identical, so liveness shows in the data
and a frozen upstream is visible. Daily feeds are growing series: identical content is a
`no_change`. The raw upstream bytes are archived per `content_sha256` when a payload bucket
is configured, so any vintage can be replayed later.

### Three detector axes, one manifest

Each feed declares its SLAs in [`fleet.yaml`](fleet.yaml):

- **write recency**: newest `captured_at` vs now (did *we* write?)
- **head lag**: newest `event_ts` vs now (is the *data* current?)
- **flat value**: N identical consecutive values on a declared column, per group (frozen upstream)

Every result is exactly one of `OK`, `BREACH`, `UNKNOWN`. `UNKNOWN` means "we could not
look" (table missing, read failed, not enough history) and is rendered amber with a `?`,
never green. An axis that is not declared renders as "not declared", never green either.

### Ledger and dispositions

A *finding* is one continuous not-OK stretch of a feed × axis. The ledger records only
transitions, `OPENED` and `CLOSED`; the hourly state lives in `detections`, and a known-open
finding costs nothing per run and is never re-diagnosed. Each finding has exactly one current
disposition: `FIXED | BENIGN | ESCALATED | DEFERRED | UNKNOWN`. In v0 a new finding gets
`UNKNOWN` ("awaiting triage") and a finding that closes by itself gets `BENIGN`; humans
override with `pipelineagent dispose`. `FIXED` is refused without an evidence reference (a
gate verdict). A warm-up `UNKNOWN` (a feed younger than its flat-value window) is shown amber
but opens no finding. `site/HEALTHLOG.md` is generated from the two tables on every render.

### Policies that are tests

- `tests/test_policy.py`: SLAs cannot be loosened relative to the merge-base `fleet.yaml`
  in git without a `feed/axis` entry in `ops/SLA_CHANGES.md`. Widening an SLA to make a
  finding disappear is the classic non-fix.
- `tests/test_no_private_leakage.py`: the tree contains no project ids, service
  accounts, tokens, or references to any private codebase.
- Removing a detector never auto-closes its findings (`test_ledger.py`).

### The repair agent (v1, runs inside the hourly job)

After the ledger and heartbeat, `agent.run()` takes this run's newly OPENED findings plus any
DEFERRED ones, at most **2 per run**. Triage is deterministic (`agent/triage.py`): a parser
`schema:` error is `schema_drift` and gets a patch; a failing upstream is `stale_source` and is
escalated after one run of patience; a green job with no rows is `green_job_dead_data` and is
escalated (nothing to patch). Only the patch step calls a model (any OpenRouter model, default DeepSeek V4 Pro,
strict JSON-schema output; measured on the FRED column-rename case: one call, $0.0018, 87 s);
it proposes a minimal change to the one failing fetcher module plus a regression test. The
proposal is judged by **replay** (archived failing bytes must break the old code and load
cleanly with the new one; the last 5 healthy payloads must produce identical rows) and by the
**gate** (`proofgate check`): a diff linter (`SLA_WIDENED`, `DETECTOR_DISABLED`,
`TEST_WEAKENED`, `SCOPE_EXCEEDED`, `POLICY_FILE_TOUCHED`, `SECRET_IN_DIFF`) plus the replay
re-run and the test suite. On `PASS` the agent opens the PR, merges it, triggers the build and
deploy, and the next live detector runs decide: cleared → `FIXED`; still open after 2 runs →
revert + redeploy; three failed attempts → `ESCALATED` with exactly one GitHub issue, which the
agent closes itself (one comment) once the finding is no longer open — a source that recovers or a
later repair that lands never leaves a stale "needs-human" issue behind. Every
step is a row in `agent_steps`, rendered as the chain on the finding card.

Without `GITHUB_TOKEN` + `GITHUB_REPO` and `OPENROUTER_API_KEY` the agent runs **dry**: it
triages and replays for real, writes the PR it would open under `site/agent/`, and opens
nothing. The hourly job is deployed in that mode until the owner mounts the two secrets.

### Fault injection and the live drill

`PIPELINEAGENT_INJECT=<feed>=<kind>` on the job corrupts that feed's payload between the HTTP
layer and the parser (`schema_rename`, `stale_source`, `html_error`), writes ground truth to
`harness_injections`, and lets everything downstream react for real. `ops/DRILL.md` is the
runbook for proving the whole loop (patch → PR → auto-merge → deploy → `FIXED`) on the deployed
job, and for the "correct refusal" drill where the right answer is one issue and no PR.

### Dead-man's switch and deploy drift

The loop writes a `heartbeat` row after detection + ledger complete. If the newest row
is older than 26h the dashboard shows a red full-width banner and downgrades every stale
cell to `UNKNOWN`. Because the page is rendered by the loop itself, it also carries a
client-side check: the viewer's clock against `generated_at` (amber after 3h, red after
26h), so a dead loop cannot leave a green page behind. The heartbeat row carries
`code_sha256`, a hash of the running package; `deploy.sh` executes the job once after every
deploy and fails with MISMATCH unless that hash equals `python -m pipelineagent version` at
the deployed commit.

## Running it

```bash
pip install -e ".[dev]"
pytest -q

# offline demo against SQLite (real HTTP to the public feeds, no cloud needed)
export PIPELINEAGENT_WAREHOUSE=sqlite:///pipelineagent.db
python -m pipelineagent fetch --feed usgs_earthquakes_hour   # pre-flight: prints rows, writes nothing
python -m pipelineagent run                                  # fetch -> detect -> ledger -> heartbeat -> render
open site/index.html

# production: BigQuery warehouse + hourly Cloud Run Job (tables are created on first run)
PROJECT=<project> REGION=us-central1 SA=<runtime-sa> SITE_BUCKET=<bucket> ./deploy.sh
```

`python -m pipelineagent dispose <finding_id> ESCALATED --who alice --note "upstream ticket"`
records a human decision.

## Provider etiquette

Identifying User-Agent, per-host pacing, a hard request budget per run, daily feeds are
not re-fetched inside 20h and identical content is never re-written, a 404 is a benign
`skipped` run, and an HTML error page served with HTTP 200 is detected and recorded as a
`failed` run instead of being parsed.

## Cost

One hourly Cloud Run Job (1 vCPU, 512 MiB, ~1 minute), one Cloud Scheduler trigger, BigQuery
load jobs (free) and a handful of KB-scale queries per run, a static site in a bucket.
Steady state is well under $2/month, plus a fraction of a cent per model call (only when a patch
is attempted).

The dashboard is published to the `gh-pages` branch after every render when the GitHub token is
mounted (`PIPELINEAGENT_PAGES=0` disables); GitHub Pages serves it at <https://b-cmd-alt.github.io/pipelineagent/>.

## The seam to a release gate

Repair PRs (when the agent exists) carry a verdict/evidence JSON conforming to
[`schemas/verdict.schema.json`](schemas/verdict.schema.json): `run_manifest`,
`patch_sha256`, `verdict`, `reason_codes`, `evidence`. A separate gate judges it; the
agent cannot grade its own work.

## License

MIT.
